use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use crate::{
    Never,
    app::AppState,
    replication::ReplicationState,
    state::{Proceed, State},
};

use super::{ClientId, Message, Reply, Request, Send, ServiceApp, ServiceState};

pub struct Service<A: AppState, R: ReplicationState<Request<A::Op>>> {
    app: A,
    replication: R,

    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    replicated: Option<Replicated<A, R>>,

    submit_buffer: Vec<Request<A::Op>>,
    #[allow(clippy::type_complexity)] // this matches <Self as State>::Send
    send_buffer: Vec<Send<Reply<A::Res, R::Metadata>, R::Send>>,
}

type Replicated<A, R> = (
    VecDeque<Request<<A as ServiceApp>::Op>>,
    <R as ReplicationState<Request<<A as ServiceApp>::Op>>>::Metadata,
);

impl<A: AppState, R: ReplicationState<Request<A::Op>>> Service<A, R> {
    pub fn new(app: A, replication: R) -> Self {
        Self {
            replication,
            app,
            replies: Default::default(),
            replicated: None,
            submit_buffer: Default::default(),
            send_buffer: Default::default(),
        }
    }
}

impl<A: AppState, R: ReplicationState<Request<A::Op>>> ServiceState<A> for Service<A, R>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type ServiceSend = R::Send;
    type ServiceMessage = R::Message;
    type Metadata = R::Metadata;
}

impl<A: AppState, R: ReplicationState<Request<A::Op>>> State for Service<A, R>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
    // ServiceMessage<R, A>: std::fmt::Debug,
{
    type Send = Send<Reply<A::Res, R::Metadata>, R::Send>;
    type Output = Never;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(send) = self.send_buffer.pop() {
            return Proceed::Send(send);
        }

        if let Some((requests, metadata)) = &mut self.replicated {
            let Some(request) = requests.pop_front() else {
                self.replicated = None;
                return self.proceed(since_start);
            };
            if let Some(reply) = self.replies.get(&request.client_id)
                && reply.client_seq >= request.client_seq
            {
                return self.proceed(since_start);
            }
            let reply = Reply {
                client_seq: request.client_seq,
                res: self.app.execute(request.op),
                metadata: metadata.clone(),
            };
            self.replies.insert(request.client_id, reply.clone());
            // in some cases `replicated` may contain plenty of requests, also, some kinds
            // of the `execute` above could be costly
            // return immediately without any batch processing to optimize for latency
            return Proceed::Send(Send::Reply(request.client_id, reply));
        }

        while let Some(request) = self.submit_buffer.pop() {
            self.replication.submit(request)
        }
        match self.replication.proceed(since_start) {
            Proceed::Pending(tick_after) => Proceed::Pending(tick_after),
            Proceed::Send(send) => Proceed::Send(Send::Intermediate(send)),
            Proceed::Output(replicated) => {
                let replaced = self
                    .replicated
                    .replace((replicated.logs.into(), replicated.metadata));
                assert!(replaced.is_none());
                self.proceed(since_start)
            }
        }
    }

    type Message = Message<Request<A::Op>, R::Message>;
    fn receive(&mut self, message: Self::Message) {
        // dbg!(&message);
        match message {
            Message::Request(request) => match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => {}
                Some(reply) if reply.client_seq == request.client_seq => self
                    .send_buffer
                    .push(Send::Reply(request.client_id, reply.clone())),
                _ => self.submit_buffer.push(request),
            },
            Message::Intermediate(message) => self.replication.receive(message),
        }
    }
}

pub mod transport {
    use std::{
        collections::HashMap, future::pending, net::SocketAddr, sync::Mutex, time::Duration,
    };

    use bincode::{Decode, Encode};
    use quinn::{Connection, Endpoint, Incoming};
    use tokio::{
        select, spawn,
        sync::mpsc,
        task::JoinHandle,
        time::{Instant, sleep},
        try_join,
    };
    use tokio_util::{sync::CancellationToken, task::TaskTracker};

    use crate::{
        app::AppState,
        crypto::cert::quinn::{client_config, server_config},
        replication::ReplicaIndex,
        state::Proceed,
        transport::{BINCODE_CONFIG, PerformSend, read_loop, run_write, trace_error},
    };

    use super::*;

    pub async fn run_service<A: AppState, R: ReplicationState<Request<A::Op>>>(
        mut service: Service<A, R>,
        replica_index: ReplicaIndex,
        addrs: Vec<SocketAddr>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>
    where
        Service<A, R>: ServiceState<
                A,
                ServiceSend = R::Send,
                ServiceMessage = R::Message,
                Metadata = R::Metadata,
            >,
        Request<A::Op>: Decode<()>,
        R::Message: Decode<()>,
        Reply<A::Res, R::Metadata>: Encode,
        HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>: PerformSend<R::Send>,
    {
        let mut endpoint = Endpoint::server(server_config(), addrs[replica_index as usize])?;
        endpoint.set_default_client_config(client_config());

        let connections = Mutex::new(HashMap::new());
        let active = async {
            for (index, &addr) in addrs.iter().enumerate().skip(replica_index as usize + 1) {
                let connection = endpoint.connect(addr, "server.example")?.await?;
                connection
                    .open_uni()
                    .await?
                    .write_all(&replica_index.to_le_bytes())
                    .await?;
                connections.lock().unwrap().insert(index as _, connection);
            }
            anyhow::Ok(())
        };
        let passive = async {
            for _ in 0..replica_index {
                let connection = endpoint
                    .accept()
                    .await
                    .expect("connection not closed")
                    .await?;
                let mut index = [0; size_of::<ReplicaIndex>()];
                connection
                    .accept_uni()
                    .await?
                    .read_exact(&mut index)
                    .await?;
                connections
                    .lock()
                    .unwrap()
                    .insert(ReplicaIndex::from_le_bytes(index), connection);
            }
            anyhow::Ok(())
        };
        try_join!(active, passive)?;
        let connections = connections.into_inner().unwrap();
        anyhow::ensure!(connections.len() == addrs.len() - 1);
        tracing::info!("replica interconnections established");

        enum Event {
            Accept(Box<Incoming>),
            Message(Vec<u8>),
            Closed(ClientId),
            ReplicationMessage(Vec<u8>),
            Tick,
        }
        let (event_sender, mut event_receiver) = mpsc::channel(1000);

        let mut replica_table = HashMap::new();
        for (index, connection) in connections {
            let task = spawn(trace_error(
                "replica connection read",
                read_loop(
                    connection.clone(),
                    event_sender.clone(),
                    Event::ReplicationMessage,
                    None,
                ),
            ));
            replica_table.insert(index, (connection, task));
        }

        let mut client_table = HashMap::new();
        let write_tracker = TaskTracker::new();

        let start = Instant::now();
        let mut tick_after = service_proceed(
            &mut service,
            start.elapsed(),
            &client_table,
            &replica_table,
            &write_tracker,
        )?;
        loop {
            let tick = async {
                if let Some(tick_after) = tick_after {
                    sleep(tick_after).await
                } else {
                    pending().await
                }
            };
            match select! {
                Some(incoming) = endpoint.accept() => Event::Accept(incoming.into()),
                Some(event) = event_receiver.recv() => event,
                () = tick => Event::Tick,
                () = cancel.cancelled() => break,
            } {
                Event::Accept(incoming) => {
                    let connection = (*incoming).await?;
                    let mut client_id = [0; size_of::<ClientId>()];
                    connection
                        .accept_uni()
                        .await?
                        .read_exact(&mut client_id)
                        .await?;
                    let client_id = ClientId::from_le_bytes(client_id);

                    let task = spawn(trace_error(
                        "connection read",
                        read_loop(
                            connection.clone(),
                            event_sender.clone(),
                            Event::Message,
                            Event::Closed(client_id),
                        ),
                    ));
                    client_table.insert(client_id, (connection, task));
                    continue;
                }
                Event::Closed(client_id) => {
                    client_table.remove(&client_id);
                    continue;
                }
                Event::Message(bytes) => {
                    let (request, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                    anyhow::ensure!(len == bytes.len());
                    service.receive(Message::Request(request))
                }
                Event::ReplicationMessage(bytes) => {
                    let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                    anyhow::ensure!(len == bytes.len());
                    service.receive(Message::Intermediate(message))
                }
                Event::Tick => {}
            }
            tick_after = service_proceed(
                &mut service,
                start.elapsed(),
                &client_table,
                &replica_table,
                &write_tracker,
            )?
        }

        write_tracker.close();
        write_tracker.wait().await;
        if !client_table.is_empty() {
            tracing::warn!("shut down with open client connections")
        }
        for (connection, task) in client_table.into_values() {
            connection.close(0u32.into(), b"service shutting down");
            task.await.unwrap() // not cancelled anywhere and propagate panics
        }
        for (connection, task) in replica_table.into_values() {
            connection.close(0u32.into(), b"service shutting down");
            task.await.unwrap() // not cancelled anywhere and propagate panics
        }
        Ok(())
    }

    fn service_proceed<A: AppState, R: ReplicationState<Request<A::Op>>>(
        service: &mut Service<A, R>,
        since_start: Duration,
        client_table: &HashMap<ClientId, (Connection, JoinHandle<()>)>,
        replica_table: &HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
        write_tracker: &TaskTracker,
    ) -> anyhow::Result<Option<Duration>>
    where
        Service<A, R>: ServiceState<A, ServiceSend = R::Send, Metadata = R::Metadata>,
        Reply<A::Res, R::Metadata>: Encode,
        HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>: PerformSend<R::Send>,
    {
        loop {
            match service.proceed(since_start) {
                Proceed::Pending(tick_after) => break Ok(tick_after),
                Proceed::Send(Send::Reply(client_id, reply)) => {
                    let Some((connection, _)) = client_table.get(&client_id) else {
                        tracing::warn!(%client_id, "client connection not found");
                        continue;
                    };
                    write_tracker.spawn(trace_error(
                        "connection write",
                        run_write(
                            connection.clone(),
                            bincode::encode_to_vec(reply, BINCODE_CONFIG)?,
                        ),
                    ));
                }
                Proceed::Send(Send::Intermediate(send)) => {
                    replica_table.perform(send, write_tracker)?
                }
            }
        }
    }
}
