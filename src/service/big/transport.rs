use std::{collections::HashMap, future::pending, net::SocketAddr, sync::Mutex, time::Duration};

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
    crypto::cert::quinn::{client_config, server_config},
    replication::{ReplicaIndex, transport::ReplicaTable},
    state::Proceed,
    transport::{BINCODE_CONFIG, PerformSend, read_loop, run_write, trace_error},
};

use super::*;

pub async fn run_service<
    A: DataShardingApp,
    R: ReplicationState<Request<A::Op>>,
    S: StorageState<A::Shard>,
>(
    mut service: Service<A, R, S>,
    replica_index: ReplicaIndex,
    addrs: Vec<SocketAddr>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    Service<A, R, S>: ServiceState<
            A,
            ServiceSend = ServiceSend<R, S>,
            ServiceMessage = ServiceMessage<R, S>,
            Metadata = R::Metadata,
        >,
    ServiceMessage<R, S>: Decode<()>,
    Request<A::Op>: Decode<()>,
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>: PerformSend<ServiceSend<R, S>>,
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

fn service_proceed<
    A: DataShardingApp,
    R: ReplicationState<Request<A::Op>>,
    S: StorageState<A::Shard>,
>(
    service: &mut Service<A, R, S>,
    since_start: Duration,
    client_table: &HashMap<ClientId, (Connection, JoinHandle<()>)>,
    replica_table: &HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
    write_tracker: &TaskTracker,
) -> anyhow::Result<Option<Duration>>
where
    Service<A, R, S>: ServiceState<A, ServiceSend = ServiceSend<R, S>, Metadata = R::Metadata>,
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>: PerformSend<ServiceSend<R, S>>,
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

impl<T, R: State, S: State> PerformSend<ServiceSend<R, S>> for T
where
    T: PerformSend<R::Send> + PerformSend<S::Send>,
{
    fn perform(&self, send: ServiceSend<R, S>, send_tracker: &TaskTracker) -> anyhow::Result<()> {
        match send {
            ServiceSend::Replication(send) => {
                PerformSend::<R::Send>::perform(self, send, send_tracker)
            }
            ServiceSend::Storage(send) => PerformSend::<S::Send>::perform(self, send, send_tracker),
        }
    }
}

impl<T: ReplicaTable, S> PerformSend<ShardedStorageSend<S>> for T
where
    ShardedStorageMessage<S>: Encode,
{
    fn perform(
        &self,
        (dest, message): ShardedStorageSend<S>,
        send_tracker: &TaskTracker,
    ) -> anyhow::Result<()> {
        let bytes = bincode::encode_to_vec(message, BINCODE_CONFIG)?;
        match dest {
            Dest::One(index) => {
                let Some(connection) = self.get(index) else {
                    anyhow::bail!("unknown replica index {index}");
                };
                send_tracker.spawn(trace_error(
                    "replica connection write",
                    run_write(connection.clone(), bytes),
                ));
            }
            Dest::Multi(indices) => {
                for index in indices {
                    let Some(connection) = self.get(index) else {
                        anyhow::bail!("unknown replica index {index}");
                    };
                    send_tracker.spawn(trace_error(
                        "replica connection write",
                        run_write(connection.clone(), bytes.clone()),
                    ));
                }
            }
            Dest::All => {
                for connection in self.get_all() {
                    send_tracker.spawn(trace_error(
                        "replica connection write",
                        run_write(connection.clone(), bytes.clone()),
                    ));
                }
            }
        }
        Ok(())
    }
}
