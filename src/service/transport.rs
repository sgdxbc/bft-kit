use std::{collections::HashMap, future::pending, net::SocketAddr, sync::Mutex, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint, Incoming};
use tokio::{
    select,
    sync::mpsc,
    time::{Instant, sleep},
    try_join,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    crypto::cert::quinn::{client_config, server_config},
    service::{
        ClientId, ReplicaIndex, ReplicationState, Reply, Request, ServiceMessage, ServiceSend,
    },
    state::{AppState, Never, Proceed, State},
};

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

pub trait ReplicationSend {
    fn apply(self, replica_connections: &HashMap<ReplicaIndex, Connection>);
}

pub async fn run_replicated_service<
    R: ReplicationState<A::Op>,
    A: AppState,
    S: State<Send = ServiceSend<R, A>, Output = Never, Message = ServiceMessage<R, A>>,
>(
    mut service: S,
    replica_index: ReplicaIndex,
    addrs: Vec<SocketAddr>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    Request<A::Op>: Decode<()>,
    Reply<A::Res, R::Metadata>: Encode,
    R::Message: Decode<()>,
    R::Send: ReplicationSend,
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
    let replica_connections = connections.into_inner().unwrap();
    anyhow::ensure!(replica_connections.len() == addrs.len() - 1);

    let tracker = TaskTracker::new();

    enum Event {
        Accept(Box<Incoming>),
        Message(Vec<u8>),
        Closed(ClientId),
        ReplicationMessage(Vec<u8>),
        Tick,
        Cancel,
    }
    let (event_sender, mut event_receiver) = mpsc::channel(1000);
    for connection in replica_connections.values() {
        tracker.spawn(trace_error(
            "replica connection read",
            read_loop(
                connection.clone(),
                event_sender.clone(),
                Event::ReplicationMessage,
                None,
            ),
        ));
    }

    let mut client_connections = HashMap::new();

    service.tick(Duration::ZERO);
    let mut last_tick = Instant::now();
    let mut tick_after = service_proceed(
        &mut service,
        &client_connections,
        &replica_connections,
        &tracker,
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
            () = cancel.cancelled() => Event::Cancel,
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

                tracker.spawn(trace_error(
                    "connection read",
                    read_loop(
                        connection.clone(),
                        event_sender.clone(),
                        Event::Message,
                        Event::Closed(client_id),
                    ),
                ));
                client_connections.insert(client_id, connection);
            }
            Event::Message(bytes) => {
                let (request, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                service.receive(ServiceMessage::Request(request));
                tick_after = service_proceed(
                    &mut service,
                    &client_connections,
                    &replica_connections,
                    &tracker,
                )?
            }
            Event::Closed(client_id) => {
                client_connections.remove(&client_id);
            }
            Event::ReplicationMessage(bytes) => {
                let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                service.receive(ServiceMessage::Replication(message));
                tick_after = service_proceed(
                    &mut service,
                    &client_connections,
                    &replica_connections,
                    &tracker,
                )?
            }
            Event::Tick => {
                service.tick(last_tick.elapsed());
                last_tick = Instant::now();
                tick_after = service_proceed(
                    &mut service,
                    &client_connections,
                    &replica_connections,
                    &tracker,
                )?
            }
            Event::Cancel => break,
        }
    }
    tracker.close();
    if !client_connections.is_empty() {
        tracing::warn!("shut down with open client connections")
    }
    for connection in client_connections.values() {
        connection.close(0u32.into(), b"service shutting down")
    }
    for connection in replica_connections.values() {
        connection.close(0u32.into(), b"service shutting down")
    }
    tracker.wait().await;
    Ok(())
}

fn service_proceed<
    R: ReplicationState<A::Op>,
    A: AppState,
    S: State<Send = ServiceSend<R, A>, Output = Never, Message = ServiceMessage<R, A>>,
>(
    service: &mut S,
    client_connections: &HashMap<ClientId, Connection>,
    replica_connections: &HashMap<ReplicaIndex, Connection>,
    tracker: &TaskTracker,
) -> anyhow::Result<Option<Duration>>
where
    Reply<A::Res, R::Metadata>: Encode,
    R::Send: ReplicationSend,
{
    loop {
        match service.proceed() {
            Proceed::Pending(tick_after) => break Ok(tick_after),
            Proceed::Send(ServiceSend::Reply(client_id, reply)) => {
                let Some(connection) = client_connections.get(&client_id) else {
                    tracing::warn!(%client_id, "client connection not found");
                    continue;
                };
                tracker.spawn(trace_error(
                    "connection write",
                    run_write(
                        connection.clone(),
                        bincode::encode_to_vec(reply, BINCODE_CONFIG)?,
                    ),
                ));
            }
            Proceed::Send(ServiceSend::Replication(send)) => send.apply(replica_connections),
        }
    }
}

async fn read_loop<E: Send + Sync + 'static>(
    connection: Connection,
    event_sender: mpsc::Sender<E>,
    into_event: impl Fn(Vec<u8>) -> E,
    close_event: impl Into<Option<E>>,
) -> anyhow::Result<()> {
    loop {
        let mut stream = match connection.accept_uni().await {
            Ok(stream) => stream,
            Err(ConnectionError::LocallyClosed | ConnectionError::ApplicationClosed(_)) => break,
            Err(err) => Err(err)?,
        };
        let message = stream.read_to_end(1 << 16).await?;
        if event_sender.capacity() == 0 {
            tracing::warn!("message channel congested")
        }
        event_sender
            .send(into_event(message))
            .await
            .map_err(|_| anyhow::format_err!("message channel closed, stopping"))?
    }
    if let Some(close_event) = close_event.into() {
        event_sender.send(close_event).await?
    } else {
        tracing::debug!("connection closed without close event")
    }
    Ok(())
}

async fn run_write(connection: Connection, message: Vec<u8>) -> anyhow::Result<()> {
    connection.open_uni().await?.write_all(&message).await?;
    Ok(())
}

async fn trace_error<T>(label: &str, task: impl Future<Output = anyhow::Result<T>>) {
    if let Err(err) = task.await {
        tracing::error!(%label, %err)
    }
}

impl ReplicationSend for Never {
    fn apply(self, _replica_connections: &HashMap<ReplicaIndex, Connection>) {
        unreachable!()
    }
}
