use std::{collections::HashMap, future::pending, net::SocketAddr, sync::Mutex, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, Endpoint, Incoming};
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
    transport::{BINCODE_CONFIG, ReplicationSend, read_loop, run_write, trace_error},
};

pub async fn run_replicated_service<
    S: State<Send = ServiceSend<R, A>, Output = Never, Message = ServiceMessage<R, A>>,
    R: ReplicationState<A::Op>,
    A: AppState,
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

    let start = Instant::now();
    let mut tick_after = service_proceed(
        &mut service,
        start.elapsed(),
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
                continue;
            }
            Event::Closed(client_id) => {
                client_connections.remove(&client_id);
                continue;
            }
            Event::Message(bytes) => {
                let (request, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                service.receive(ServiceMessage::Request(request))
            }
            Event::ReplicationMessage(bytes) => {
                let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                service.receive(ServiceMessage::Replication(message))
            }
            Event::Tick => {}
        }
        tick_after = service_proceed(
            &mut service,
            start.elapsed(),
            &client_connections,
            &replica_connections,
            &tracker,
        )?
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
    S: State<Send = ServiceSend<R, A>, Output = Never, Message = ServiceMessage<R, A>>,
    R: ReplicationState<A::Op>,
    A: AppState,
>(
    service: &mut S,
    since_start: Duration,
    client_connections: &HashMap<ClientId, Connection>,
    replica_connections: &HashMap<ReplicaIndex, Connection>,
    tracker: &TaskTracker,
) -> anyhow::Result<Option<Duration>>
where
    Reply<A::Res, R::Metadata>: Encode,
    R::Send: ReplicationSend,
{
    loop {
        match service.proceed(since_start) {
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
            Proceed::Send(ServiceSend::Replication(send)) => {
                send.apply(replica_connections, tracker)
            }
        }
    }
}
