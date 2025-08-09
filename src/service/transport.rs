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
    app::AppState,
    crypto::cert::quinn::{client_config, server_config},
    replication::{ReplicaIndex, transport::ReplicaTable},
    service::{ClientId, ReplicationState, Reply, Request, ServiceMessage, ServiceSend},
    state::Proceed,
    transport::{BINCODE_CONFIG, PerformSend, read_loop, run_write, trace_error},
};

use super::ServiceState;

pub async fn run_replicated_service<
    S: ServiceState<A, R>,
    A: AppState,
    R: ReplicationState<S::Log>,
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

fn service_proceed<S: ServiceState<A, R>, A: AppState, R: ReplicationState<S::Log>>(
    service: &mut S,
    since_start: Duration,
    client_table: &HashMap<ClientId, (Connection, JoinHandle<()>)>,
    replica_table: &HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
    write_tracker: &TaskTracker,
) -> anyhow::Result<Option<Duration>>
where
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>: PerformSend<R::Send>,
{
    loop {
        match service.proceed(since_start) {
            Proceed::Pending(tick_after) => break Ok(tick_after),
            Proceed::Send(ServiceSend::Reply(client_id, reply)) => {
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
            Proceed::Send(ServiceSend::Replication(send)) => {
                replica_table.perform(send, write_tracker)?
            }
        }
    }
}

impl ReplicaTable for HashMap<ReplicaIndex, (Connection, JoinHandle<()>)> {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection> {
        self.get(&index).map(|(connection, _)| connection)
    }

    fn get_all(&self) -> impl Iterator<Item = &Connection> {
        self.values().map(|(connection, _)| connection)
    }
}
