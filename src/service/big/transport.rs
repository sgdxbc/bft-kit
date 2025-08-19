use std::{
    collections::{HashMap, hash_map::Entry},
    future::pending,
    net::SocketAddr,
    sync::Mutex,
    time::Duration,
};

use bincode::{Decode, Encode};
use quinn::{Connection, Endpoint, Incoming};
use tokio::{
    fs, select, spawn,
    sync::mpsc,
    task::{JoinHandle, yield_now},
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

struct ConnectionTables {
    client: HashMap<ClientId, (Connection, JoinHandle<()>)>,
    replica: HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
    storage: HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
}

pub async fn run_service<
    A: DataShardingApp,
    R: ReplicationState<Request<A::Op>>,
    S: StorageState<A::Shard>,
>(
    mut service: Service<A, R, S>,
    replica_index: ReplicaIndex,
    addrs: Vec<SocketAddr>,
    cancel: CancellationToken,
    storage_dir: impl AsRef<str>,
    send_reply: bool,
) -> anyhow::Result<()>
where
    Service<A, R, S>: ServiceState<
            A,
            ServiceSend = ServiceSend<R, S>,
            ServiceMessage = ServiceMessage<R::Message, S::Message>,
            Metadata = R::Metadata,
        >,
    Request<A::Op>: Decode<()>,
    R::Message: Decode<()>,
    S::Message: Decode<()>,
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>:
        PerformSend<R::Send> + PerformSend<S::Send>,
    R::Message: std::fmt::Debug,
    S::Message: std::fmt::Debug,
{
    let mut endpoint = Endpoint::server(server_config(), addrs[replica_index as usize])?;
    endpoint.set_default_client_config(client_config());

    let connections = Mutex::new((HashMap::new(), HashMap::new()));
    let active = async {
        for (index, &addr) in addrs.iter().enumerate().skip(replica_index as usize + 1) {
            let replica_connection = endpoint.connect(addr, "server.example")?.await?;
            replica_connection
                .open_uni()
                .await?
                .write_all(&replica_index.to_le_bytes())
                .await?;
            connections
                .lock()
                .unwrap()
                .0
                .insert(index as _, replica_connection);

            let storage_connection = endpoint.connect(addr, "server.example")?.await?;
            storage_connection
                .open_uni()
                .await?
                .write_all(&replica_index.to_le_bytes())
                .await?;
            connections
                .lock()
                .unwrap()
                .1
                .insert(index as _, storage_connection);
        }
        anyhow::Ok(())
    };
    let passive = async {
        for _ in 0..replica_index * 2 {
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
            let index = ReplicaIndex::from_le_bytes(index);
            let (replica_connections, storage_connections) = &mut *connections.lock().unwrap();
            if let Entry::Vacant(entry) = replica_connections.entry(index) {
                entry.insert(connection);
            } else {
                storage_connections.insert(index, connection);
            }
        }
        anyhow::Ok(())
    };
    try_join!(active, passive)?;
    let (replica_connections, storage_connections) = connections.into_inner().unwrap();
    anyhow::ensure!(replica_connections.len() == addrs.len() - 1);
    anyhow::ensure!(storage_connections.len() == addrs.len() - 1);

    enum Event {
        Accept(Box<Incoming>),
        Message(Vec<u8>),
        Closed(ClientId),
        ReplicationMessage(Vec<u8>),
        StorageMessage(Vec<u8>),
        Tick,
    }
    let (event_sender, mut event_receiver) = mpsc::channel(1000);

    let mut replica_table = HashMap::new();
    for (index, connection) in replica_connections {
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
    let mut storage_table = HashMap::new();
    for (index, connection) in storage_connections {
        let task = spawn(trace_error(
            "storage connection read",
            read_loop(
                connection.clone(),
                event_sender.clone(),
                Event::StorageMessage,
                None,
            ),
        ));
        storage_table.insert(index, (connection, task));
    }
    let mut connection_tables = ConnectionTables {
        client: Default::default(),
        replica: replica_table,
        storage: storage_table,
    };
    tracing::info!("interconnections established");

    let write_tracker = TaskTracker::new();

    let storage_dir = storage_dir.as_ref();
    if let Err(err) = fs::remove_dir_all(storage_dir).await {
        tracing::debug!(%replica_index, %err)
    } else {
        tracing::warn!("removed previous storage directory")
    }
    fs::create_dir(storage_dir).await?;

    let start = Instant::now();
    let mut tick_after = service_proceed(
        &mut service,
        start.elapsed(),
        &connection_tables,
        &write_tracker,
        &cancel,
        storage_dir,
        send_reply,
    )
    .await?;
    tracing::info!(%replica_index, "enter event loop");
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
                connection_tables
                    .client
                    .insert(client_id, (connection, task));
                continue;
            }
            Event::Closed(client_id) => {
                connection_tables.client.remove(&client_id);
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
                tracing::trace!(%replica_index, ?message);
                service.receive(Message::Intermediate(ServiceMessage::Replication(message)))
            }
            Event::StorageMessage(bytes) => {
                let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                tracing::trace!(%replica_index, ?message);
                service.receive(Message::Intermediate(ServiceMessage::Storage(message)))
            }
            Event::Tick => {}
        }
        tick_after = service_proceed(
            &mut service,
            start.elapsed(),
            &connection_tables,
            &write_tracker,
            &cancel,
            storage_dir,
            send_reply,
        )
        .await?
    }

    let elapsed = start.elapsed();
    write_tracker.close();
    write_tracker.wait().await;
    if !connection_tables.client.is_empty() {
        tracing::warn!("shut down with open client connections")
    }
    for (connection, task) in connection_tables.client.into_values() {
        connection.close(0u32.into(), b"service shutting down");
        task.await.unwrap() // not cancelled anywhere and propagate panics
    }
    for (connection, task) in connection_tables.replica.into_values() {
        connection.close(0u32.into(), b"service shutting down");
        task.await.unwrap() // not cancelled anywhere and propagate panics
    }
    for (connection, task) in connection_tables.storage.into_values() {
        connection.close(0u32.into(), b"service shutting down");
        task.await.unwrap() // not cancelled anywhere and propagate panics
    }

    let latency_stat = format!(
        "{} ops, tput {:.2} ops/sec, 50th {:?}",
        service.execute_latencies.len(),
        service.execute_latencies.len() as f32 / elapsed.as_secs_f32(),
        Duration::from_nanos(service.execute_latencies.value_at_quantile(0.5))
    );
    let mut total_size = 0;
    let mut read_dir = fs::read_dir(storage_dir).await?;
    while let Some(entry) = read_dir.next_entry().await? {
        let metadata = entry.metadata().await?;
        if metadata.is_file() {
            total_size += metadata.len()
        }
    }
    fs::remove_dir_all(storage_dir).await?;
    tracing::info!("Replica {replica_index}\n  {latency_stat}\n  storage size {total_size}");
    Ok(())
}

async fn service_proceed<
    A: DataShardingApp,
    R: ReplicationState<Request<A::Op>>,
    S: StorageState<A::Shard>,
>(
    service: &mut Service<A, R, S>,
    since_start: Duration,
    connection_tables: &ConnectionTables,
    write_tracker: &TaskTracker,
    cancel: &CancellationToken,
    storage_dir: &str,
    send_reply: bool,
) -> anyhow::Result<Option<Duration>>
where
    Service<A, R, S>: ServiceState<A, ServiceSend = ServiceSend<R, S>, Metadata = R::Metadata>,
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>:
        PerformSend<R::Send> + PerformSend<S::Send>,
{
    loop {
        if cancel.is_cancelled() {
            break Ok(None); // consider better returned value
        }
        match service.proceed(since_start) {
            Proceed::Pending(tick_after) => break Ok(tick_after),
            Proceed::Send(Send::Reply(..)) if !send_reply => {}
            Proceed::Send(Send::Reply(client_id, reply)) => {
                let Some((connection, _)) = connection_tables.client.get(&client_id) else {
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
            Proceed::Send(Send::Intermediate(ServiceSend::Replication(send))) => {
                connection_tables.replica.perform(send, write_tracker)?
            }
            Proceed::Send(Send::Intermediate(ServiceSend::Storage(send))) => {
                connection_tables.storage.perform(send, write_tracker)?
            }
            // TODO concurrent to main proceed loop for better performance
            Proceed::Output(Output::Read(key)) => {
                let value = fs::read(format!("{storage_dir}/{key}")).await?;
                service.read_ok(key, value)
            }
            Proceed::Output(Output::Write(key, value)) => {
                fs::write(format!("{storage_dir}/{key}"), value).await?;
                service.write_ok(key)
            }
        }
        yield_now().await
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
        let label = "storage connection write";
        match dest {
            Dest::One(index) => {
                let Some(connection) = self.get(index) else {
                    anyhow::bail!("unknown replica index {index}");
                };
                send_tracker.spawn(trace_error(label, run_write(connection.clone(), bytes)));
            }
            Dest::Multi(indices) => {
                for index in indices {
                    let Some(connection) = self.get(index) else {
                        anyhow::bail!("unknown replica index {index}");
                    };
                    send_tracker.spawn(trace_error(
                        label,
                        run_write(connection.clone(), bytes.clone()),
                    ));
                }
            }
            Dest::All => {
                for connection in self.get_all() {
                    send_tracker.spawn(trace_error(
                        label,
                        run_write(connection.clone(), bytes.clone()),
                    ));
                }
            }
        }
        Ok(())
    }
}
