use std::{
    collections::{HashMap, hash_map::Entry},
    fmt::Debug,
    future::pending,
    net::SocketAddr,
    sync::Mutex,
    time::Duration,
};

use bincode::{Decode, Encode};
use quinn::{Connection, Endpoint, Incoming};
use rocksdb::{DB, properties::LIVE_SST_FILES_SIZE};
use tokio::{
    select, spawn,
    sync::mpsc,
    task::{JoinHandle, spawn_blocking, yield_now},
    time::{Instant, sleep},
    try_join,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken, task::TaskTracker};

use crate::{
    app::DataShardingApp,
    crypto::cert::quinn::{client_config, server_config},
    replication::{ReplicaIndex, ReplicationState, transport::ReplicaTable},
    service::{ClientId, Effect, Message, Reply, Request, ServiceState, Store},
    state::{Action, State as _},
    transport::{BINCODE_CONFIG, PerformSend, read_loop, run_write, trace_error},
};

use super::{
    BigService, BigServiceLog, ServiceEffect, ServiceMessage,
    storage::{Dest, ShardedStorageSend, StorageState},
};

struct ConnectionTables {
    client: HashMap<ClientId, (Connection, JoinHandle<()>)>,
    replica: HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
    storage: HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>,
}

enum Event {
    Accept(Box<Incoming>),
    Closed(ClientId),
    Message(Vec<u8>),
    ReplicationMessage(Vec<u8>),
    StorageMessage(Vec<u8>),
    StoreRead(String, Bytes),
    StoreWrite(String),
    Tick,
}

pub async fn run_service<
    A: DataShardingApp,
    R: ReplicationState<BigServiceLog<A, S>>,
    S: StorageState,
>(
    mut service: BigService<A, R, S>,
    replica_index: ReplicaIndex,
    addrs: Vec<SocketAddr>,
    cancel: CancellationToken,
    send_reply: bool,
) -> anyhow::Result<()>
where
    BigService<A, R, S>: ServiceState<
            A,
            ServiceEffect = ServiceEffect<R, S>,
            ServiceMessage = ServiceMessage<R::Message, S::Message>,
            Metadata = R::Metadata,
        >,
    Request<A::Op>: Decode<()>,
    R::Message: Decode<()>,
    S::Message: Decode<()>,
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>:
        PerformSend<R::Effect> + PerformSend<S::StorageSend>,
    R::Message: Debug,
    S::Message: Debug,
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

    let temp_dir = tempfile::Builder::new().prefix("big-storage").tempdir()?;
    let db = DB::open_default(temp_dir.path())?;
    // service.init_store(&init_shard, &mut db)?;
    tracing::info!("store initialized");

    let (store_command_sender, store_command_receiver) = mpsc::channel(100);
    let store_task = spawn_blocking({
        let event_sender = event_sender.clone();
        move || store_task(db, store_command_receiver, event_sender)
    });

    let write_tracker = TaskTracker::new();
    let start = Instant::now();
    let mut tick_after = service_proceed(
        &mut service,
        Duration::ZERO,
        &connection_tables,
        &store_command_sender,
        &write_tracker,
        &cancel,
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
            Event::StoreRead(key, value) => service.put_complete(key, value),
            Event::StoreWrite(key) => service.get_complete(key),
            Event::Tick => {}
        }
        tick_after = service_proceed(
            &mut service,
            start.elapsed(),
            &connection_tables,
            &store_command_sender,
            &write_tracker,
            &cancel,
            send_reply,
        )
        .await?
    }

    let elapsed = start.elapsed();
    drop(store_command_sender);
    store_task.await??;
    temp_dir.close()?;

    write_tracker.close();
    write_tracker.wait().await;
    if !connection_tables.client.is_empty() {
        tracing::warn!(
            "shut down with {} open client connections",
            connection_tables.client.len()
        )
    }
    for (connection, task) in connection_tables
        .client
        .into_values()
        .chain(connection_tables.replica.into_values())
        .chain(connection_tables.storage.into_values())
    {
        connection.close(0u32.into(), b"service shutting down");
        task.await.unwrap() // not cancelled anywhere and propagate panics
    }

    let latency_stat = format!(
        "{} ops, tput {:.2} ops/sec, mean {:?}, 50th {:?}",
        service.execute_latencies.len(),
        service.execute_latencies.len() as f32 / elapsed.as_secs_f32(),
        Duration::from_nanos(service.execute_latencies.mean() as _),
        Duration::from_nanos(service.execute_latencies.value_at_quantile(0.5))
    );
    tracing::info!("replica {replica_index}\n  {latency_stat}");
    Ok(())
}

async fn service_proceed<
    A: DataShardingApp,
    R: ReplicationState<BigServiceLog<A, S>>,
    S: StorageState,
>(
    service: &mut BigService<A, R, S>,
    since_start: Duration,
    connection_tables: &ConnectionTables,
    store_command_sender: &mpsc::Sender<Store>,
    write_tracker: &TaskTracker,
    cancel: &CancellationToken,
    send_reply: bool,
) -> anyhow::Result<Option<Duration>>
where
    BigService<A, R, S>:
        ServiceState<A, ServiceEffect = ServiceEffect<R, S>, Metadata = R::Metadata>,
    Reply<A::Res, R::Metadata>: Encode,
    HashMap<ReplicaIndex, (Connection, JoinHandle<()>)>:
        PerformSend<R::Effect> + PerformSend<S::StorageSend>,
{
    loop {
        if cancel.is_cancelled() {
            break Ok(None); // consider better returned value
        }
        match service.proceed(since_start) {
            Action::Pending(tick_after) => break Ok(tick_after),

            Action::Perform(Effect::Reply(..)) if !send_reply => {}
            Action::Perform(Effect::Reply(client_id, reply)) => {
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

            Action::Perform(Effect::Intermediate(ServiceEffect::Replication(send))) => {
                connection_tables.replica.perform(send, write_tracker)?
            }
            Action::Perform(Effect::Intermediate(ServiceEffect::StorageSend(send))) => {
                connection_tables.storage.perform(send, write_tracker)?
            }

            Action::Perform(Effect::Intermediate(ServiceEffect::Store(store))) => {
                if store_command_sender.capacity() == 0 {
                    tracing::warn!("store command sender congested");
                }
                store_command_sender.send(store).await?
            }
        }
        yield_now().await
    }
}

impl<T: ReplicaTable> PerformSend<ShardedStorageSend> for T {
    fn perform(
        &self,
        (dest, message): ShardedStorageSend,
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

fn store_task(
    db: DB,
    mut command_receiver: mpsc::Receiver<Store>,
    event_sender: mpsc::Sender<Event>,
) -> anyhow::Result<()> {
    while let Some(command) = command_receiver.blocking_recv() {
        let event = match command {
            Store::Get(key) => {
                let Some(value) = db.get(&key)? else {
                    tracing::warn!(%key, "key not found");
                    continue;
                };
                Event::StoreRead(key, value.into())
            }
            Store::Put(key, value) => {
                db.put(&key, value)?;
                Event::StoreWrite(key)
            }
            Store::Delete(key) => {
                db.delete(key)?;
                continue;
            }
        };
        event_sender
            .blocking_send(event)
            .map_err(|_| anyhow::format_err!("store read event channel closed, stopping"))?
    }
    let total_size = db.property_int_value(LIVE_SST_FILES_SIZE)?;
    tracing::info!(?total_size, "live SST files size");
    Ok(())
}
