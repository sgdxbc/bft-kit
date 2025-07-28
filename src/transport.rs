use std::{collections::HashMap, future::pending, net::SocketAddr, pin::pin, time::Duration};

use bincode::{Decode, Encode};
use futures_concurrency::future::{FutureGroup, Race, TryJoin};
use futures_lite::{FutureExt, StreamExt, future::Boxed};
use quinn::{Connection, ConnectionError, Endpoint, Incoming};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::{Instant, sleep},
    try_join,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    Command,
    command::{Execute, ReceiveAction, Service},
    crypto::cert::quinn::{client_config, server_config},
    replica::{ReplicaProtocol, ReplyProtocol},
};

// pub mod tcp;

type Id = u64;

pub async fn run_read(
    connection: Connection,
    sender: Sender<impl Decode<()> + Send + Sync + 'static>,
) -> anyhow::Result<()> {
    let mut stream = connection.accept_uni().await?;
    loop {
        let bytes = stream.read_to_end(1 << 16).await?;
        if bytes.is_empty() {
            break;
        }
        let (message, len) = bincode::decode_from_slice(&bytes, bincode::config::standard())?;
        anyhow::ensure!(len == bytes.len());
        if sender.capacity() == 0 {
            tracing::warn!("read channel full")
        }
        sender.send(message).await?
    }
    Ok(())
}

pub async fn run_write(
    connection: Connection,
    mut receiver: Receiver<Bytes>,
) -> anyhow::Result<()> {
    while let Some(message) = receiver.recv().await {
        let mut stream = connection.open_uni().await?;
        // TODO figure a way to concurrently write into a connection (via multiple
        // streams) if that matters to performance
        stream.write_chunk(message).await?
    }
    Ok(())
}

pub async fn write(
    message: impl Encode,
    senders: impl IntoIterator<Item = &'_ Sender<Bytes>>,
) -> anyhow::Result<()> {
    let bytes = Bytes::from(bincode::encode_to_vec(
        message,
        bincode::config::standard(),
    )?);
    for sender in senders {
        if sender.capacity() == 0 {
            tracing::warn!("write channel full")
        }
        sender.send(bytes.clone()).await?;
    }
    Ok(())
}

async fn spawn<T: Send + 'static>(
    task: impl Future<Output = anyhow::Result<T>> + Send + 'static,
) -> anyhow::Result<T> {
    tokio::spawn(task).await?
}

pub async fn run_transport(
    connection: Connection,
    read_sender: Sender<impl Decode<()> + Send + Sync + 'static>,
    write_receiver: Receiver<Bytes>,
) -> anyhow::Result<()> {
    (
        spawn(run_read(connection.clone(), read_sender)),
        spawn(run_write(connection, write_receiver)),
    )
        .try_join()
        .await?;
    Ok(())
}

// TODO extract client skeleton
// not sure whether that is possible or not since (simple) clients are inline
// implemented in transport
// nonetheless, bootstrapping part is extracted below

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    external_addresses: HashMap<Id, SocketAddr>,
}

pub type TransportTasks = FutureGroup<Boxed<anyhow::Result<()>>>;
pub type WriteSenders = HashMap<Id, Sender<Bytes>>;
pub type Start = (TransportTasks, WriteSenders);

pub async fn start_client<M: Decode<()> + Send + Sync + 'static>(
    id: Id,
    service_config: &ServiceConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<Start> {
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());
    let mut tasks = FutureGroup::new();
    let mut write_senders = HashMap::new();
    for (&replica_id, &addr) in &service_config.external_addresses {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        connection
            .open_uni()
            .await?
            .write_all(&id.to_le_bytes())
            .await?;
        let (write_sender, write_receiver) = mpsc::channel(1000);
        tasks.insert(run_transport(connection, message_sender.clone(), write_receiver).boxed());
        write_senders.insert(replica_id, write_sender);
    }
    Ok((tasks, write_senders))
}

pub async fn run_service<E: Execute, P: ReplyProtocol>(
    mut service: Service<E>,
    external_address: SocketAddr,
    submit_sender: Sender<Command>,
    mut finalized_receiver: Receiver<(Vec<Command>, P::FinalizeMetadata)>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    P::Reply: Encode,
{
    let endpoint = Endpoint::server(server_config(), external_address)?;
    let (request_sender, mut request_receiver) = mpsc::channel(10_000);
    let mut transport_tasks = FutureGroup::new();
    let mut last_finalized_metadata = None;
    let mut write_senders = HashMap::new();
    loop {
        enum Race<M> {
            Accept(Option<Box<Incoming>>),
            RequestRecv(Option<Command>),
            FinalizedRecv(Option<(Vec<Command>, M)>),
            TransportTask(anyhow::Result<()>),
            Cancel,
        }
        use Race::*;
        let transport = async {
            if let Some(result) = transport_tasks.next().await {
                TransportTask(result)
            } else {
                pending().await
            }
        };
        match (
            async { Accept(endpoint.accept().await.map(Into::into)) },
            async { RequestRecv(request_receiver.recv().await) },
            async { FinalizedRecv(finalized_receiver.recv().await) },
            async {
                cancel.cancelled().await;
                Cancel
            },
            transport,
        )
            .race()
            .await
        {
            Cancel => break Ok(()),

            FinalizedRecv(None) => anyhow::bail!("finalized channel closed"),
            TransportTask(Err(err)) => {
                if let Some(ConnectionError::ApplicationClosed(_)) = err.downcast_ref() {
                    // TODO remove corresponding write sender to garbage collect
                    // current evaluation setups only work with one batch of clients so should be
                    // fine to leak the senders
                } else {
                    anyhow::bail!(err)
                }
            }

            // local endpoint and no code path closes it
            Accept(None) => unreachable!("endpoint closed"),
            RequestRecv(None) => unreachable!("read channel closed"),
            TransportTask(Ok(())) => unreachable!("active close of write task"),

            Accept(Some(incoming)) => {
                let connection = (*incoming).await?;
                let mut client_id = [0; size_of::<Id>()];
                connection
                    .accept_uni()
                    .await?
                    .read_exact(&mut client_id)
                    .await?;
                let client_id = Id::from_le_bytes(client_id);
                let (write_sender, write_receiver) = mpsc::channel(100);
                transport_tasks.insert(
                    run_transport(connection, request_sender.clone(), write_receiver).boxed(),
                );
                write_senders.insert(client_id, write_sender);
            }

            RequestRecv(Some(command)) => match service.receive(&command) {
                ReceiveAction::Ignore => {}
                ReceiveAction::Submit => {
                    if submit_sender.capacity() == 0 {
                        tracing::warn!("submit channel full");
                    }
                    submit_sender.send(command).await?
                }
                ReceiveAction::Reply(result) => {
                    let Some(finalized_metadata) = &last_finalized_metadata else {
                        anyhow::bail!("missing finalized metadata")
                    };
                    // maybe not a good idea to silently ignore the missing sender
                    write(
                        P::new_reply(command.seq, result, finalized_metadata),
                        write_senders.get(&(command.client_id.0 as Id)),
                    )
                    .await?;
                }
            },

            FinalizedRecv(Some((commands, metadata))) => {
                for (client_id, seq, result) in service.execute(&commands) {
                    let sender = write_senders.get(&(client_id.0 as Id));
                    if sender.is_none() {
                        // assume the client has closed the connection during service processing
                        // this command and don't care about the result anymore
                        tracing::info!(%client_id, "send to closed connection");
                        continue;
                    }
                    write(P::new_reply(seq, result, &metadata), sender).await?
                }
                last_finalized_metadata = Some(metadata);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    internal_addresses: HashMap<Id, SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. not necessary for QUIC because it allows connect before
    // accept. set longer in higher latency environments (or human action is
    // involved)
    interconnect_delay: Duration,
    tick_interval: Duration,
}

pub async fn start_replica<M: Decode<()> + Send + Sync + 'static>(
    id: Id,
    config: &ReplicaConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<Start> {
    let mut internal_endpoint = Endpoint::server(server_config(), config.internal_addresses[&id])?;
    internal_endpoint.set_default_client_config(client_config());
    let active_task = async {
        let mut connections = HashMap::new();
        for (&i, &addr) in &config.internal_addresses {
            if i <= id {
                continue;
            }
            let connection = internal_endpoint.connect(addr, "server.example")?.await?;
            tracing::debug!(remote = ?connection.remote_address(), "connect");
            connection
                .open_uni()
                .await?
                // to need to `to_le_bytes()` for current u8 based ReplicaId, just future proof
                .write_all(&id.to_le_bytes())
                .await?;
            connections.insert(i, connection);
        }
        anyhow::Ok(connections)
    };
    let passive_task = async {
        tracing::info!(addr = ?internal_endpoint.local_addr(), "start listening");
        let mut connections = HashMap::new();
        for _ in 0..id {
            let connection = internal_endpoint
                .accept()
                .await
                .expect("endpoint not closed")
                .await?;
            tracing::debug!(remote = ?connection.remote_address(), "accept");
            let mut replica_id = [0; size_of::<Id>()];
            connection
                .accept_uni()
                .await?
                .read_exact(&mut replica_id)
                .await?;
            connections.insert(Id::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (mut connections, other_connections) = try_join!(active_task, passive_task)?;
    connections.extend(other_connections);
    anyhow::ensure!(connections.len() == config.internal_addresses.len() - 1);

    let mut tasks = FutureGroup::new();
    let mut write_senders = HashMap::new();
    for (id, connection) in connections {
        let (write_sender, write_receiver) = mpsc::channel(100);
        tasks.insert(Box::pin(run_transport(
            connection,
            message_sender.clone(),
            write_receiver,
        )) as Boxed<_>);
        write_senders.insert(id, write_sender);
    }
    Ok((tasks, write_senders))
}

pub struct ReplicaContext<M> {
    send_buffer: Vec<M>,
    finalize_buffer: Vec<Vec<Command>>,
}

pub async fn run_replica<R: ReplicaProtocol<ReplicaContext<M>, Message = M>, M>(
    mut replica: R,
    id: Id,
    config: ReplicaConfig,
    mut submit_receiver: Receiver<Command>,
    finalized_sender: Sender<(Vec<Command>, R::FinalizeMetadata)>,
) -> anyhow::Result<()>
where
    R::Message: Encode + Decode<()> + Send + Sync + 'static,
    R::FinalizeMetadata: Send + Sync + 'static,
{
    let (message_sender, mut message_receiver) = mpsc::channel(100);
    let (mut transport_tasks, write_senders) = start_replica(id, &config, message_sender).await?;
    let mut context = ReplicaContext {
        send_buffer: Default::default(),
        finalize_buffer: Default::default(),
    };

    replica.init(&mut context);

    let mut next_tick = pin!(sleep(config.tick_interval));
    let mut ticked_at = Instant::now();
    loop {
        enum Race<M> {
            SubmitRecv(Option<Command>),
            MessageRecv(M),
            Sleep,
            TransportTask(anyhow::Result<()>),
        }
        use Race::*;
        let message = async {
            if let Some(message) = message_receiver.recv().await {
                MessageRecv(message)
            } else {
                tracing::warn!("message channel closed");
                pending().await
            }
        };
        let transport = async {
            if let Some(result) = transport_tasks.next().await {
                TransportTask(result)
            } else {
                // it is probably fine to assert unreachable here since "replica" is supposed to
                // be more than one
                tracing::warn!("no transport task exists");
                pending().await
            }
        };
        match (
            async { SubmitRecv(submit_receiver.recv().await) },
            message,
            async {
                next_tick.as_mut().await;
                Sleep
            },
            transport,
        )
            .race()
            .await
        {
            SubmitRecv(None) => break Ok(()),
            TransportTask(Err(err)) => anyhow::bail!(err),
            TransportTask(Ok(())) => unreachable!("active close of write task"),

            SubmitRecv(Some(command)) => replica.submit(command, &mut context),
            MessageRecv(message) => replica.receive(message, &mut context),
            Sleep => {
                replica.tick(ticked_at.elapsed(), &mut context);
                ticked_at = Instant::now();
                next_tick.as_mut().reset(ticked_at + config.tick_interval);
            }
        }
        for message in context.send_buffer.drain(..) {
            write(message, write_senders.values()).await?
        }
        for commands in context.finalize_buffer.drain(..) {
            if finalized_sender.capacity() == 0 {
                tracing::warn!("finalized channel full");
            }
            finalized_sender
                .send((commands, replica.finalize_metadata()))
                .await?
        }
    }
}

mod parse {
    use std::time::Duration;

    use crate::parse::Settings;

    impl TryFrom<Settings> for crate::transport::ServiceConfig {
        type Error = anyhow::Error;

        fn try_from(settings: Settings) -> Result<Self, Self::Error> {
            Ok(Self {
                // if necessary, allow nonconsecutive replica id
                external_addresses: settings
                    .get_values("server_external_address")?
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (i as _, addr))
                    .collect(),
            })
        }
    }

    impl TryFrom<Settings> for super::ReplicaConfig {
        type Error = anyhow::Error;

        fn try_from(settings: Settings) -> Result<Self, Self::Error> {
            Ok(Self {
                // if necessary, allow nonconsecutive replica id
                internal_addresses: settings
                    .get_values("server_internal_address")?
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (i as _, addr))
                    .collect(),
                interconnect_delay: Duration::from_secs_f32(
                    settings
                        .get_option("server_interconnect_delay")?
                        .unwrap_or(0.),
                ),
                tick_interval: Duration::from_secs_f32(settings.get("server_tick_interval")?),
            })
        }
    }
}
