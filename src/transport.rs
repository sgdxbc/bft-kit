use std::{collections::HashMap, net::SocketAddr, pin::pin, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint, Incoming};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep},
    try_join,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    Command,
    command::{ClientSeq, Execute, ReceiveAction, ServiceState},
    crypto::cert::quinn::{client_config, server_config},
    replica::ReplicaProtocol,
};

// pub mod tcp;

type Id = u64;

pub struct Transport {
    // read_tasks never return Ok, while write_tasks return Ok when write channel is
    // closed. this fact is not leveraged by current abstraction, but saving this
    // read/write task separation just in case it becomes useful
    read_tasks: JoinSet<anyhow::Result<()>>,
    write_tasks: JoinSet<anyhow::Result<()>>,
}

impl Transport {
    pub fn new() -> Self {
        Self {
            read_tasks: JoinSet::new(),
            write_tasks: JoinSet::new(),
        }
    }

    pub fn add_connection<RM: Decode<()> + Send + Sync + 'static>(
        &mut self,
        connection: Connection,
        read_sender: Sender<RM>,
        mut write_receiver: Receiver<Bytes>,
    ) {
        self.read_tasks.spawn({
            let connection = connection.clone();
            async move {
                loop {
                    let mut stream = connection.accept_uni().await?;
                    let bytes = stream.read_to_end(1 << 16).await?;
                    let (message, len) =
                        bincode::decode_from_slice(&bytes, bincode::config::standard())?;
                    anyhow::ensure!(len == bytes.len());
                    if read_sender.capacity() == 0 {
                        tracing::warn!("read channel full")
                    }
                    read_sender.send(message).await?;
                }
            }
        });
        self.write_tasks.spawn(async move {
            while let Some(message) = write_receiver.recv().await {
                let mut stream = connection.open_uni().await?;
                // TODO figure a way to concurrently write into a connection (via multiple
                // streams) if that matters to performance
                stream.write_chunk(message).await?
            }
            Ok(())
        });
    }

    pub async fn write<WM: Encode>(
        message: WM,
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

    pub async fn select(&mut self) -> Option<anyhow::Result<()>> {
        async fn join_next(
            tasks: &mut JoinSet<anyhow::Result<()>>,
            allow_exit: bool,
        ) -> Option<anyhow::Result<()>> {
            Some(match (tasks.join_next().await, allow_exit) {
                (Some(Ok(Ok(()))), true) => Ok(()),
                (Some(Ok(Ok(()))), false) => unreachable!(),
                (Some(Err(err)), _) => Err(err.into()),
                (Some(Ok(Err(err))), _) => Err(err),
                (None, _) => return None,
            })
        }
        tokio::select! {
            Some(result) = join_next(&mut self.read_tasks, false) => Some(result),
            Some(result) = join_next(&mut self.write_tasks, true) => Some(result),
            else => None,
        }
    }
}

impl Default for Transport {
    fn default() -> Self {
        Self::new()
    }
}

// TODO extract client skeleton
// not sure whether that is possible or not since (simple) clients are inline
// implemented in transport
// nonetheless, bootstrapping part is extracted below

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub external_addresses: HashMap<Id, SocketAddr>,
}

pub type TransportAndSenders = (Transport, HashMap<Id, Sender<Bytes>>);

pub async fn start_client<M: Decode<()> + Send + Sync + 'static>(
    id: Id,
    service_config: ServiceConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<TransportAndSenders> {
    let mut transport = Transport::new();
    let mut write_senders = HashMap::new();
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());
    for (&replica_id, &addr) in &service_config.external_addresses {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        connection
            .open_uni()
            .await?
            .write_all(&id.to_le_bytes())
            .await?;
        let (write_sender, write_receiver) = mpsc::channel(1000);
        transport.add_connection(connection, message_sender.clone(), write_receiver);
        write_senders.insert(replica_id, write_sender);
    }
    Ok((transport, write_senders))
}

pub trait ReplyProtocol {
    type FinalizeMetadata;
    type Reply;

    fn new_reply(
        seq: ClientSeq,
        result: Vec<u8>,
        finalize_metadata: &Self::FinalizeMetadata,
    ) -> Self::Reply;
}

pub async fn run_service<E: Execute, P: ReplyProtocol>(
    mut service: ServiceState<E>,
    external_address: SocketAddr,
    submit_sender: Sender<Command>,
    mut finalized_receiver: Receiver<(Vec<Command>, P::FinalizeMetadata)>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    P::Reply: Encode,
{
    let endpoint = Endpoint::server(server_config(), external_address)?;
    let mut transport = Transport::new();
    let mut last_finalized_metadata = None;
    let (message_sender, mut message_receiver) = mpsc::channel(10_000);
    let mut write_senders = HashMap::new();
    loop {
        enum Select<M> {
            Accept(Option<Box<Incoming>>),
            MessageRecv(Option<Command>),
            FinalizedRecv(Option<(Vec<Command>, M)>),
            #[allow(clippy::enum_variant_names)]
            TransportSelect(anyhow::Result<()>),
            Cancel,
        }
        use Select::*;
        match tokio::select! {
            accept = endpoint.accept() => Accept(accept.map(Into::into)),
            command = message_receiver.recv() => MessageRecv(command),
            finalized = finalized_receiver.recv() => FinalizedRecv(finalized),
            Some(result) = transport.select() => TransportSelect(result),
            () = cancel.cancelled() => Cancel,
        } {
            Cancel => break Ok(()),
            FinalizedRecv(None) => anyhow::bail!("finalized channel closed"),
            Accept(None) => unreachable!("endpoint closed"),
            MessageRecv(None) => unreachable!("read channel closed"),
            TransportSelect(Ok(())) => unreachable!("active close of write task"),
            TransportSelect(Err(err)) => {
                if let Some(ConnectionError::ApplicationClosed(_)) = err.downcast_ref() {
                    // TODO remove corresponding write sender to garbage collect
                    // current evaluation setups only work with one batch of clients so should be
                    // fine to leak the senders
                } else {
                    anyhow::bail!(err)
                }
            }
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
                transport.add_connection(connection, message_sender.clone(), write_receiver);
                write_senders.insert(client_id, write_sender);
            }
            MessageRecv(Some(command)) => match service.receive(&command) {
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
                    Transport::write(
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
                    Transport::write(P::new_reply(seq, result, &metadata), sender).await?
                }
                last_finalized_metadata = Some(metadata);
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    pub internal_addresses: HashMap<Id, SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. not necessary for QUIC because it allows connect before
    // accept. set longer in higher latency environments (or human action is
    // involved)
    pub interconnect_delay: Duration,
    pub tick_interval: Duration,
}

pub async fn start_replica<M: Decode<()> + Send + Sync + 'static>(
    id: Id,
    config: ReplicaConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<TransportAndSenders> {
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

    let mut transport = Transport::new();
    let mut write_senders = HashMap::new();
    for (id, connection) in connections {
        let (write_sender, write_receiver) = mpsc::channel(100);
        transport.add_connection(connection, message_sender.clone(), write_receiver);
        write_senders.insert(id, write_sender);
    }
    Ok((transport, write_senders))
}

struct ReplicaContext<M> {
    send_buffer: Vec<M>,
    finalize_buffer: Vec<Vec<Command>>,
}

impl<M> crate::replica::ReplicaContext<M> for ReplicaContext<M> {
    fn send(&mut self, message: M) {
        self.send_buffer.push(message)
    }

    fn finalize(&mut self, commands: Vec<Command>) {
        self.finalize_buffer.push(commands)
    }
}

pub async fn run_replica<R: ReplicaProtocol>(
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
    let tick_interval = config.tick_interval;
    let (mut transport, write_senders) = start_replica(id, config, message_sender).await?;
    let mut context = ReplicaContext {
        send_buffer: Default::default(),
        finalize_buffer: Default::default(),
    };
    let mut sleep = pin!(sleep(tick_interval));
    loop {
        enum Select<M> {
            SubmitRecv(Option<Command>),
            MessageRecv(M),
            Sleep,
            #[allow(clippy::enum_variant_names)]
            TransportSelect(anyhow::Result<()>),
        }
        use Select::*;
        match tokio::select! {
            command = submit_receiver.recv() => SubmitRecv(command),
            Some(message) = message_receiver.recv() => MessageRecv(message),
            () = sleep.as_mut() => Sleep,
            Some(result) = transport.select() => TransportSelect(result),
        } {
            SubmitRecv(None) => break Ok(()),
            TransportSelect(Ok(())) => unreachable!("active close of write task"),
            TransportSelect(Err(err)) => anyhow::bail!(err),
            SubmitRecv(Some(command)) => replica.submit(command, &mut context),
            MessageRecv(message) => replica.receive(message, &mut context),
            Sleep => {
                sleep.as_mut().reset(Instant::now() + tick_interval);
                replica.tick(&mut context)
            }
        }
        for message in context.send_buffer.drain(..) {
            Transport::write(message, write_senders.values()).await?
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
