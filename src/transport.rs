use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use bytes::Bytes;
use quinn::{Connection, ConnectionError, Endpoint, Incoming, TransportConfig};
use tokio::{
    sync::mpsc::{self, Receiver, Sender, error::TrySendError},
    task::JoinSet,
    time::sleep,
    try_join,
};
use tokio_util::sync::CancellationToken;

use crate::{
    common::ReplicaAction,
    crypto::cert::quinn::{client_config, server_config},
};

use crate::common::{ClientId, ClientSeq, Command, ReplicaId};

pub async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    ingress: Connection,
    read_sender: Sender<M>,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    loop {
        let mut stream = match cancel.run_until_cancelled(ingress.accept_uni()).await {
            None => break Ok(()),
            Some(Ok(stream)) => stream,
            Some(Err(err)) => anyhow::bail!(err),
        };
        let mut offset = 0;
        while let Some(len) = stream.read(&mut decode_bytes).await? {
            offset += len
        }
        let (message, len) =
            bincode::decode_from_slice(&decode_bytes[..offset], bincode::config::standard())?;
        anyhow::ensure!(len == offset); //
        // read_sender.send(message).await?;
        if !checked_send(&read_sender, message).await? {
            tracing::warn!("request channel full");
        }
    }
}

pub struct WriteMessage {
    encode_bytes: Bytes,
}

impl WriteMessage {
    pub fn new(message: impl Encode) -> anyhow::Result<Self> {
        Ok(Self {
            encode_bytes: bincode::encode_to_vec(message, bincode::config::standard())?.into(),
        })
    }

    pub fn run(
        &self,
        egress: &Connection,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>> + use<>> {
        let bytes = self.encode_bytes.clone();
        let egress = egress.clone();
        Ok(async move {
            let mut stream = egress.open_uni().await?;
            stream.write_all(&bytes).await?;
            Ok(())
        })
    }

    pub fn reply_client(
        self,
        egress: &Connection,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>> + use<>> {
        let egress = egress.clone();
        Ok(async move {
            let stream = match egress.open_uni().await {
                Ok(stream) => Some(stream),
                Err(ConnectionError::ApplicationClosed(_)) => None,
                Err(err) => anyhow::bail!(err),
            };
            if let Some(mut stream) = stream {
                stream.write_all(&self.encode_bytes).await?
            }
            Ok(())
        })
    }
}

#[derive(Debug, Clone)]
pub enum ClientConfig {
    CloseLoop,
    OpenLoop(OpenLoopClientConfig),
}

#[derive(Debug, Clone)]
pub struct OpenLoopClientConfig {
    pub num_max_concurrent: usize,
    pub sending_rate: f32,
}

// TODO extract client skeleton
// not sure whether that is possible or not since (simple) clients are inline
// implemented in transport
// nonetheless, bootstrapping part is extracted below

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub server_external_addresses: Vec<SocketAddr>,
}

type BootClient = (JoinSet<Result<(), anyhow::Error>>, Vec<Connection>);

pub async fn boot_client<M: Decode<()> + Send + Sync + 'static>(
    id: ClientId,
    service_config: ServiceConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<BootClient> {
    let mut replica_egresses = Vec::new();
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());
    for &addr in &service_config.server_external_addresses {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        read_tasks.spawn(read_task(
            connection.clone(),
            message_sender.clone(),
            CancellationToken::new(), // TODO properly cancel?
        ));
        replica_egresses.push(connection);
    }
    for egress in &mut replica_egresses {
        egress
            .open_uni()
            .await?
            .write_all(&id.to_le_bytes())
            .await?;
    }
    Ok((read_tasks, replica_egresses))
}

// this abstraction feels weird (maybe it's over engineered)
pub struct ServiceTask<P>
where
    Self: AbstractService,
{
    pub replies: HashMap<ClientId, <Self as AbstractService>::Reply>,
    pub request_sender: Sender<Command>,
    // TODO service state machine
    pub replica_id: ReplicaId,
}

pub trait AbstractService {
    type Reply;
    type Finalized;

    fn reply_seq(reply: &Self::Reply) -> ClientSeq;

    fn on_finalized(
        &mut self,
        finalized: Self::Finalized,
    ) -> impl Iterator<Item = (ClientId, Self::Reply)>;
}

impl<K> ServiceTask<K>
where
    Self: AbstractService,
{
    pub fn new(replica_id: ReplicaId, request_sender: Sender<Command>) -> Self {
        Self {
            replica_id,
            request_sender,
            replies: Default::default(),
        }
    }

    pub async fn run(
        mut self,
        config: ServiceConfig,
        mut finalized_receiver: Receiver<<Self as AbstractService>::Finalized>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>
    where
        <Self as AbstractService>::Reply: Encode,
    {
        let mut transport = TransportConfig::default();
        transport.max_idle_timeout(None);
        transport.max_concurrent_uni_streams((1u32 << 12).into());
        let external_endpoint = Endpoint::server(
            // server_config(),
            {
                let mut config = server_config();
                config.transport_config(transport.into());
                config
            },
            config.server_external_addresses[self.replica_id as usize],
        )?;
        let mut read_tasks = JoinSet::new();
        let mut write_tasks = JoinSet::new();
        let mut client_egresses = HashMap::new();
        let (message_sender, mut message_receiver) = mpsc::channel(1 << 12);
        loop {
            enum Select<F> {
                Accept(Option<Incoming>),
                Message(Option<Command>),
                Finalized(Option<F>),
                ReadJoinNext(anyhow::Result<()>),
                WriteJoinNext(()),
                Cancel,
            }
            use Select::*;
            match tokio::select! {
                accept = external_endpoint.accept() => Accept(accept),
                message = message_receiver.recv() => Message(message),
                finalize = finalized_receiver.recv() => Finalized(finalize),
                Some(result) = read_tasks.join_next() => ReadJoinNext(result?),
                Some(result) = write_tasks.join_next() => WriteJoinNext(result??),
                () = cancel.cancelled() => Cancel,
            } {
                Cancel | ReadJoinNext(Ok(())) | Finalized(None) => {
                    anyhow::ensure!(cancel.is_cancelled());
                    break;
                }
                ReadJoinNext(Err(err)) => 'join_err: {
                    if let Some(ConnectionError::ApplicationClosed(_)) = err.downcast_ref() {
                        break 'join_err;
                    }
                    anyhow::bail!(err)
                }
                WriteJoinNext(()) => {}
                Accept(None) => anyhow::bail!("endpoint closed"),
                Accept(Some(incoming)) => {
                    let connection = incoming.await?;
                    let mut client_id = [0; size_of::<ClientId>()];
                    connection
                        .accept_uni()
                        .await?
                        .read_exact(&mut client_id)
                        .await?;
                    let client_id = ClientId::from_le_bytes(client_id);
                    tracing::debug!(%client_id, "accept client connection");
                    read_tasks.spawn(read_task(
                        connection.clone(),
                        message_sender.clone(),
                        cancel.clone(),
                    ));
                    let replaced = client_egresses.insert(client_id, connection);
                    anyhow::ensure!(replaced.is_none());
                }
                Message(None) => unreachable!(),
                Message(Some(command)) => match self.replies.get(&command.client_id) {
                    Some(reply) if Self::reply_seq(reply) > command.seq => {}
                    Some(reply) if Self::reply_seq(reply) == command.seq => {
                        let Some(egress) = client_egresses.get(&command.client_id) else {
                            anyhow::bail!("send to unexpected client id {}", command.client_id)
                        };
                        write_tasks.spawn(WriteMessage::new(reply)?.reply_client(egress)?);
                    }
                    // _ => self.request_sender.send(command).await?,
                    _ => {
                        if !checked_send(&self.request_sender, command).await? {
                            tracing::warn!("request channel full");
                        }
                    }
                },
                Finalized(Some(finalized)) => {
                    for (client_id, reply) in self.on_finalized(finalized) {
                        let Some(egress) = client_egresses.get(&client_id) else {
                            anyhow::bail!("send to unexpected client id {client_id}")
                        };
                        write_tasks.spawn(WriteMessage::new(reply)?.reply_client(egress)?);
                    }
                }
            }
        }
        while let Some(result) = read_tasks.join_next().await {
            result??
        }
        let path_stats = client_egresses
            .values()
            .map(|connection| connection.stats().path)
            .collect::<Vec<_>>();
        tracing::info!(lost_sum = path_stats.iter().map(|stats| stats.lost_packets).sum::<u64>(), lost_max = ?path_stats.iter().map(|stats| stats.lost_packets).max(), "service");
        tracing::info!(sent_sum = path_stats.iter().map(|stats| stats.sent_packets).sum::<u64>(), sent_max = ?path_stats.iter().map(|stats| stats.sent_packets).max(), "service");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    pub server_internal_addresses: Vec<SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. set longer in higher latency environments (or human
    // action is involved)
    pub server_interconnect_delay: Duration,
}

type BootReplica = (JoinSet<anyhow::Result<()>>, HashMap<ReplicaId, Connection>);

pub async fn boot_replica<M: Decode<()> + Send + Sync + 'static>(
    replica_id: ReplicaId,
    config: ReplicaConfig,
    message_sender: Sender<M>,
    cancel: CancellationToken,
) -> anyhow::Result<BootReplica> {
    let mut transport = TransportConfig::default();
    transport.max_idle_timeout(None);
    let transport = Arc::new(transport);
    let mut internal_endpoint = Endpoint::server(
        // server_config(),
        {
            let mut config = server_config();
            config.transport_config(transport.clone());
            config
        },
        config.server_internal_addresses[replica_id as usize],
    )?;
    internal_endpoint.set_default_client_config({
        let mut config = client_config();
        config.transport_config(transport);
        config
    });
    let active_task = async {
        tracing::info!(
            "start server interconnect after {:?}",
            config.server_interconnect_delay
        );
        sleep(config.server_interconnect_delay).await;
        let mut connections = HashMap::new();
        for (i, &addr) in config
            .server_internal_addresses
            .iter()
            .enumerate()
            .skip(replica_id as usize + 1)
        {
            let connection = internal_endpoint.connect(addr, "server.example")?.await?;
            tracing::debug!(remote = ?connection.remote_address(), "connect");
            connection
                .open_uni()
                .await?
                // to need to `to_le_bytes()` for current u8 based ReplicaId, just future proof
                .write_all(&replica_id.to_le_bytes())
                .await?;
            connections.insert(i as ReplicaId, connection);
        }
        anyhow::Ok(connections)
    };
    let passive_task = async {
        tracing::info!(addr = ?internal_endpoint.local_addr(), "start listening");
        let mut connections = HashMap::new();
        for _ in 0..replica_id {
            let connection = internal_endpoint
                .accept()
                .await
                .expect("endpoint not closed")
                .await?;
            tracing::debug!(remote = ?connection.remote_address(), "accept");
            let mut replica_id = [0; size_of::<ReplicaId>()];
            connection
                .accept_uni()
                .await?
                .read_exact(&mut replica_id)
                .await?;
            connections.insert(ReplicaId::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (mut connections, other_connections) = try_join!(active_task, passive_task)?;
    connections.extend(other_connections);
    anyhow::ensure!(connections.len() == config.server_internal_addresses.len() - 1);
    let replica_egresses = connections;
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    for connection in replica_egresses.values() {
        read_tasks.spawn(read_task(
            connection.clone(),
            message_sender.clone(),
            cancel.clone(),
        ));
    }
    Ok((read_tasks, replica_egresses))
}

pub trait AbstractReplica: crate::common::AbstractReplica {
    type Finalized;

    fn finalized(&self, commands: Vec<Command>) -> Self::Finalized;
}

pub struct ReplicaTask<R: AbstractReplica> {
    pub replica_egresses: HashMap<ReplicaId, Connection>,
    pub finalized_sender: Sender<R::Finalized>,
    pub replica: R,
    pub write_tasks: JoinSet<anyhow::Result<()>>,
}

pub trait Effect<R>
where
    R: AbstractReplica,
{
    fn effect(self, task: &mut ReplicaTask<R>) -> impl Future<Output = anyhow::Result<()>>;
}

impl<R: AbstractReplica> ReplicaTask<R> {
    pub fn new(replica: R, finalized_sender: Sender<R::Finalized>) -> Self {
        Self {
            replica_egresses: Default::default(),
            finalized_sender,
            replica,
            write_tasks: JoinSet::new(),
        }
    }

    pub async fn run(
        mut self,
        replica_id: ReplicaId,
        config: ReplicaConfig,
        tick_interval: Duration,
        mut request_receiver: Receiver<Command>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()>
    where
        R::Action: Effect<R>,
        R::Message: Decode<()> + Send + Sync + 'static,
    {
        let (message_sender, mut message_receiver) = mpsc::channel(100);
        let mut read_tasks;
        (read_tasks, self.replica_egresses) =
            boot_replica(replica_id, config, message_sender, cancel.clone()).await?;
        tracing::info!("replica ready");

        let mut actions = Vec::new();

        self.replica.init(&mut actions);
        loop {
            for action in actions.drain(..) {
                action.effect(&mut self).await?
            }

            enum Select<M> {
                Sleep,
                Request(Option<Command>),
                Message(Option<M>),
                ReadJoinNext(()),
                WriteJoinNext(()),
                Cancel,
            }
            use Select::*;
            match tokio::select! {
                () = sleep(tick_interval) => Sleep,
                request = request_receiver.recv() => Request(request),
                message = message_receiver.recv() => Message(message),
                Some(result) = read_tasks.join_next() => ReadJoinNext(result??),
                Some(result) = self.write_tasks.join_next() => WriteJoinNext(result??),
                () = cancel.cancelled() => Cancel,
            } {
                Cancel | Message(None) | Request(None) | ReadJoinNext(()) => {
                    anyhow::ensure!(cancel.is_cancelled());
                    break;
                }
                WriteJoinNext(()) => {}
                Sleep => self.replica.tick(&mut actions),
                Request(Some(command)) => self.replica.request(command, &mut actions),
                Message(Some(message)) => self.replica.receive(message, &mut actions),
            }
        }
        while let Some(result) = read_tasks.join_next().await {
            result??
        }
        let path_stats = self
            .replica_egresses
            .values()
            .map(|connection| connection.stats().path)
            .collect::<Vec<_>>();
        tracing::info!(lost_sum = path_stats.iter().map(|stats| stats.lost_packets).sum::<u64>(), lost_max = ?path_stats.iter().map(|stats| stats.lost_packets).max(), "replica");
        tracing::info!(sent_sum = path_stats.iter().map(|stats| stats.sent_packets).sum::<u64>(), sent_max = ?path_stats.iter().map(|stats| stats.sent_packets).max(), "replica");
        // delay releasing self.replica_egresses until remote replicas are canceled and
        // actively close the connection from read_task side
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

impl<R: AbstractReplica<Action = Self>, M: Encode> Effect<R> for ReplicaAction<M>
where
    R::Finalized: Send + Sync + 'static,
{
    async fn effect(self, task: &mut ReplicaTask<R>) -> anyhow::Result<()> {
        match self {
            ReplicaAction::SendToReplica(replica_id, message) => {
                let Some(egress) = task.replica_egresses.get(&replica_id) else {
                    anyhow::bail!("send to unexpected replica id {replica_id}");
                };
                task.write_tasks
                    .spawn(WriteMessage::new(message)?.run(egress)?);
            }
            ReplicaAction::SendToAllReplicas(message) => {
                let write_message = WriteMessage::new(message)?;
                for egress in task.replica_egresses.values() {
                    task.write_tasks.spawn(write_message.run(egress)?);
                }
            }
            ReplicaAction::Finalize(commands) => {
                // task.finalized_sender
                //     .send(task.replica.finalized(commands))
                //     .await?
                if !checked_send(&task.finalized_sender, task.replica.finalized(commands)).await? {
                    // tracing::warn!("finalized channel full");
                }
            }
        }
        Ok(())
    }
}

pub async fn checked_send<T: Send + Sync + 'static>(
    sender: &Sender<T>,
    value: T,
) -> anyhow::Result<bool> {
    let Err(err) = sender.try_send(value) else {
        return Ok(true);
    };
    let TrySendError::Full(value) = err else {
        anyhow::bail!(err)
    };
    sender.send(value).await?;
    Ok(false)
}
