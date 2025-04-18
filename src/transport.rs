use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint, Incoming};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::sleep,
    try_join,
};

use crate::{
    common::ReplicaAction,
    crypto::cert::quinn::{client_config, server_config},
};

use crate::common::{ClientId, ClientSeq, Command, ReplicaId};

pub async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    ingress: Connection,
    read_sender: Sender<M>,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    loop {
        let mut stream = match ingress.accept_uni().await {
            Ok(stream) => stream,
            Err(err) => anyhow::bail!(err),
        };
        let mut offset = 0;
        while let Some(len) = stream.read(&mut decode_bytes).await? {
            offset += len
        }
        let (message, len) =
            bincode::decode_from_slice(&decode_bytes[..offset], bincode::config::standard())?;
        anyhow::ensure!(len == offset); //
        read_sender.send(message).await?;
    }
}

pub struct WriteMessage {
    encode_bytes: Vec<u8>,
}

pub trait AbstractEgress {
    fn write_bytes(self, encode_bytes: &[u8]) -> impl Future<Output = anyhow::Result<()>> + Send;
}

impl WriteMessage {
    pub fn new() -> Self {
        Self {
            encode_bytes: vec![0; 1 << 16],
        }
    }

    pub async fn run<C: AbstractEgress>(
        &mut self,
        message: impl Encode,
        egresses: impl IntoIterator<Item = C>,
    ) -> anyhow::Result<()> {
        let len = bincode::encode_into_slice(
            message,
            &mut self.encode_bytes,
            bincode::config::standard(),
        )?;
        for egress in egresses {
            egress.write_bytes(&self.encode_bytes[..len]).await?
        }
        Ok(())
    }
}

impl Default for WriteMessage {
    fn default() -> Self {
        Self::new()
    }
}

impl AbstractEgress for &'_ Connection {
    async fn write_bytes(self, encode_bytes: &[u8]) -> anyhow::Result<()> {
        self.open_uni().await?.write_all(encode_bytes).await?;
        Ok(())
    }
}

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
        read_tasks.spawn(read_task(connection.clone(), message_sender.clone()));
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
) -> anyhow::Result<BootReplica> {
    let mut transport = quinn::TransportConfig::default();
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
        let mut connections = HashMap::new();
        for _ in 0..replica_id {
            let connection = internal_endpoint
                .accept()
                .await
                .expect("endpoint not closed")
                .await?;
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
        read_tasks.spawn(read_task(connection.clone(), message_sender.clone()));
    }
    Ok((read_tasks, replica_egresses))
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

// this abstraction feels weird (maybe it's over engineered)
pub struct Service<P>
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

impl<K> Service<K>
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
    ) -> anyhow::Result<()>
    where
        <Self as AbstractService>::Reply: Encode,
    {
        let external_endpoint = Endpoint::server(
            server_config(),
            config.server_external_addresses[self.replica_id as usize],
        )?;
        let mut read_tasks = JoinSet::new();
        let mut client_egresses = HashMap::new();
        let mut write_message = WriteMessage::new();
        let (message_sender, mut message_receiver) = mpsc::channel(1 << 12);
        loop {
            enum Select<F> {
                Accept(Option<Incoming>),
                Message(Option<Command>),
                Finalized(Option<F>),
                JoinNext(anyhow::Result<()>),
            }
            use Select::*;
            match tokio::select! {
                accept = external_endpoint.accept() => Accept(accept),
                message = message_receiver.recv() => Message(message),
                finalize = finalized_receiver.recv() => Finalized(finalize),
                Some(result) = read_tasks.join_next() => JoinNext(result?),
            } {
                JoinNext(Ok(())) => unreachable!(),
                JoinNext(Err(err)) => 'join_next: {
                    if let Some(ConnectionError::ApplicationClosed(_)) = err.downcast_ref() {
                        break 'join_next;
                    }
                    // TODO suppress certain errors
                    // tracing::info!(%err, "client read task failed")
                    anyhow::bail!(err)
                }
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
                    read_tasks.spawn(read_task(connection.clone(), message_sender.clone()));
                    let replaced = client_egresses.insert(client_id, connection);
                    anyhow::ensure!(replaced.is_none());
                }
                Message(None) => unreachable!(),
                Message(Some(command)) => {
                    match self.replies.get(&command.client_id) {
                        Some(reply) if Self::reply_seq(reply) > command.seq => {}
                        Some(reply) if Self::reply_seq(reply) == command.seq => {
                            let egress = client_egresses.get(&command.client_id);
                            anyhow::ensure!(
                                egress.is_some(),
                                "send to unexpected client {}",
                                command.client_id
                            );
                            if let Err(err) = write_message.run(reply, egress).await {
                                // TODO only suppress certain errors e.g. ApplicationClose
                                tracing::info!(%err, "egress to client failed")
                                // not removing from egress table to prevent the following
                                // (failed) writing errors
                                // may cause repeatedly logging but the pattern should be rare
                            }
                        }
                        _ => self.request_sender.send(command).await?,
                    }
                }
                Finalized(None) => {
                    tracing::warn!("finalized channel closed");
                    break Ok(());
                }
                Finalized(Some(finalized)) => {
                    for (client_id, reply) in self.on_finalized(finalized) {
                        let egress = client_egresses.get(&client_id);
                        anyhow::ensure!(egress.is_some(), "send to unexpected client {client_id}");
                        if let Err(err) = write_message.run(reply, egress).await {
                            // TODO only suppress certain errors e.g. ApplicationClose
                            tracing::info!(%err, "egress to client failed")
                            // not removing from egress table to prevent the following
                            // (failed) writing errors
                            // may cause repeatedly logging but the pattern should be rare
                        }
                    }
                }
            }
        }
    }
}

pub trait AbstractReplica: crate::common::AbstractReplica {
    type Finalized;

    fn finalized(&self, commands: Vec<Command>) -> Self::Finalized;
}

pub async fn replica_task<
    R: AbstractReplica<Action = ReplicaAction<M>, Message = M>,
    M: Encode + Decode<()> + Send + Sync + 'static,
>(
    replica_id: ReplicaId,
    mut replica: R,
    config: ReplicaConfig,
    tick_interval: Duration,
    mut request_receiver: Receiver<Command>,
    finalized_sender: Sender<R::Finalized>,
) -> anyhow::Result<()>
where
    R::Finalized: Send + Sync + 'static,
{
    let (message_sender, mut message_receiver) = mpsc::channel(100);
    let (mut read_tasks, replica_egresses) =
        boot_replica(replica_id, config, message_sender.clone()).await?;
    tracing::info!("replica ready");

    let mut write_message = WriteMessage::new();
    let mut actions = Vec::new();

    replica.init(&mut actions);
    loop {
        for action in actions.drain(..) {
            match action {
                ReplicaAction::SendToReplica(replica_id, message) => {
                    let egress = replica_egresses.get(&replica_id);
                    anyhow::ensure!(
                        egress.is_some(),
                        "send to unexpected replica id {replica_id}"
                    );
                    write_message.run(message, egress).await?
                }
                ReplicaAction::SendToAllReplicas(message) => {
                    write_message
                        .run(message, replica_egresses.values())
                        .await?
                }
                ReplicaAction::Finalize(commands) => {
                    finalized_sender.send(replica.finalized(commands)).await?
                }
            }
        }

        enum Select<M> {
            Sleep,
            Request(Option<Command>),
            Message(Option<M>),
            JoinNext(()),
        }
        use Select::*;
        match tokio::select! {
            () = sleep(tick_interval) => Sleep,
            request = request_receiver.recv() => Request(request),
            message = message_receiver.recv() => Message(message),
            Some(result) = read_tasks.join_next() => JoinNext(result??)
        } {
            Sleep => replica.tick(&mut actions),
            Request(command) => {
                let Some(command) = command else {
                    break Ok(()); // think about whether this is correct
                };
                replica.request(command, &mut actions)
            }
            Message(message) => {
                let Some(message) = message else {
                    anyhow::bail!("message receive channel close")
                };
                replica.receive(message, &mut actions)
            }
            JoinNext(()) => unreachable!(),
        }
    }
}
