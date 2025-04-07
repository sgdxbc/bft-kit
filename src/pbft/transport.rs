use std::{collections::HashMap, net::SocketAddr, pin::pin, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use hdrhistogram::Histogram;
use quinn::{Connection, ConnectionError, Endpoint};
use rand::random;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep, timeout_at},
    try_join,
};

use crate::{
    common::{ClientId, ReplicaId},
    crypto::cert::quinn::{client_config, server_config},
    pbft::ViewNum,
};

use super::{Client, ClientAction, ClientConfig, Replica, ReplicaAction, Spec, ToReplica, message};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub num_client: usize,
    pub client_tick_interval: Duration,
    pub client_duration: Duration,
    pub replica_tick_interval: Duration,
    pub replica_external_addresses: Vec<SocketAddr>,
    pub replica_internal_addresses: Vec<SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. set longer in higher latency environments (or human
    // action is involved)
    pub replica_connect_delay: Duration,
}

pub const WARMUP_DURATION: Duration = Duration::from_secs(1);

// the first transport implemented is with TCP but it doesn't work well (or it
// is just broken), archive it in case of needed
pub mod tcp;

async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    ingress: Connection,
    read_sender: Sender<M>,
    remote_close: bool,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    loop {
        let mut stream = match ingress.accept_uni().await {
            Ok(stream) => stream,
            Err(ConnectionError::ConnectionClosed(_) | ConnectionError::ApplicationClosed(_)) => {
                anyhow::ensure!(remote_close);
                tracing::debug!("remote closed");
                return Ok(());
            }
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

pub trait AbstractEgress {
    fn write_bytes(&mut self, encode_bytes: &[u8]) -> impl Future<Output = anyhow::Result<()>>;
}

async fn write_message<'a, C: AbstractEgress + 'a>(
    message: impl Encode,
    egresses: impl IntoIterator<Item = &'a mut C>,
    encode_bytes: &mut [u8],
) -> anyhow::Result<()> {
    let len = bincode::encode_into_slice(message, encode_bytes, bincode::config::standard())?;
    for egress in egresses {
        egress.write_bytes(&encode_bytes[..len]).await?
    }
    Ok(())
}

pub struct ClientTask {
    client: Client,
    config: TaskConfig,
    replica_ingress: Receiver<message::Reply>,
    read_tasks: JoinSet<anyhow::Result<()>>,
    replica_egresses: Vec<Connection>,
    encode_bytes: Vec<u8>,
}

impl ClientTask {
    pub async fn init(client: Client, config: TaskConfig) -> anyhow::Result<Self> {
        let mut replica_egresses = Vec::new();
        let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
        let (read_sender, read_receiver) = mpsc::channel(64);
        let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
        endpoint.set_default_client_config(client_config());
        for &addr in &config.replica_external_addresses {
            let connection = endpoint.connect(addr, "server.example")?.await?;
            read_tasks.spawn(read_task(connection.clone(), read_sender.clone(), false));
            replica_egresses.push(connection);
        }
        for egress in &mut replica_egresses {
            egress
                .open_uni()
                .await?
                .write_all(&client.config.id.to_le_bytes())
                .await?;
        }

        Ok(Self {
            client,
            config,
            replica_ingress: read_receiver,
            read_tasks,
            replica_egresses,
            encode_bytes: vec![0; 1 << 16],
        })
    }

    pub async fn invoke(&mut self, op: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let mut action = self.client.invoke(op);
        loop {
            tracing::trace!(?action);
            match action {
                ClientAction::Nop => {}
                ClientAction::SendToReplica(replica_id, message) => {
                    write_message(
                        message,
                        [&mut self.replica_egresses[replica_id as usize]],
                        &mut self.encode_bytes,
                    )
                    .await?
                }
                ClientAction::SendToAllReplicas(message) => {
                    write_message(message, &mut self.replica_egresses, &mut self.encode_bytes)
                        .await?
                }
                ClientAction::Return(result) => break Ok(result),
            }
            // tracing::trace!("action performed");

            enum Select {
                Sleep,
                Read(Option<message::Reply>),
                Join(()),
            }
            use Select::*;
            action = match tokio::select! {
                () = sleep(self.config.client_tick_interval) => Sleep,
                reply = self.replica_ingress.recv() => Read(reply),
                Some(result) = self.read_tasks.join_next() => Join(result??),
            } {
                Sleep => self.client.tick(),
                Read(reply) => self
                    .client
                    .receive(reply.ok_or(anyhow::format_err!("unexpect read channel close"))?),
                Join(()) => unreachable!(),
            }
        }
    }
}

pub trait AbstractClientTask: Sized {
    fn init(client: Client, config: TaskConfig) -> impl Future<Output = anyhow::Result<Self>>;
    fn invoke(&mut self, op: Vec<u8>) -> impl Future<Output = anyhow::Result<Vec<u8>>> + Send;
}

impl AbstractClientTask for ClientTask {
    fn init(client: Client, config: TaskConfig) -> impl Future<Output = anyhow::Result<Self>> {
        Self::init(client, config)
    }

    fn invoke(&mut self, op: Vec<u8>) -> impl Future<Output = anyhow::Result<Vec<u8>>> + Send {
        Self::invoke(self, op)
    }
}

pub async fn concurrent_close_loop_clients_task<C: AbstractClientTask + Send + 'static>(
    spec: Spec,
    config: TaskConfig,
) -> anyhow::Result<Vec<Histogram<u32>>> {
    let mut client_tasks = Vec::new();
    for _ in 0..config.num_client {
        let client = Client::new(ClientConfig {
            spec: spec.clone(),
            id: random(),
        });
        client_tasks.push(C::init(client, config.clone()).await?)
    }
    let mut tasks = JoinSet::new();
    for mut client_task in client_tasks {
        let config = config.clone();
        tasks.spawn(async move {
            let now = Instant::now();
            let deadline = now + config.client_duration;
            let start_record = now + WARMUP_DURATION; // TODO configurable?
            let mut latencies = Histogram::new(3)?;
            loop {
                let start = Instant::now();
                let record = start >= start_record;
                match timeout_at(deadline, client_task.invoke(Default::default())).await {
                    Ok(result) => {
                        result?;
                        if record {
                            latencies += start.elapsed().as_micros() as u64
                        }
                    }
                    Err(_) => break anyhow::Ok(latencies),
                }
            }
        });
    }
    let mut latencies = Vec::new();
    while let Some(client_latencies) = tasks.join_next().await {
        latencies.push(client_latencies??)
    }
    Ok(latencies)
}

type BootServer<E> = (JoinSet<anyhow::Result<()>>, HashMap<ReplicaId, E>);

pub trait AbstractServer {
    type Egress;
    fn boot_server(
        replica_id: ReplicaId,
        config: TaskConfig,
        read_sender: Sender<ToReplica>,
    ) -> impl Future<Output = anyhow::Result<BootServer<Self::Egress>>>;

    fn service_task(
        replica_id: ReplicaId,
        config: TaskConfig,
        submit_sender: Sender<ToReplica>,
        finalize_receiver: Receiver<Finalize>,
    ) -> impl Future<Output = anyhow::Result<()>>;
}

pub async fn server_task<S: AbstractServer>(
    mut replica: Replica,
    config: TaskConfig,
) -> anyhow::Result<()>
where
    S::Egress: AbstractEgress,
{
    let (read_sender, mut read_receiver) = mpsc::channel(100);
    let (mut read_tasks, mut replica_egresses) =
        S::boot_server(replica.core.config.id, config.clone(), read_sender.clone()).await?;
    tracing::info!("replica ready");

    // a bit terrible to abuse read_sender, which suppose to directly connect to
    // read tasks in the original design
    let submit_sender = read_sender;
    let (finalize_sender, finalize_receiver) = mpsc::channel(16);
    let replica_id = replica.core.config.id;
    let mut service_task = pin!(S::service_task(
        replica_id,
        config.clone(),
        submit_sender,
        finalize_receiver,
    ));
    let mut encode_bytes = vec![0; 1 << 16];
    let mut actions = Vec::new();
    loop {
        #[derive(Debug)]
        enum Select {
            Sleep,
            Read(Option<ToReplica>),
            Service(()),
            ReadJoin(()),
        }
        use Select::*;
        let select = tokio::select! {
            () = sleep(config.replica_tick_interval) => Sleep,
            message = read_receiver.recv() => Read(message),
            result = &mut service_task => Service(result?),
            Some(result) = read_tasks.join_next() => ReadJoin(result??)
        };
        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(?select)
        }
        match select {
            Sleep => replica.tick(&mut actions),
            Read(message) => {
                let message = message.ok_or(anyhow::format_err!("unexpect read channel close"))?;
                replica.receive(message, &mut actions)
            }
            Service(()) | ReadJoin(()) => unreachable!(),
        }
        for action in actions.drain(..) {
            match action {
                ReplicaAction::SendToReplica(replica_id, message) => {
                    let egress =
                        replica_egresses
                            .get_mut(&replica_id)
                            .ok_or(anyhow::format_err!(
                                "send to unexpected replica id {replica_id}"
                            ))?;
                    write_message(message, [egress], &mut encode_bytes).await?
                }
                ReplicaAction::SendToAllReplicas(message) => {
                    write_message(message, replica_egresses.values_mut(), &mut encode_bytes).await?
                }
                ReplicaAction::Finalize(requests) => {
                    finalize_sender
                        .send(Finalize {
                            requests,
                            view_num: replica.core.view_num,
                        })
                        .await?
                }
            }
        }
    }
}

async fn boot_server(
    replica_id: ReplicaId,
    config: TaskConfig,
    read_sender: Sender<ToReplica>,
) -> anyhow::Result<(JoinSet<anyhow::Result<()>>, HashMap<u8, Connection>)> {
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
        config.replica_internal_addresses[replica_id as usize],
    )?;
    internal_endpoint.set_default_client_config({
        let mut config = client_config();
        config.transport_config(transport);
        config
    });
    let active_task = async {
        sleep(config.replica_connect_delay).await;
        let mut connections = HashMap::new();
        for (i, &addr) in config
            .replica_internal_addresses
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
    anyhow::ensure!(connections.len() == config.replica_internal_addresses.len() - 1);
    let replica_egresses = connections;
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    for connection in replica_egresses.values() {
        read_tasks.spawn(read_task(connection.clone(), read_sender.clone(), false));
    }
    Ok((read_tasks, replica_egresses))
}

pub struct Finalize {
    requests: Vec<message::Request>,
    view_num: ViewNum,
}

async fn service_task(
    replica_id: ReplicaId,
    config: TaskConfig,
    submit_sender: Sender<ToReplica>,
    mut finalize_receiver: Receiver<Finalize>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_endpoint = Endpoint::server(
        server_config(),
        config.replica_external_addresses[replica_id as usize],
    )?;
    let mut read_tasks = JoinSet::new();
    let mut client_egresses = HashMap::new();
    let mut encode_bytes = vec![0; 1 << 16];
    let (client_read_sender, mut client_read_receiver) = mpsc::channel(4096);
    loop {
        let accept = async {
            external_endpoint
                .accept()
                .await
                .expect("endpoint not closed")
                .await
        };
        enum Select {
            Accept(Connection),
            Read(Option<ToReplica>),
            Finalize(Option<Finalize>),
            Join(()),
        }
        use Select::{Accept, Join, Read};
        match tokio::select! {
            accept = accept => Accept(accept?),
            message = client_read_receiver.recv() => Read(message),
            finalize = finalize_receiver.recv() => Select::Finalize(finalize),
            Some(result) = read_tasks.join_next() => Join(result??),
        } {
            Accept(connection) => {
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
                    client_read_sender.clone(),
                    true,
                ));
                let replaced = client_egresses.insert(client_id, connection);
                anyhow::ensure!(replaced.is_none());
            }
            Read(message) => {
                let Some(ToReplica::Request(request)) = message else {
                    unimplemented!()
                };
                match replies.get(&request.client_id) {
                    Some(reply) if reply.seq > request.seq => {}
                    Some(reply) if reply.seq == request.seq => {
                        let egress = client_egresses.get_mut(&request.client_id).ok_or(
                            anyhow::format_err!(
                                "send to unexpected client id {}",
                                request.client_id
                            ),
                        )?;
                        write_message(reply.clone(), [egress], &mut encode_bytes).await?
                    }
                    _ => submit_sender.send(ToReplica::Request(request)).await?,
                }
            }
            Select::Finalize(finalize) => 'finalize: {
                let Some(finalize) = finalize else {
                    tracing::warn!("finalize channel closed");
                    break 'finalize;
                };
                for request in finalize.requests {
                    let reply = message::Reply {
                        seq: request.seq,
                        view_num: finalize.view_num,
                        // a 0/0 service, extend to support arbitrary state machine later
                        result: Default::default(),
                        replica_id,
                    };
                    let replaced = replies.insert(request.client_id, reply.clone());
                    assert!(replaced.map(|reply| reply.seq) < Some(reply.seq));
                    let egress =
                        client_egresses
                            .get_mut(&request.client_id)
                            .ok_or(anyhow::format_err!(
                                "send to unexpected client {}",
                                request.client_id
                            ))?;
                    if let Err(err) = write_message(reply, [egress], &mut encode_bytes).await {
                        // TODO only suppress certain errors e.g. BrokenPipe and ApplicationClose
                        tracing::info!(%err, "egress to client failed")
                        // not removing from egress table to prevent the following
                        // (failed) writing errors
                        // may cause repeatedly logging but the pattern should be rare
                    }
                }
            }
            Join(()) => {}
        }
    }
}

pub struct Server;

impl AbstractServer for Server {
    type Egress = Connection;

    fn boot_server(
        replica_id: ReplicaId,
        config: TaskConfig,
        read_sender: Sender<ToReplica>,
    ) -> impl Future<
        Output = anyhow::Result<(
            JoinSet<anyhow::Result<()>>,
            HashMap<ReplicaId, Self::Egress>,
        )>,
    > {
        boot_server(replica_id, config, read_sender)
    }

    fn service_task(
        replica_id: ReplicaId,
        config: TaskConfig,
        submit_sender: Sender<ToReplica>,
        finalize_receiver: Receiver<Finalize>,
    ) -> impl Future<Output = anyhow::Result<()>> {
        service_task(replica_id, config, submit_sender, finalize_receiver)
    }
}

impl AbstractEgress for Connection {
    async fn write_bytes(&mut self, encode_bytes: &[u8]) -> anyhow::Result<()> {
        self.open_uni().await?.write_all(encode_bytes).await?;
        Ok(())
    }
}
