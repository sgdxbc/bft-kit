use std::{collections::HashMap, io::ErrorKind, net::SocketAddr, sync::Arc, time::Duration};

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

// the first transport was implemented with TCP but it doesn't work well (or
// doesn't even work), archive it in case of needed
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

async fn write_message(
    message: impl Encode,
    egresses: impl IntoIterator<Item = &Connection>,
    encode_bytes: &mut [u8],
) -> anyhow::Result<()> {
    let len = bincode::encode_into_slice(message, encode_bytes, bincode::config::standard())?;
    for egress in egresses {
        egress
            .open_uni()
            .await?
            .write_all(&encode_bytes[..len])
            .await?;
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
            if tracing::enabled!(tracing::Level::TRACE) {
                tracing::trace!(?action);
            }
            match action {
                ClientAction::Nop => {}
                ClientAction::SendToReplica(replica_id, message) => {
                    write_message(
                        message,
                        [&self.replica_egresses[replica_id as usize]],
                        &mut self.encode_bytes,
                    )
                    .await?
                }
                ClientAction::SendToAllReplicas(message) => {
                    write_message(message, &self.replica_egresses, &mut self.encode_bytes).await?
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

pub async fn concurrent_close_loop_clients_task(
    spec: Spec,
    config: TaskConfig,
) -> anyhow::Result<Vec<Histogram<u32>>> {
    let mut client_tasks = Vec::new();
    for _ in 0..config.num_client {
        let client = Client::new(ClientConfig {
            spec: spec.clone(),
            id: random(),
        });
        client_tasks.push(ClientTask::init(client, config.clone()).await?)
    }
    let mut tasks = JoinSet::new();
    for mut client_task in client_tasks {
        let config = config.clone();
        tasks.spawn(async move {
            let deadline = Instant::now() + config.client_duration;
            let mut latencies = Histogram::new(3)?;
            loop {
                let start = Instant::now();
                match timeout_at(deadline, client_task.invoke(Default::default())).await {
                    Ok(result) => {
                        result?;
                        latencies += start.elapsed().as_micros() as u64;
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

pub async fn server_task(mut replica: Replica, config: TaskConfig) -> anyhow::Result<()> {
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
        config.replica_internal_addresses[replica.config.id as usize],
    )?;
    // internal_endpoint.set_default_client_config(client_config());
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
            .skip(replica.config.id as usize + 1)
        {
            let connection = internal_endpoint.connect(addr, "server.example")?.await?;
            connection
                .open_uni()
                .await?
                // to need to `to_le_bytes()` for current u8 based ReplicaId, just future proof
                .write_all(&replica.config.id.to_le_bytes())
                .await?;
            connections.insert(i as ReplicaId, connection);
        }
        anyhow::Ok(connections)
    };
    let passive_task = async {
        let mut connections = HashMap::new();
        for _ in 0..replica.config.id {
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
    let (read_sender, mut read_receiver) = mpsc::channel(4096);
    for connection in replica_egresses.values() {
        read_tasks.spawn(read_task(connection.clone(), read_sender.clone(), false));
    }
    tracing::info!("replica ready");

    let replies = HashMap::<ClientId, message::Reply>::new();
    let external_endpoint = Endpoint::server(
        server_config(),
        config.replica_external_addresses[replica.config.id as usize],
    )?;
    let mut client_egresses = HashMap::new();
    let mut encode_bytes = vec![0; 1 << 16];
    loop {
        #[derive(Debug)]
        enum Select {
            Sleep,
            Accept(Connection),
            Read(Option<ToReplica>),
            Join(()),
        }
        use Select::*;
        let mut option_action = {
            let accept = async {
                external_endpoint
                    .accept()
                    .await
                    .expect("endpoint not closed")
                    .await
            };
            let select = tokio::select! {
                () = sleep(config.replica_tick_interval) => Sleep,
                accept = accept => Accept(accept?),
                message = read_receiver.recv() => Read(message),
                Some(result) = read_tasks.join_next() => Join(result??),
            };
            if tracing::enabled!(tracing::Level::TRACE) {
                tracing::trace!(?select)
            }
            match select {
                Sleep => Some(replica.tick()),
                Accept(connection) => {
                    let mut client_id = [0; size_of::<ClientId>()];
                    connection
                        .accept_uni()
                        .await?
                        .read_exact(&mut client_id)
                        .await?;
                    let client_id = ClientId::from_le_bytes(client_id);
                    tracing::debug!(%client_id, "accept client connection");
                    read_tasks.spawn(read_task(connection.clone(), read_sender.clone(), true));
                    let replaced = client_egresses.insert(client_id, connection);
                    anyhow::ensure!(replaced.is_none());
                    None
                }
                Read(message) => 'read: {
                    let mut message =
                        message.ok_or(anyhow::format_err!("unexpect read channel close"))?;
                    if let ToReplica::Request(request) = message {
                        match replies.get(&request.client_id) {
                            Some(reply) if reply.seq > request.seq => break 'read None,
                            Some(reply) if reply.seq == request.seq => {
                                let egress = client_egresses.get(&request.client_id).ok_or(
                                    anyhow::format_err!(
                                        "send to unexpected client id {}",
                                        request.client_id
                                    ),
                                )?;
                                write_message(reply.clone(), [egress], &mut encode_bytes).await?;
                                break 'read None;
                            }
                            _ => message = ToReplica::Request(request),
                        }
                    }
                    Some(replica.receive(message))
                }
                Join(()) => None,
            }
        };
        while let Some(action) = option_action.take() {
            match action {
                ReplicaAction::Nop => {}
                ReplicaAction::SendToReplica(replica_id, message) => {
                    let egress = replica_egresses
                        .get(&replica_id)
                        .ok_or(anyhow::format_err!(
                            "send to unexpected replica id {replica_id}"
                        ))?;
                    write_message(message, [egress], &mut encode_bytes).await?
                }
                ReplicaAction::SendToAllReplicas(message) => {
                    write_message(message, replica_egresses.values(), &mut encode_bytes).await?
                }
                ReplicaAction::Propose(pre_prepares) => {
                    for pre_prepare in pre_prepares {
                        write_message(
                            ToReplica::PrePrepare(pre_prepare),
                            replica_egresses.values(),
                            &mut encode_bytes,
                        )
                        .await?
                    }
                }
                ReplicaAction::Prepare(vote) => {
                    write_message(
                        ToReplica::Prepare(vote.clone()),
                        replica_egresses.values(),
                        &mut encode_bytes,
                    )
                    .await?;
                    option_action = Some(replica.insert_prepare(vote))
                }
                ReplicaAction::Commit(vote) => {
                    write_message(
                        ToReplica::Commit(vote.clone()),
                        replica_egresses.values(),
                        &mut encode_bytes,
                    )
                    .await?;
                    option_action = Some(replica.insert_commit(vote))
                }
                ReplicaAction::Finalize(requests) => {
                    for request in requests {
                        let reply = message::Reply {
                            seq: request.seq,
                            view_num: replica.view_num,
                            result: Default::default(),
                            replica_id: replica.config.id,
                        };
                        let egress =
                            client_egresses
                                .get(&request.client_id)
                                .ok_or(anyhow::format_err!(
                                    "send to unexpected client {}",
                                    request.client_id
                                ))?;
                        if let Err(err) =
                            write_message(reply.clone(), [egress], &mut encode_bytes).await
                        {
                            if let Some(err) = err.downcast_ref::<std::io::Error>() {
                                if err.kind() == ErrorKind::BrokenPipe {
                                    tracing::info!(%request.client_id, "egress closed")
                                    // not removing from egress table to prevent the following
                                    // (failed) writing errors
                                    // may cause repeatedly logging but the pattern should be rare
                                }
                            }
                        }
                    }
                    option_action = Some(replica.on_finalize())
                }
            }
        }
    }
}
