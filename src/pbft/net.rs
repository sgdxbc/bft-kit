use std::{collections::HashMap, io::ErrorKind, net::SocketAddr, time::Duration};

use bincode::{Decode, Encode, error::DecodeError};
use hdrhistogram::Histogram;
use rand::random;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep, timeout_at},
    try_join,
};

use crate::common::{ClientId, ReplicaId};

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

async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    mut ingress: impl AsyncRead + Unpin,
    read_sender: Sender<M>,
    remote_close: bool,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    let mut offset = 0;
    loop {
        let len = ingress.read(&mut decode_bytes[offset..]).await?;
        if len == 0 {
            anyhow::ensure!(remote_close);
            break Ok(());
        }
        offset += len;
        match bincode::decode_from_slice(&decode_bytes[..offset], bincode::config::standard()) {
            Ok((message, num_bytes)) => {
                read_sender.send(message).await?;
                if num_bytes != offset {
                    decode_bytes.copy_within(num_bytes..offset, 0)
                }
                offset -= num_bytes
            }
            // expect to be rare and performance does not matter
            Err(DecodeError::UnexpectedEnd { additional }) => {
                tracing::warn!(offset, additional, "read partial message")
            }
            Err(err) => anyhow::bail!(err),
        }
    }
}

async fn write_message<W: AsyncWrite + Unpin>(
    message: impl Encode,
    egresses: impl IntoIterator<Item = W>,
    encode_bytes: &mut [u8],
) -> anyhow::Result<()> {
    let len = bincode::encode_into_slice(message, encode_bytes, bincode::config::standard())?;
    for mut egress in egresses {
        egress.write_all(&encode_bytes[..len]).await?
    }
    Ok(())
}

pub struct ClientTask {
    client: Client,
    config: TaskConfig,
    replica_ingress: Receiver<message::Reply>,
    read_tasks: JoinSet<anyhow::Result<()>>,
    replica_egresses: Vec<OwnedWriteHalf>,
    encode_bytes: Vec<u8>,
}

impl ClientTask {
    pub async fn init(client: Client, config: TaskConfig) -> anyhow::Result<Self> {
        let mut replica_egresses = Vec::new();
        let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
        let (read_sender, read_receiver) = mpsc::channel(64);
        for &addr in &config.replica_external_addresses {
            let (read_half, write_half) = TcpStream::connect(addr).await?.into_split();
            read_tasks.spawn(read_task(read_half, read_sender.clone(), false));
            replica_egresses.push(write_half);
        }
        for egress in &mut replica_egresses {
            egress.write_all(&client.config.id.to_le_bytes()).await?
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
    let mut replica_egresses = HashMap::new();
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    let (read_sender, mut read_receiver) = mpsc::channel(4096);
    let active_task = async {
        sleep(config.replica_connect_delay).await;
        for (i, &addr) in config.replica_internal_addresses.iter().enumerate() {
            if i as ReplicaId == replica.config.id {
                continue;
            }
            replica_egresses.insert(i as ReplicaId, TcpStream::connect(addr).await?);
        }
        anyhow::Ok(())
    };
    let internal_listener =
        TcpListener::bind(config.replica_internal_addresses[replica.config.id as usize]).await?;
    let passive_task = async {
        for _ in 0..config.replica_internal_addresses.len() - 1 {
            let (connection, _) = internal_listener.accept().await?;
            // this probably causes some `read_task` errors, as there has to be a replica
            // that is the one exits earliest and "remote close" other replicas' read tasks
            // will try some neat polling to prevent the error being _revealed_
            read_tasks.spawn(read_task(connection, read_sender.clone(), false));
        }
        Ok(())
    };
    try_join!(active_task, passive_task)?;
    tracing::info!("replica ready");

    let replies = HashMap::<ClientId, message::Reply>::new();
    let external_listener =
        TcpListener::bind(config.replica_external_addresses[replica.config.id as usize]).await?;
    let mut client_egresses = HashMap::new();
    let mut encode_bytes = vec![0; 1 << 16];
    loop {
        enum Select {
            Sleep,
            Accept((TcpStream, SocketAddr)),
            Read(Option<ToReplica>),
            Join(()),
        }
        use Select::*;
        let mut option_action = match tokio::select! {
            () = sleep(config.replica_tick_interval) => Sleep,
            accept = external_listener.accept() => Accept(accept?),
            message = read_receiver.recv() => Read(message),
            Some(result) = read_tasks.join_next() => Join(result??),
        } {
            Sleep => Some(replica.tick()),
            Accept((mut connection, _)) => {
                let mut client_id = [0; size_of::<ClientId>()];
                connection.read_exact(&mut client_id).await?;
                let client_id = ClientId::from_le_bytes(client_id);
                let (read_half, write_half) = connection.into_split();
                read_tasks.spawn(read_task(read_half, read_sender.clone(), true));
                let replaced = client_egresses.insert(client_id, write_half);
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
                            let egress = client_egresses.get_mut(&request.client_id).ok_or(
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
        };
        while let Some(action) = option_action.take() {
            match action {
                ReplicaAction::Nop => {}
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
                ReplicaAction::Propose(pre_prepares) => {
                    for pre_prepare in pre_prepares {
                        write_message(
                            ToReplica::PrePrepare(pre_prepare),
                            replica_egresses.values_mut(),
                            &mut encode_bytes,
                        )
                        .await?
                    }
                }
                ReplicaAction::Prepare(vote) => {
                    write_message(
                        ToReplica::Prepare(vote.clone()),
                        replica_egresses.values_mut(),
                        &mut encode_bytes,
                    )
                    .await?;
                    option_action = Some(replica.insert_prepare(vote))
                }
                ReplicaAction::Commit(vote) => {
                    write_message(
                        ToReplica::Commit(vote.clone()),
                        replica_egresses.values_mut(),
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
                            sig: Default::default(), // TODO
                        };
                        let egress = client_egresses.get_mut(&request.client_id).ok_or(
                            anyhow::format_err!("send to unexpected client {}", request.client_id),
                        )?;
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
