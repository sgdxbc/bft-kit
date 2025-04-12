use std::{
    collections::{BTreeMap, HashMap},
    net::SocketAddr,
};

use bincode::{Decode, error::DecodeError};
use hdrhistogram::Histogram;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep},
    try_join,
};

use crate::{
    common::{
        ClientId, Quorum, ReplicaId,
        transport::{BootServerConfig, ClientConfig, ServiceConfig, WriteMessage},
        workload::{ConcurrentClients, Invoke},
    },
    pbft::{Command, Spec, ToClient},
};

use crate::pbft::{ToReplica, message};

use super::{AbstractEgress, AbstractServer, Finalize, TaskConfig};

async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    mut ingress: impl AsyncRead + Unpin,
    read_sender: Sender<M>,
    remote_close: bool,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    let mut bytes_len = 0;
    loop {
        let len = ingress.read(&mut decode_bytes[bytes_len..]).await?;
        if len == 0 {
            anyhow::ensure!(remote_close);
            break Ok(());
        }
        bytes_len += len;
        let mut offset = 0;
        while offset < bytes_len {
            let range = offset..bytes_len;
            match bincode::decode_from_slice(
                &decode_bytes[range.clone()],
                bincode::config::standard(),
            ) {
                Ok((message, num_bytes)) => {
                    read_sender.send(message).await?;
                    offset += num_bytes // assert offset not > bytes_len?
                }
                // expect to be rare and performance does not matter
                Err(DecodeError::UnexpectedEnd { additional }) => {
                    tracing::warn!(?range, additional, "read partial message");
                    bytes_len = range.len();
                    decode_bytes.copy_within(range, 0);
                    break;
                }
                Err(err) => anyhow::bail!(err),
            }
        }
    }
}

// client-replica egress
impl AbstractEgress for &'_ mut OwnedWriteHalf {
    async fn write_bytes(self, encode_bytes: &[u8]) -> anyhow::Result<()> {
        self.write_all(encode_bytes).await?;
        Ok(())
    }
}

// replica-replica egress
impl AbstractEgress for &'_ mut TcpStream {
    async fn write_bytes(self, encode_bytes: &[u8]) -> anyhow::Result<()> {
        self.write_all(encode_bytes).await?;
        Ok(())
    }
}

async fn boot_client<M: Decode<()> + Send + Sync + 'static>(
    id: ClientId,
    service_config: ServiceConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<(JoinSet<Result<(), anyhow::Error>>, Vec<OwnedWriteHalf>)> {
    let mut replica_egresses = Vec::new();
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    for &addr in &service_config.server_external_addresses {
        let (read_half, write_half) = TcpStream::connect(addr).await?.into_split();
        read_tasks.spawn(read_task(read_half, message_sender.clone(), false));
        replica_egresses.push(write_half);
    }
    for egress in &mut replica_egresses {
        egress.write_all(&id.to_le_bytes()).await?
    }
    Ok((read_tasks, replica_egresses))
}

pub async fn client_task(
    spec: Spec,
    config: ClientConfig,
    service_config: ServiceConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<Invoke>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Histogram<u32>> {
    let (message_sender, mut message_receiver) = mpsc::channel(64);
    let (mut read_tasks, mut replica_egresses) =
        boot_client(id, service_config, message_sender).await?;

    struct SeqScratch {
        results: Quorum<Vec<u8>>,
        expected_result: Option<Vec<u8>>,
        start: Instant,
    }
    let mut seq = 0;
    let mut seq_scratch = BTreeMap::new();
    let mut view_num = 0;
    let mut write_message = WriteMessage::new();
    let mut latencies = Histogram::new(3)?;
    loop {
        enum Select {
            Invoke(Option<Invoke>),
            Message(Option<ToClient>),
            JoinNext(()),
        }
        match tokio::select! {
            invoke = invoke_receiver.recv() => Select::Invoke(invoke),
            message = message_receiver.recv() => Select::Message(message),
            Some(result) = read_tasks.join_next() => Select::JoinNext(result??)
        } {
            Select::JoinNext(()) => unreachable!(),
            Select::Invoke(None) => break Ok(latencies),
            Select::Invoke(Some((op, result))) => {
                seq += 1;
                let command = Command {
                    client_id: id,
                    seq,
                    op,
                };
                write_message
                    .run(
                        ToReplica::Request(command),
                        [&mut replica_egresses[spec.primary(view_num) as usize]],
                    )
                    .await?;
                // resend for close loop?
                if seq_scratch.len() == config.num_max_concurrent {
                    seq_scratch.pop_first();
                }
                seq_scratch.insert(
                    seq,
                    SeqScratch {
                        results: Default::default(),
                        expected_result: result,
                        start: Instant::now(),
                    },
                );
            }
            Select::Message(reply) => {
                let Some(reply) = reply else {
                    anyhow::bail!("message receive channel close")
                };
                let Some(scratch) = seq_scratch.get_mut(&reply.seq) else {
                    continue;
                };
                scratch
                    .results
                    .insert(reply.replica_id, reply.result.clone());
                if scratch
                    .results
                    .values()
                    .filter(|&result| result == &reply.result)
                    .count() as ReplicaId
                    == spec.num_faulty + 1
                {
                    view_num = reply.view_num;
                    let scratch = seq_scratch.remove(&reply.seq).unwrap();
                    if let Some(result) = scratch.expected_result {
                        anyhow::ensure!(reply.result == result)
                    }
                    latencies += scratch.start.elapsed().as_micros() as u64;
                    commit_sender.send(id).await?
                }
            }
        }
    }
}

pub async fn run_close_loop_clients(
    spec: Spec,
    config: TaskConfig,
) -> anyhow::Result<Vec<Histogram<u32>>> {
    let mut concurrent_clients = ConcurrentClients::new();
    for _ in 0..config.num_client {
        concurrent_clients.spawn(|id, invoke_receiver, commit_sender| {
            client_task(
                spec.clone(),
                config.client.clone(),
                config.service.clone(),
                id,
                invoke_receiver,
                commit_sender,
            )
        })
    }
    concurrent_clients.close_loop(config.client_duration).await
}

async fn boot_server(
    replica_id: ReplicaId,
    config: BootServerConfig,
    read_sender: Sender<ToReplica>,
) -> anyhow::Result<(JoinSet<anyhow::Result<()>>, HashMap<u8, TcpStream>)> {
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    let active_task = async {
        tracing::info!(
            "start server interconnect after {:?}",
            config.server_interconnect_delay
        );
        sleep(config.server_interconnect_delay).await;
        let mut connections = HashMap::new();
        for (i, &addr) in config.server_internal_addresses.iter().enumerate() {
            if i as ReplicaId == replica_id {
                continue;
            }
            let mut connection = TcpStream::connect(addr).await?;
            connection.write_all(&replica_id.to_le_bytes()).await?;
            connections.insert(i as ReplicaId, connection);
        }
        anyhow::Ok(connections)
    };
    let internal_listener =
        TcpListener::bind(config.server_internal_addresses[replica_id as usize]).await?;
    let passive_task = async {
        let mut connections = HashMap::new();
        for _ in 0..config.server_internal_addresses.len() - 1 {
            // the peer address here usually can be used for lookup, but in proxied
            // environment like AWS VPC it fails
            let (mut connection, _) = internal_listener.accept().await?;
            let mut replica_id = [0; size_of::<ReplicaId>()];
            connection.read_exact(&mut replica_id).await?;
            connections.insert(ReplicaId::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (egress_connections, ingress_connections) = try_join!(active_task, passive_task)?;
    for connection in ingress_connections.into_values() {
        read_tasks.spawn(read_task(
            connection,
            read_sender.clone(),
            // this probably causes some `read_task` errors, as there has to be a replica
            // that is the one exits earliest and "remote close" other replicas' read tasks
            // will try some neat polling to prevent the error being _revealed_
            false,
        ));
    }
    Ok((read_tasks, egress_connections))
}

async fn service_task(
    replica_id: ReplicaId,
    config: ServiceConfig,
    submit_sender: Sender<ToReplica>,
    mut finalize_receiver: Receiver<Finalize>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_listener =
        TcpListener::bind(config.server_external_addresses[replica_id as usize]).await?;
    let mut read_tasks = JoinSet::new();
    let mut client_egresses = HashMap::new();
    let mut write_message = WriteMessage::new();
    let (client_read_sender, mut client_read_receiver) = mpsc::channel(4096);
    loop {
        enum Select {
            Accept((TcpStream, SocketAddr)),
            Message(Option<ToReplica>),
            Finalize(Option<Finalize>),
            JoinNext(()),
        }
        use Select::{Accept, JoinNext, Message};
        match tokio::select! {
            accept = external_listener.accept() => Accept(accept?),
            message = client_read_receiver.recv() => Message(message),
            finalize = finalize_receiver.recv() => Select::Finalize(finalize),
            Some(result) = read_tasks.join_next() => JoinNext(result??),
        } {
            JoinNext(()) => {}
            Accept((mut connection, _)) => {
                let mut client_id = [0; size_of::<ClientId>()];
                connection.read_exact(&mut client_id).await?;
                let client_id = ClientId::from_le_bytes(client_id);
                tracing::debug!(%client_id, "accept client connection");
                let (read_half, write_half) = connection.into_split();
                read_tasks.spawn(read_task(read_half, client_read_sender.clone(), true));
                let replaced = client_egresses.insert(client_id, write_half);
                anyhow::ensure!(replaced.is_none());
            }
            Message(message) => {
                let Some(ToReplica::Request(command)) = message else {
                    unimplemented!()
                };
                match replies.get(&command.client_id) {
                    Some(reply) if reply.seq > command.seq => {}
                    Some(reply) if reply.seq == command.seq => {
                        let egress = client_egresses.get_mut(&command.client_id);
                        anyhow::ensure!(
                            egress.is_some(),
                            "send to unexpected client id {}",
                            command.client_id
                        );
                        write_message.run(reply.clone(), egress).await?
                    }
                    _ => submit_sender.send(ToReplica::Request(command)).await?,
                }
            }
            Select::Finalize(finalize) => 'finalize: {
                let Some(finalize) = finalize else {
                    tracing::warn!("finalize channel closed");
                    break 'finalize;
                };
                for command in finalize.commands {
                    let reply = message::Reply {
                        seq: command.seq,
                        view_num: finalize.view_num,
                        // a 0/0 service, extend to support arbitrary state machine later
                        result: Default::default(),
                        replica_id,
                    };
                    let replaced = replies.insert(command.client_id, reply.clone());
                    assert!(replaced.map(|reply| reply.seq) < Some(reply.seq));
                    let egress = client_egresses.get_mut(&command.client_id);
                    anyhow::ensure!(
                        egress.is_some(),
                        "send to unexpected client id {}",
                        command.client_id
                    );
                    if let Err(err) = write_message.run(reply, egress).await {
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

pub struct Server;
impl AbstractServer for Server {
    type Egress = TcpStream;

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
        boot_server(replica_id, config.boot_server, read_sender)
    }

    fn service_task(
        replica_id: ReplicaId,
        config: TaskConfig,
        submit_sender: Sender<ToReplica>,
        finalize_receiver: Receiver<Finalize>,
    ) -> impl Future<Output = anyhow::Result<()>> {
        service_task(replica_id, config.service, submit_sender, finalize_receiver)
    }

    fn into_egress(egress: &mut Self::Egress) -> impl AbstractEgress {
        egress
    }
}

// cspell:enableCompoundWords
