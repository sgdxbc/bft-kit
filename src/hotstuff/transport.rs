use std::{collections::HashMap, net::SocketAddr, pin::pin, sync::Arc, time::Duration};

use hdrhistogram::Histogram;
use quinn::{Connection, Endpoint};
use rand::random;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep, timeout_at},
    try_join,
};

use crate::{
    common::{
        ClientId, Command, Quorum, ReplicaId,
        transport::{WriteMessage, read_task},
    },
    crypto::cert::quinn::{client_config, server_config},
};

use super::{Replica, ReplicaAction, Spec, ToClient, ToReplica, message};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub num_client: usize,
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

pub async fn client_task(
    spec: Spec,
    config: TaskConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<(Vec<u8>, Option<Vec<u8>>)>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Histogram<u32>> {
    let mut replica_egresses = Vec::new();
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    let (message_sender, mut message_receiver) = mpsc::channel(64);
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());
    for &addr in &config.replica_external_addresses {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        read_tasks.spawn(read_task(connection.clone(), message_sender.clone(), false));
        replica_egresses.push(connection);
    }
    for egress in &mut replica_egresses {
        egress
            .open_uni()
            .await?
            .write_all(&id.to_le_bytes())
            .await?;
    }
    let mut write_message = WriteMessage::new();

    let mut seq = 0;
    let mut latencies = Histogram::new(3)?;
    struct SeqScratch {
        results: Quorum<Vec<u8>>,
        expected_result: Option<Vec<u8>>,
        start: Instant,
    }
    let mut seq_scratch = HashMap::new();
    loop {
        enum Select {
            Invoke(Option<(Vec<u8>, Option<Vec<u8>>)>),
            Message(Option<ToClient>),
            JoinNext(()),
        }
        use Select::*;
        match tokio::select! {
            invoke = invoke_receiver.recv() => Invoke(invoke),
            message = message_receiver.recv() => Message(message),
            Some(result) = read_tasks.join_next() => JoinNext(result??)
        } {
            Invoke(None) => break Ok(latencies),
            Invoke(Some((op, result))) => {
                seq += 1;
                let command = Command {
                    client_id: id,
                    seq,
                    op,
                };
                write_message
                    .run(ToReplica::Request(command), &replica_egresses)
                    .await?;
                seq_scratch.insert(
                    seq,
                    SeqScratch {
                        results: Default::default(),
                        expected_result: result,
                        start: Instant::now(),
                    },
                );
            }
            Message(reply) => {
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
                    let scratch = seq_scratch.remove(&reply.seq).unwrap();
                    if let Some(result) = scratch.expected_result {
                        anyhow::ensure!(reply.result == result)
                    }
                    latencies += scratch.start.elapsed().as_micros() as u64;
                    commit_sender.send(id).await?
                }
            }
            JoinNext(()) => unreachable!(),
        }
    }
}

pub async fn run_close_loop_clients(
    spec: Spec,
    config: TaskConfig,
) -> anyhow::Result<Vec<Histogram<u32>>> {
    let mut client_tasks = JoinSet::new();
    let mut invoke_senders = HashMap::new();
    let (commit_sender, mut commit_receiver) = mpsc::channel(64);
    for _ in 0..config.num_client {
        let (invoke_sender, invoke_receiver) = mpsc::channel(64);
        let id = ClientId(random());
        let replaced = invoke_senders.insert(id, invoke_sender);
        anyhow::ensure!(replaced.is_none());
        client_tasks.spawn(client_task(
            spec.clone(),
            config.clone(),
            id,
            invoke_receiver,
            commit_sender.clone(),
        ));
    }
    for sender in invoke_senders.values() {
        sender
            .send((Default::default(), Some(Default::default())))
            .await?
    }

    let deadline = Instant::now() + config.client_duration;
    while let Ok(client_id) = timeout_at(deadline, commit_receiver.recv()).await {
        let Some(client_id) = client_id else {
            anyhow::bail!("commit receive channel closed")
        };
        invoke_senders[&client_id]
            .send((Default::default(), Some(Default::default())))
            .await?
    }

    drop(invoke_senders);
    let mut latencies = Vec::new();
    while let Some(client_latencies) = client_tasks.join_next().await {
        latencies.push(client_latencies??)
    }
    Ok(latencies)
}

async fn boot_server(
    replica_id: ReplicaId,
    config: TaskConfig,
    message_sender: Sender<ToReplica>,
) -> anyhow::Result<(JoinSet<anyhow::Result<()>>, HashMap<ReplicaId, Connection>)> {
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
        read_tasks.spawn(read_task(connection.clone(), message_sender.clone(), false));
    }
    Ok((read_tasks, replica_egresses))
}

async fn service_task(
    replica_id: ReplicaId,
    config: TaskConfig,
    submit_sender: Sender<ToReplica>,
    mut finalize_receiver: Receiver<Vec<Command>>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_endpoint = Endpoint::server(
        server_config(),
        config.replica_external_addresses[replica_id as usize],
    )?;
    let mut read_tasks = JoinSet::new();
    let mut client_egresses = HashMap::new();
    let mut write_message = WriteMessage::new();
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
            Finalize(Option<Vec<Command>>),
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
                let Some(ToReplica::Request(command)) = message else {
                    unimplemented!()
                };
                match replies.get(&command.client_id) {
                    Some(reply) if reply.seq > command.seq => {}
                    Some(reply) if reply.seq == command.seq => {
                        let egress =
                            client_egresses
                                .get(&command.client_id)
                                .ok_or(anyhow::format_err!(
                                    "send to unexpected client id {}",
                                    command.client_id
                                ))?;
                        write_message.run(reply.clone(), [egress]).await?
                    }
                    _ => submit_sender.send(ToReplica::Request(command)).await?,
                }
            }
            Select::Finalize(commands) => 'finalize: {
                let Some(commands) = commands else {
                    tracing::warn!("finalize channel closed");
                    break 'finalize;
                };
                for command in commands {
                    let reply = message::Reply {
                        seq: command.seq,
                        // a 0/0 service, extend to support arbitrary state machine later
                        result: Default::default(),
                        replica_id,
                    };
                    let replaced = replies.insert(command.client_id, reply.clone());
                    assert!(replaced.map(|reply| reply.seq) < Some(reply.seq));
                    let egress = client_egresses.get(&command.client_id);
                    anyhow::ensure!(
                        egress.is_some(),
                        "send to unexpected client {}",
                        command.client_id
                    );
                    if let Err(err) = write_message.run(reply, egress).await {
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

pub async fn server_task(mut replica: Replica, config: TaskConfig) -> anyhow::Result<()> {
    let (message_sender, mut message_receiver) = mpsc::channel(100);
    let (mut read_tasks, replica_egresses) = boot_server(
        replica.core.config.id,
        config.clone(),
        message_sender.clone(),
    )
    .await?;
    tracing::info!("replica ready");

    // a bit terrible to abuse read_sender, which suppose to directly connect to
    // read tasks in the original design
    let submit_sender = message_sender;
    let (finalize_sender, finalize_receiver) = mpsc::channel(16);
    let replica_id = replica.core.config.id;
    let mut service_task = pin!(service_task(
        replica_id,
        config.clone(),
        submit_sender,
        finalize_receiver,
    ));
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
                ReplicaAction::Finalize(commands) => finalize_sender.send(commands).await?,
            }
        }

        enum Select {
            Sleep,
            Message(Option<ToReplica>),
            Service(()),
            ReadJoin(()),
        }
        use Select::*;
        match tokio::select! {
            () = sleep(config.replica_tick_interval) => Sleep,
            message = message_receiver.recv() => Message(message),
            result = &mut service_task => Service(result?),
            Some(result) = read_tasks.join_next() => ReadJoin(result??)
        } {
            // Sleep => replica.tick(&mut actions),
            Sleep => {} // TODO impl tick on replica
            Message(message) => {
                let Some(message) = message else {
                    anyhow::bail!("message receive channel close")
                };
                replica.receive(message, &mut actions)
            }
            Service(()) | ReadJoin(()) => unreachable!(),
        }
    }
}
