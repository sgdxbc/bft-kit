use std::{
    collections::{BTreeMap, HashMap},
    pin::pin,
    time::Duration,
};

use hdrhistogram::Histogram;
use quinn::{Connection, Endpoint};
use rand::random;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep, timeout_at},
};

use crate::{
    common::{
        ClientId, Command, Quorum, ReplicaId,
        transport::{
            BootServerConfig, ClientConfig, Invoke, ServiceConfig, WriteMessage, boot_client,
            boot_server, read_task,
        },
    },
    crypto::cert::quinn::server_config,
};

use super::{Replica, ReplicaAction, Spec, ToClient, ToReplica, message};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub client: ClientConfig,
    pub boot_server: BootServerConfig,
    pub service: ServiceConfig,
    pub num_client: usize,
    pub client_duration: Duration,
    pub tick_interval: Duration,
}

pub const WARMUP_DURATION: Duration = Duration::from_secs(1);

pub async fn client_task(
    spec: Spec,
    config: ClientConfig,
    service_config: ServiceConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<Invoke>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Histogram<u32>> {
    let (message_sender, mut message_receiver) = mpsc::channel(64);
    let (mut read_tasks, replica_egresses) =
        boot_client(id, service_config, message_sender).await?;

    let mut seq = 0;
    let mut write_message = WriteMessage::new();
    let mut latencies = Histogram::new(3)?;
    struct SeqScratch {
        results: Quorum<Vec<u8>>,
        expected_result: Option<Vec<u8>>,
        start: Instant,
    }
    let mut seq_scratch = BTreeMap::new();
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
                    .run(ToReplica::Request(command), &replica_egresses)
                    .await?;
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
            config.client.clone(),
            config.service.clone(),
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

async fn service_task(
    replica_id: ReplicaId,
    config: ServiceConfig,
    submit_sender: Sender<ToReplica>,
    mut finalize_receiver: Receiver<Vec<Command>>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_endpoint = Endpoint::server(
        server_config(),
        config.server_external_addresses[replica_id as usize],
    )?;
    let mut read_tasks = JoinSet::new();
    let mut client_egresses = HashMap::new();
    let mut write_message = WriteMessage::new();
    let (message_sender, mut message_receiver) = mpsc::channel(4096);
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
            Message(Option<ToReplica>),
            Finalize(Option<Vec<Command>>),
            Join(()),
        }
        use Select::{Accept, Join, Message};
        match tokio::select! {
            accept = accept => Accept(accept?),
            message = message_receiver.recv() => Message(message),
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
                read_tasks.spawn(read_task(connection.clone(), message_sender.clone(), true));
                let replaced = client_egresses.insert(client_id, connection);
                anyhow::ensure!(replaced.is_none());
            }
            Message(message) => {
                let Some(ToReplica::Request(command)) = message else {
                    unimplemented!()
                };
                match replies.get(&command.client_id) {
                    Some(reply) if reply.seq > command.seq => {}
                    Some(reply) if reply.seq == command.seq => {
                        let egress = client_egresses.get(&command.client_id);
                        anyhow::ensure!(
                            egress.is_some(),
                            "send to unexpected client {}",
                            command.client_id
                        );
                        write_message.run(reply.clone(), egress).await?
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
                    if matches!(replies.get(&command.client_id), Some(reply) if reply.seq >= command.seq)
                    {
                        tracing::warn!(?command, "duplicated finalize");
                        continue;
                    }
                    let reply = message::Reply {
                        seq: command.seq,
                        // a 0/0 service, extend to support arbitrary state machine later
                        result: Default::default(),
                        replica_id,
                    };
                    replies.insert(command.client_id, reply.clone());
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
        config.boot_server,
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
        config.service,
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
            () = sleep(config.tick_interval) => Sleep,
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
