use std::{
    collections::{BTreeMap, HashMap},
    pin::pin,
    time::Duration,
};

use hdrhistogram::Histogram;
use quinn::{Connection, Endpoint};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, sleep},
};

use crate::{
    common::{
        ClientId, Quorum, ReplicaId,
        transport::{
            AbstractEgress, BootServerConfig, ClientConfig, ServiceConfig, WriteMessage,
            boot_client, boot_server, read_task,
        },
        workload::{ConcurrentClients, Invoke, Latencies},
    },
    crypto::cert::quinn::server_config,
    pbft::{Command, ToClient, ViewNum},
};

use super::{Replica, ReplicaAction, Spec, ToReplica, message};

// the first transport implemented is with TCP but it doesn't work well (or it
// is just broken), archive it in case of needed
pub mod tcp;

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
                        [&replica_egresses[spec.primary(view_num) as usize]],
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
) -> anyhow::Result<Vec<Latencies>> {
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

type BootServer<E> = (JoinSet<anyhow::Result<()>>, HashMap<ReplicaId, E>);

pub trait AbstractServer {
    type Egress;

    fn into_egress(egress: &mut Self::Egress) -> impl AbstractEgress;

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
) -> anyhow::Result<()> {
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
    let mut write_message = WriteMessage::new();
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
            () = sleep(config.tick_interval) => Sleep,
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
                    let egress = replica_egresses.get_mut(&replica_id);
                    anyhow::ensure!(
                        egress.is_some(),
                        "send to unexpected replica id {replica_id}"
                    );
                    write_message
                        .run(message, egress.map(S::into_egress))
                        .await?
                }
                ReplicaAction::SendToAllReplicas(message) => {
                    write_message
                        .run(message, replica_egresses.values_mut().map(S::into_egress))
                        .await?
                }
                ReplicaAction::Finalize(commands) => {
                    finalize_sender
                        .send(Finalize {
                            commands,
                            view_num: replica.core.view_num,
                        })
                        .await?
                }
            }
        }
    }
}

pub struct Finalize {
    commands: Vec<super::Command>,
    view_num: ViewNum,
}

async fn service_task(
    replica_id: ReplicaId,
    config: ServiceConfig,
    submit_sender: Sender<ToReplica>,
    mut finalize_receiver: Receiver<Finalize>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_endpoint = Endpoint::server(
        server_config(),
        config.server_external_addresses[replica_id as usize],
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
            Select::Finalize(finalize) => 'finalize: {
                let Some(finalize) = finalize else {
                    tracing::warn!("finalize channel closed");
                    break 'finalize;
                };
                for command in finalize.commands {
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
                        view_num: finalize.view_num,
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
        &*egress
    }
}
