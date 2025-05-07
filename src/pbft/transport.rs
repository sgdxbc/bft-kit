use std::{collections::BTreeMap, time::Duration};

use hdrhistogram::Histogram;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    ClientId, ClientSeq, ReplicaId,
    pbft::{Command, ToClient, ViewNum},
    replica::Quorum,
    transport::{
        AbstractReplica, AbstractService, ReplicaConfig, ReplicaTask, ServiceConfig, ServiceTask,
        Transport, TransportAndSenders, boot_client,
    },
    workload::{ClientConfig, ConcurrentClients, Invoke, Latencies},
};

use super::{Replica, Spec, message};

pub mod tcp;

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub client: ClientConfig,
    pub replica: ReplicaConfig,
    pub service: ServiceConfig,
    pub use_tcp: bool,
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
    invoke_receiver: Receiver<Invoke>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Histogram<u32>> {
    client_task_with_bootstrap(
        spec,
        config,
        id,
        invoke_receiver,
        commit_sender,
        |message_sender| boot_client(id, service_config, message_sender),
    )
    .await
}

async fn client_task_with_bootstrap(
    spec: Spec,
    config: ClientConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<Invoke>,
    commit_sender: Sender<ClientId>,
    bootstrap: impl AsyncFnOnce(Sender<ToClient>) -> anyhow::Result<TransportAndSenders>,
) -> anyhow::Result<Latencies> {
    let (message_sender, mut message_receiver) = mpsc::channel(100);
    let (mut transport, replica_egresses) = bootstrap(message_sender).await?;

    struct SeqScratch {
        results: Quorum<Vec<u8>>,
        expected_result: Option<Vec<u8>>,
        start: Instant,
    }
    let mut seq = 0;
    let mut seq_scratch = BTreeMap::new();
    let mut view_num = 0;
    let mut latencies = Histogram::new(3)?;
    let start = Instant::now();
    loop {
        enum Select {
            Invoke(Option<Invoke>),
            Message(Option<ToClient>),
            TransportJoinNext(()),
        }
        match tokio::select! {
            invoke = invoke_receiver.recv() => Select::Invoke(invoke),
            message = message_receiver.recv() => Select::Message(message),
            result = transport.join_next() => Select::TransportJoinNext(result?)
        } {
            Select::TransportJoinNext(()) => unreachable!(),
            Select::Invoke(None) => break Ok(latencies),
            Select::Invoke(Some((op, result))) => {
                seq += 1;
                let command = Command {
                    client_id: id,
                    seq,
                    op,
                };
                let egress = replica_egresses.get(&spec.primary(view_num));
                anyhow::ensure!(egress.is_some());
                Transport::write(command, egress).await?;
                match &config {
                    // resend for close loop?
                    ClientConfig::CloseLoop => anyhow::ensure!(seq_scratch.is_empty()),
                    ClientConfig::OpenLoop(config) => {
                        if seq_scratch.len() == config.num_max_inflight {
                            seq_scratch.pop_first();
                        }
                    }
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
            Select::Message(None) => anyhow::bail!("message receive channel close"),
            Select::Message(Some(reply)) => {
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
                    let end = Instant::now();
                    if end.duration_since(start) >= WARMUP_DURATION {
                        latencies += end.duration_since(scratch.start).as_micros() as u64;
                    }
                    commit_sender.send(id).await?
                }
            }
        }
    }
}

pub async fn clients_task(spec: Spec, config: TaskConfig) -> anyhow::Result<Vec<Latencies>> {
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
    concurrent_clients
        .run(config.client, config.client_duration)
        .await
}

pub struct Finalized {
    commands: Vec<super::Command>,
    view_num: ViewNum,
}

pub struct Service;
impl AbstractService for Service {
    type Reply = message::Reply;
    type Finalized = Finalized;

    fn reply_seq(reply: &Self::Reply) -> ClientSeq {
        reply.seq
    }

    fn on_finalized(
        &mut self,
        finalized: Self::Finalized,
        task: &mut ServiceTask<Self>,
    ) -> impl Iterator<Item = (ClientId, Self::Reply)> {
        finalized.commands.into_iter().filter_map(move |command| {
            if matches!(task.replies.get(&command.client_id), Some(reply) if reply.seq >= command.seq) {
                tracing::warn!(?command, "duplicated finalize");
                return None;
            }
            let reply = message::Reply {
                seq: command.seq,
                // a 0/0 service, extend to support arbitrary state machine later
                result: Default::default(),
                replica_id: task.replica_id,
                view_num: finalized.view_num,
            };
            task.replies.insert(command.client_id, reply.clone());
            Some((command.client_id, reply))
        })
    }
}

impl AbstractReplica for Replica {
    type Finalized = Finalized;

    fn finalized(&self, commands: Vec<Command>) -> Self::Finalized {
        Finalized {
            commands,
            view_num: self.core.view_num,
        }
    }
}

pub async fn server_task(
    replica: Replica,
    config: TaskConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (request_sender, request_receiver) = mpsc::channel(1000);
    let (finalized_sender, finalized_receiver) = mpsc::channel(100);

    let replica_id = replica.core.config.id;
    let service_task = ServiceTask::<Service>::new(replica_id, request_sender).run(
        Service,
        config.service,
        finalized_receiver,
        cancel.clone(),
    );
    let replica_task = ReplicaTask::new(replica, finalized_sender).run(
        replica_id,
        config.replica,
        config.tick_interval,
        request_receiver,
    );
    tokio::try_join!(service_task, replica_task)?;
    Ok(())
}
