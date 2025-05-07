use std::{collections::BTreeMap, time::Duration};

use hdrhistogram::Histogram;
use tokio::{
    sync::mpsc::{self, Receiver},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    ClientId, ClientSeq, Command, ReplicaId,
    replica::Quorum,
    transport::{
        AbstractReplica, AbstractService, ReplicaConfig, ReplicaTask, ServiceConfig, ServiceTask,
        Transport, boot_client,
    },
    workload::{self, ClientConfig, ConcurrentClients, Invoke, Latencies},
};

use super::{Replica, Spec, ToClient, message};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub workload: workload::Config,
    pub replica: ReplicaConfig,
    pub service: ServiceConfig,
    pub tick_interval: Duration,
}

pub const WARMUP_DURATION: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct ClientTask {
    pub spec: Spec,
    pub service_config: ServiceConfig,
}

impl crate::workload::ClientTask for ClientTask {
    async fn run(
        self,
        client_id: ClientId,
        config: ClientConfig,
        mut invoke_receiver: Receiver<Invoke>,
        mut context: impl crate::workload::AbstractContext,
    ) -> anyhow::Result<Latencies> {
        let (message_sender, mut message_receiver) = mpsc::channel(1000);
        let (mut transport, replica_egresses) =
            boot_client(client_id, self.service_config, message_sender).await?;

        let mut seq = 0;
        let mut latencies = Histogram::new(3)?;
        let start = Instant::now();
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
                    let command = Command { client_id, seq, op };
                    Transport::write(command, replica_egresses.values()).await?;
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
                        == self.spec.num_faulty + 1
                    {
                        let scratch = seq_scratch.remove(&reply.seq).unwrap();
                        if let Some(result) = scratch.expected_result {
                            anyhow::ensure!(reply.result == result)
                        }
                        let end = Instant::now();
                        if end.duration_since(start) >= WARMUP_DURATION {
                            latencies += end.duration_since(scratch.start).as_micros() as u64;
                        }
                        context.commit().await?
                    }
                }
            }
        }
    }
}

pub async fn clients_task(spec: Spec, config: TaskConfig) -> anyhow::Result<Vec<Latencies>> {
    ConcurrentClients::new()
        .run(
            config.workload,
            ClientTask {
                spec,
                service_config: config.service,
            },
        )
        .await
}

pub struct Service;
impl AbstractService for Service {
    type Reply = message::Reply;
    type Finalized = Vec<Command>;

    fn reply_seq(reply: &Self::Reply) -> ClientSeq {
        reply.seq
    }

    fn on_finalized(
        &mut self,
        finalized: Self::Finalized,
        task: &mut ServiceTask<Self>,
    ) -> impl Iterator<Item = (ClientId, Self::Reply)> {
        finalized.into_iter().filter_map(move |command| {
            if matches!(task.replies.get(&command.client_id), Some(reply) if reply.seq >= command.seq) {
                tracing::warn!(?command, "duplicated finalized");
                return None;
            }
            let reply = message::Reply {
                seq: command.seq,
                // a 0/0 service, extend to support arbitrary state machine later
                result: Default::default(),
                replica_id: task.replica_id,
            };
            task.replies.insert(command.client_id, reply.clone());
            Some((command.client_id, reply))
        })
    }
}

impl AbstractReplica for Replica {
    type Finalized = Vec<Command>;

    fn finalized(&self, commands: Vec<Command>) -> Self::Finalized {
        commands
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
