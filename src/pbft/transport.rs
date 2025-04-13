use std::{collections::BTreeMap, time::Duration};

use hdrhistogram::Histogram;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use crate::{
    common::{ClientId, ClientSeq, Quorum, ReplicaId},
    pbft::{Command, ToClient, ViewNum},
    transport::{
        AbstractEgress, AbstractReplica, AbstractService, ClientConfig, ReplicaConfig, Service,
        ServiceConfig, WriteMessage, boot_client, replica_task,
    },
    workload::{ConcurrentClients, Invoke, Latencies},
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
                        command,
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

pub struct Finalize {
    commands: Vec<super::Command>,
    view_num: ViewNum,
}

pub struct ServiceKit;
impl AbstractService for Service<ServiceKit> {
    type Reply = message::Reply;
    type Finalize = Finalize;

    fn reply_seq(reply: &Self::Reply) -> ClientSeq {
        reply.seq
    }

    fn on_finalize(
        &mut self,
        finalize: Self::Finalize,
    ) -> impl Iterator<Item = (ClientId, Self::Reply)> {
        finalize.commands.into_iter().filter_map(move |command| {
            if matches!(self.replies.get(&command.client_id), Some(reply) if reply.seq >= command.seq) {
                tracing::warn!(?command, "duplicated finalize");
                return None;
            }
            let reply = message::Reply {
                seq: command.seq,
                // a 0/0 service, extend to support arbitrary state machine later
                result: Default::default(),
                replica_id: self.replica_id,
                view_num: finalize.view_num,
            };
            self.replies.insert(command.client_id, reply.clone());
            Some((command.client_id, reply))
        })
    }
}

impl AbstractReplica for Replica {
    type Finalize = Finalize;

    fn finalize(&self, commands: Vec<Command>) -> Self::Finalize {
        Finalize {
            commands,
            view_num: self.core.view_num,
        }
    }
}

pub async fn server_task(replica: Replica, config: TaskConfig) -> anyhow::Result<()> {
    let (request_sender, request_receiver) = mpsc::channel(100);
    let (finalize_sender, finalize_receiver) = mpsc::channel(100);

    let service_task = Service::<ServiceKit>::new(replica.core.config.id, request_sender)
        .run(config.service.clone(), finalize_receiver);
    let replica_task = replica_task(
        replica.core.config.id,
        replica,
        config.replica,
        config.tick_interval,
        request_receiver,
        finalize_sender,
    );
    tokio::try_join!(service_task, replica_task)?;
    unreachable!()
}
