use std::{collections::BTreeMap, time::Duration};

use hdrhistogram::Histogram;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use crate::{
    common::{ClientId, ClientSeq, Command, Quorum, ReplicaId},
    transport::{
        AbstractReplica, AbstractService, ClientConfig, ReplicaConfig, Service, ServiceConfig,
        WriteMessage, boot_client, replica_task,
    },
    workload::{ConcurrentClients, Invoke, Latencies},
};

use super::{Replica, Spec, ToClient, message};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub client: ClientConfig,
    pub replica: ReplicaConfig,
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
                write_message.run(command, &replica_egresses).await?;
                match &config {
                    // resend for close loop?
                    ClientConfig::CloseLoop => anyhow::ensure!(seq_scratch.is_empty()),
                    ClientConfig::OpenLoop(config) => {
                        if seq_scratch.len() == config.num_max_concurrent {
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
    match config.client {
        ClientConfig::CloseLoop => concurrent_clients.close_loop(config.client_duration).await,
        ClientConfig::OpenLoop(client_config) => {
            concurrent_clients
                .open_loop(config.client_duration, client_config.sending_rate)
                .await
        }
    }
}

pub async fn run_open_loop_clients(
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
    concurrent_clients
        .open_loop(config.client_duration, 1.) // TODO
        .await
}

pub struct ServiceKit;
impl AbstractService for Service<ServiceKit> {
    type Reply = message::Reply;
    type Finalize = Vec<Command>;

    fn reply_seq(reply: &Self::Reply) -> ClientSeq {
        reply.seq
    }

    fn on_finalize(
        &mut self,
        finalize: Self::Finalize,
    ) -> impl Iterator<Item = (ClientId, Self::Reply)> {
        finalize.into_iter().filter_map(move |command| {
            if matches!(self.replies.get(&command.client_id), Some(reply) if reply.seq >= command.seq) {
                tracing::warn!(?command, "duplicated finalize");
                return None;
            }
            let reply = message::Reply {
                seq: command.seq,
                // a 0/0 service, extend to support arbitrary state machine later
                result: Default::default(),
                replica_id: self.replica_id,
            };
            self.replies.insert(command.client_id, reply.clone());
            Some((command.client_id, reply))
        })
    }
}

impl AbstractReplica for Replica {
    type Finalize = Vec<Command>;

    fn finalize(&self, commands: Vec<Command>) -> Self::Finalize {
        commands
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
