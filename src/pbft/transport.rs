use std::{collections::HashMap, future::pending};

use futures_concurrency::future::{FutureGroup, Race, future_group::Key};
use futures_lite::{StreamExt as _, future::Boxed};
use tokio::sync::{
    mpsc::{self, Receiver},
    oneshot,
};

use crate::{
    Command,
    command::{ClientId, ClientSeq},
    transport::{ServiceConfig, TransportTasks, WriteSenders, start_client, write},
};

use super::ReplicaId;

pub struct ClientConfig {
    num_replica: ReplicaId,
    num_faulty_replica: ReplicaId,
}

pub struct Client {
    id: ClientId,
    config: ClientConfig,
    transport_tasks: TransportTasks,
    write_senders: WriteSenders,
    message_receiver: Receiver<super::Reply>,

    seq: ClientSeq,
    request_states: HashMap<ClientSeq, RequestState>,
    ticks: FutureGroup<Boxed<ClientTick>>,
    view_num: u64,
}

struct RequestState {
    op: Vec<u8>,
    results: HashMap<ReplicaId, Vec<u8>>,
    result_sender: oneshot::Sender<Vec<u8>>,
    tick_key: Key,
}

struct ClientTick {
    seq: ClientSeq,
    count: usize,
}

impl Client {
    pub async fn start(
        id: ClientId,
        config: ClientConfig,
        service_config: &ServiceConfig,
    ) -> anyhow::Result<Self> {
        let (message_sender, message_receiver) = mpsc::channel(100);
        let (transport_tasks, write_senders) =
            start_client(id.0 as _, service_config, message_sender).await?;
        Ok(Self {
            id,
            config,
            transport_tasks,
            write_senders,
            message_receiver,
            seq: 0,
            request_states: Default::default(),
            ticks: Default::default(),
            view_num: 0,
        })
    }

    pub fn invoke(&mut self, op: Vec<u8>) -> oneshot::Receiver<Vec<u8>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.seq += 1;
        let seq = self.seq;
        let tick_key = self
            .ticks
            // the first tick of a sequence number fires immediately to send the initial
            // request. this is probably not desirably efficient, but it can keep the async
            // (or "un-pure") stuff out of this method
            .insert(Box::pin(async move { ClientTick { seq, count: 0 } }));
        self.request_states.insert(
            seq,
            RequestState {
                op,
                results: Default::default(),
                result_sender,
                tick_key,
            },
        );
        result_receiver
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Race {
                MessageRecv(super::Reply),
                Tick(ClientTick),
                TransportTask(Option<anyhow::Result<()>>),
            }
            use Race::*;
            let message = async {
                if let Some(message) = self.message_receiver.recv().await {
                    MessageRecv(message)
                } else {
                    tracing::warn!("message channel closed");
                    pending().await
                }
            };
            let timeout = async {
                if let Some(timeout) = self.ticks.next().await {
                    Tick(timeout)
                } else {
                    pending().await
                }
            };
            match (message, timeout, async {
                TransportTask(self.transport_tasks.next().await)
            })
                .race()
                .await
            {
                TransportTask(None) => unimplemented!(),
                TransportTask(Some(Ok(()))) => unreachable!(),
                TransportTask(Some(Err(err))) => anyhow::bail!(err),

                MessageRecv(reply) => {
                    let Some(state) = self.request_states.get_mut(&reply.seq) else {
                        continue;
                    };
                    state.results.insert(reply.replica_id, reply.result.clone());
                    if state
                        .results
                        .values()
                        .filter(|&result| result == &reply.result)
                        .count()
                        > self.config.num_faulty_replica as usize
                    {
                        let state = self.request_states.remove(&reply.seq).unwrap();
                        if state.result_sender.send(reply.result).is_err() {
                            tracing::warn!("result channel closed")
                        }
                        self.ticks.remove(state.tick_key);
                    }
                }
                Tick(ClientTick { seq, count }) => {
                    let Some(state) = self.request_states.get_mut(&seq) else {
                        unreachable!()
                    };
                    if count == 0 {
                        let command = Command {
                            client_id: self.id,
                            seq,
                            op: state.op.clone(),
                        };
                        let index = self.view_num as ReplicaId % self.config.num_replica;
                        write(command, self.write_senders.get(&(index as _))).await?;
                        state.tick_key = self.ticks.insert(Box::pin(async move {
                            // TODO timeout
                            ClientTick {
                                seq,
                                count: count + 1,
                            }
                        }))
                    } else {
                        let Some(state) = self.request_states.get_mut(&seq) else {
                            unreachable!()
                        };
                        // TODO error in close loop case
                        self.ticks.remove(state.tick_key);
                    }
                }
            }
        }
    }
}

pub async fn run_client(service_config: ServiceConfig) -> anyhow::Result<()> {
    Ok(())
}
