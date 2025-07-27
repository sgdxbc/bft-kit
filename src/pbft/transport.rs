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

pub struct Client {
    id: ClientId,
    transport_tasks: TransportTasks,
    write_senders: WriteSenders,
    message_receiver: Receiver<super::Reply>,

    seq: ClientSeq,
    request_states: HashMap<ClientSeq, RequestState>,
    timeouts: FutureGroup<Boxed<ClientTimeout>>,
    view_num: u64,
}

struct RequestState {
    op: Vec<u8>,
    results: HashMap<super::ReplicaId, Vec<u8>>,
    result_sender: oneshot::Sender<Vec<u8>>,
    resend_key: Key,
    remove_key: Key,
}

enum ClientTimeout {
    Resend(ClientSeq),
    Remove(ClientSeq),
}

impl Client {
    pub async fn start(id: ClientId, service_config: &ServiceConfig) -> anyhow::Result<Self> {
        let (message_sender, message_receiver) = mpsc::channel(100);
        let (transport_tasks, write_senders) =
            start_client(id.0 as _, service_config, message_sender).await?;
        Ok(Self {
            id,
            transport_tasks,
            write_senders,
            message_receiver,
            seq: 0,
            request_states: Default::default(),
            timeouts: Default::default(),
            view_num: 0,
        })
    }

    pub fn invoke(&mut self, op: Vec<u8>) -> oneshot::Receiver<Vec<u8>> {
        let (result_sender, result_receiver) = oneshot::channel();
        self.seq += 1;
        let seq = self.seq;
        let resend_key = self
            .timeouts
            .insert(Box::pin(async move { ClientTimeout::Resend(seq) }));
        let remove_key = self.timeouts.insert(Box::pin(async move {
            //
            ClientTimeout::Remove(seq)
        }));
        self.request_states.insert(
            seq,
            RequestState {
                op,
                results: Default::default(),
                result_sender,
                resend_key,
                remove_key,
            },
        );
        result_receiver
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Race {
                MessageRecv(super::Reply),
                Timeout(ClientTimeout),
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
                if let Some(timeout) = self.timeouts.next().await {
                    Timeout(timeout)
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
                        // TODO
                        > 1
                    {
                        let state = self.request_states.remove(&reply.seq).unwrap();
                        if state.result_sender.send(reply.result).is_err() {
                            tracing::warn!("result channel closed")
                        }
                        self.timeouts.remove(state.resend_key);
                        self.timeouts.remove(state.remove_key);
                    }
                }
                Timeout(ClientTimeout::Resend(seq)) => {
                    let Some(state) = self.request_states.get_mut(&seq) else {
                        unreachable!()
                    };
                    let command = Command {
                        client_id: self.id,
                        seq,
                        op: state.op.clone(),
                    };
                    // TODO
                    write(command, self.write_senders.get(&0)).await?;
                    state.resend_key = self.timeouts.insert(Box::pin(async move {
                        //
                        ClientTimeout::Resend(seq)
                    }))
                }
                Timeout(ClientTimeout::Remove(seq)) => {
                    let Some(state) = self.request_states.remove(&seq) else {
                        unreachable!()
                    };
                    self.timeouts.remove(state.resend_key);
                }
            }
        }
    }
}

pub async fn run_client(service_config: ServiceConfig) -> anyhow::Result<()> {
    Ok(())
}
