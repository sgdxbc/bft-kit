use std::{collections::HashMap, future::pending, pin::pin, time::Duration};

use futures_concurrency::future::{FutureGroup, Race, future_group::Key};
use futures_util::{StreamExt as _, future::BoxFuture};
use rand::random;
use tokio::{
    sync::{
        mpsc::{self, Receiver},
        oneshot,
    },
    time::{Instant, sleep},
};

use crate::{
    Command,
    command::{ClientId, ClientSeq},
    transport::{ServiceConfig, TransportTasks, WriteSenders, start_client, write},
};

use super::{ReplicaId, SecurityParams};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    params: SecurityParams,
    tick: ClientTickConfig,
}

#[derive(Debug, Clone)]
pub enum ClientTickConfig {
    CloseLoop(Duration), // of resending
    OpenLoop(Duration),  // of timeout
}

pub struct Client {
    id: ClientId,
    config: ClientConfig,
    transport_tasks: TransportTasks,
    write_senders: WriteSenders,
    message_receiver: Receiver<super::Reply>,

    seq: ClientSeq,
    request_states: HashMap<ClientSeq, RequestState>,
    ticks: FutureGroup<BoxFuture<'static, ClientTick>>,
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
    // currently a boolean flag of count == 0 should be sufficient
    // record richer information in case of e.g. supporting maximum number of
    // retries
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
                        > self.config.params.num_faulty_replica as usize
                    {
                        let state = self.request_states.remove(&reply.seq).unwrap();
                        if state.result_sender.send(reply.result).is_err() {
                            tracing::warn!("result channel closed")
                        }
                        self.ticks.remove(state.tick_key);
                        self.view_num = reply.view_num
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
                        let index = self.config.params.primary_of(self.view_num);
                        write(command, self.write_senders.get(&(index as _))).await?;

                        let tick_after = match self.config.tick {
                            ClientTickConfig::OpenLoop(timeout) => timeout,
                            ClientTickConfig::CloseLoop(timeout) => timeout,
                        };
                        state.tick_key = self.ticks.insert(Box::pin(async move {
                            sleep(tick_after).await;
                            ClientTick {
                                seq,
                                count: count + 1,
                            }
                        }))
                    } else {
                        match self.config.tick {
                            ClientTickConfig::OpenLoop(_) => {
                                self.ticks.remove(state.tick_key);
                                // implicitly close result channel
                            }
                            ClientTickConfig::CloseLoop(tick_after) => {
                                state.tick_key = self.ticks.insert(Box::pin(async move {
                                    sleep(tick_after).await;
                                    ClientTick {
                                        seq,
                                        count: count + 1,
                                    }
                                }));
                            }
                        }
                    }
                }
            }
        }
    }
}

pub enum WorkloadConfig {
    CloseLoop(CloseLoopWorkloadConfig),
    OpenLoop(OpenLoopWorkloadConfig),
}

pub struct CloseLoopWorkloadConfig {
    concurrency: usize,
    resend_internal: Duration,
}

pub struct OpenLoopWorkloadConfig {
    sending_rate: f32, // #request per second
    timeout: Duration,
}

impl WorkloadConfig {
    fn tick_config(&self) -> ClientTickConfig {
        match self {
            WorkloadConfig::CloseLoop(config) => {
                ClientTickConfig::CloseLoop(config.resend_internal)
            }
            WorkloadConfig::OpenLoop(config) => ClientTickConfig::OpenLoop(config.timeout),
        }
    }
}

pub async fn run_client(
    params: SecurityParams,
    workload_config: WorkloadConfig,
    service_config: ServiceConfig,
) -> anyhow::Result<()> {
    let client_config = ClientConfig {
        params,
        tick: workload_config.tick_config(),
    };
    match workload_config {
        WorkloadConfig::CloseLoop(config) => {
            let mut clients = FutureGroup::new();
            for _ in 0..config.concurrency {
                let mut client =
                    Client::start(ClientId(random()), client_config.clone(), &service_config)
                        .await?;
                clients.insert(async move {
                    loop {
                        let result = client.invoke(Default::default()); // TODO

                        enum Race {
                            ResultRecv(Result<Vec<u8>, oneshot::error::RecvError>),
                            Run(anyhow::Result<()>),
                        }
                        use Race::*;
                        match (async { ResultRecv(result.await) }, async {
                            Run(client.run().await)
                        })
                            .race()
                            .await
                        {
                            Run(Ok(())) | ResultRecv(Err(_)) => unreachable!(),
                            Run(Err(err)) => anyhow::bail!(err),
                            ResultRecv(Ok(_result)) => {}
                        }
                    }
                    #[allow(unreachable_code)]
                    anyhow::Ok(())
                });
            }
            //
        }
        WorkloadConfig::OpenLoop(config) => {
            let mut client =
                Client::start(ClientId(random()), client_config, &service_config).await?;
            let mut results = FutureGroup::new();

            let mut next_invoke = pin!(sleep(Duration::ZERO));
            loop {
                enum Race {
                    NextInvoke,
                    ResultRecv(Result<Vec<u8>, oneshot::error::RecvError>),
                    Run(anyhow::Result<()>),
                }
                use Race::*;

                let result = async {
                    if let Some(result) = results.next().await {
                        ResultRecv(result)
                    } else {
                        pending().await
                    }
                };
                match (
                    async {
                        next_invoke.as_mut().await;
                        NextInvoke
                    },
                    result,
                    async { Run(client.run().await) },
                )
                    .race()
                    .await
                {
                    Run(Ok(())) => unreachable!(),
                    Run(Err(err)) => anyhow::bail!(err),

                    NextInvoke => {
                        let result = client.invoke(Default::default()); // TODO
                        results.insert(result);
                        // randomize?
                        next_invoke.as_mut().reset(
                            Instant::now() + Duration::from_secs_f32(1.0 / config.sending_rate),
                        );
                    }
                    ResultRecv(Ok(_result)) => {
                        // TODO
                    }
                    ResultRecv(Err(_)) => {} // open loop abort on timeout
                }
            }
        }
    }
    Ok(())
}
