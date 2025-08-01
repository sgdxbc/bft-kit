use std::{collections::HashMap, pin::pin, time::Duration};

use hdrhistogram::Histogram;
use rand::random;
use tokio::{
    select,
    sync::{
        mpsc::{self, Receiver},
        oneshot,
    },
    task::{AbortHandle, JoinError, JoinSet},
    time::{Instant, sleep},
};

use crate::{
    Command,
    command::{ClientId, ClientSeq},
    transport::{ServiceConfig, WriteSenders, start_client, write},
};

use super::{ReplicaId, SecurityParams};

#[derive(Debug, Clone)]
pub struct ClientConfig {
    params: SecurityParams,
    request_timeout: Duration,
    close_loop: bool,
}

pub struct Client {
    id: ClientId,
    config: ClientConfig,

    seq: ClientSeq,
    view_num: u64,
    request_states: HashMap<ClientSeq, RequestState>,
    timeout_tasks: JoinSet<ClientSeq>,
}

type Invoke = (Vec<u8>, oneshot::Sender<Vec<u8>>); // (op, result_sender)

struct RequestState {
    op: Vec<u8>,
    results: HashMap<ReplicaId, Vec<u8>>,
    result_sender: oneshot::Sender<Vec<u8>>,
    timeout_handle: AbortHandle,
}

impl Client {
    fn new(id: ClientId, config: ClientConfig) -> Self {
        Self {
            id,
            config,
            seq: 0,
            request_states: Default::default(),
            timeout_tasks: Default::default(),
            view_num: 0,
        }
    }

    async fn run(
        mut self,
        mut invoke_receiver: Receiver<Invoke>,
        write_senders: WriteSenders,
        mut message_receiver: Receiver<super::Reply>,
    ) -> anyhow::Result<()> {
        enum Event {
            Invoke(Option<(Vec<u8>, oneshot::Sender<Vec<u8>>)>),
            Message(super::Reply),
            Timeout(Result<ClientSeq, JoinError>),
        }
        use Event::*;
        loop {
            match select! {
                invoke = invoke_receiver.recv() => Invoke(invoke),
                Some(message) = message_receiver.recv() => Message(message),
                Some(timeout) = self.timeout_tasks.join_next() => Timeout(timeout),
            } {
                Invoke(None) => break Ok(()),
                Invoke(Some((op, result_sender))) => {
                    if self.config.close_loop {
                        anyhow::ensure!(self.request_states.is_empty())
                    }
                    self.seq += 1;
                    let seq = self.seq;

                    let command = Command {
                        client_id: self.id,
                        seq,
                        op: op.clone(),
                    };
                    let index = self.config.params.primary_of(self.view_num);
                    write(command, write_senders.get(&(index as _))).await?;
                    let timeout = self.config.request_timeout;
                    let timeout_handle = self.timeout_tasks.spawn(async move {
                        sleep(timeout).await;
                        seq
                    });

                    self.request_states.insert(
                        seq,
                        RequestState {
                            op,
                            result_sender,
                            timeout_handle,
                            results: Default::default(),
                        },
                    );
                }
                Message(reply) => {
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
                        state.timeout_handle.abort();
                        self.view_num = reply.view_num
                    }
                }
                Timeout(Err(err)) if err.is_cancelled() => {}
                Timeout(Err(_)) => unreachable!("timeout tasks never panic"),
                Timeout(Ok(seq)) => {
                    anyhow::ensure!(!self.config.close_loop, "request #{seq} timeout");
                    self.request_states.remove(&seq);
                    // implicitly close result channel
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
}

pub struct OpenLoopWorkloadConfig {
    sending_rate: f32, // #request per second
}

pub struct ClientWorkloadConfig {
    duration: Duration,
    workload: WorkloadConfig,
    service: ServiceConfig,
    params: SecurityParams,
    request_timeout: Duration,
}

impl ClientWorkloadConfig {
    fn client(&self) -> ClientConfig {
        ClientConfig {
            params: self.params.clone(),
            request_timeout: self.request_timeout,
            close_loop: matches!(self.workload, WorkloadConfig::CloseLoop(_)),
        }
    }
}

pub async fn run_client_workload(config: ClientWorkloadConfig) -> anyhow::Result<()> {
    let mut transport_tasks = JoinSet::new();
    let mut latencies = Histogram::<u64>::new(3).unwrap();
    match &config.workload {
        WorkloadConfig::CloseLoop(workload_config) => {}
        WorkloadConfig::OpenLoop(workload_config) => {
            let id = random();
            let (message_sender, message_receiver) = mpsc::channel(100);
            let write_senders =
                start_client(id, &config.service, message_sender, &mut transport_tasks).await?;

            let client = Client::new(ClientId(id as _), config.client());
            let (invoke_sender, invoke_receiver) = mpsc::channel(1);
            let client_task = client.run(invoke_receiver, write_senders, message_receiver);

            let invoke_task = async {
                let mut invoke_tasks = JoinSet::new();
                let mut next_invoke = pin!(sleep(Duration::ZERO));
                loop {
                    enum Event {
                        NextInvoke,
                        Invoke(Result<anyhow::Result<Duration>, JoinError>),
                    }
                    use Event::*;
                    match select! {
                        () = &mut next_invoke => NextInvoke,
                        Some(res) = invoke_tasks.join_next() => Invoke(res),
                    } {
                        NextInvoke => {
                            let (result_sender, result) = oneshot::channel();
                            invoke_sender
                                .send((Default::default(), result_sender)) // TODO
                                .await?;
                            invoke_tasks.spawn(async {
                                let start = Instant::now();
                                let result = result.await?;
                                anyhow::ensure!(result == Vec::new()); // TODO
                                Ok(start.elapsed())
                            });
                            next_invoke.as_mut().reset(
                                Instant::now()
                            // randomize?
                                + Duration::from_secs_f32(1.0 / workload_config.sending_rate),
                            )
                        }
                        Invoke(Ok(Ok(latency))) => latencies += latency.as_micros() as u64,
                        Invoke(Ok(Err(err))) if err.is::<oneshot::error::RecvError>() => {}
                        Invoke(Err(err)) => anyhow::bail!(err),
                        Invoke(Ok(Err(err))) => anyhow::bail!(err),
                    }
                }
                #[allow(unreachable_code)]
                Ok(())
            };

            tokio::select! {
                result = client_task => result?,
                result = invoke_task => result?,
                Some(result) = transport_tasks.join_next() => result??,
                
            }
        }
    }

    Ok(())
}
