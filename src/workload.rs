use std::{
    cmp::Ordering::{Equal, Less},
    collections::HashMap,
    pin::pin,
    time::Duration,
};

use hdrhistogram::Histogram;
use rand::random;
use rand_distr::{Distribution, Exp};
use tokio::{
    sync::{
        mpsc::{self, Receiver, Sender},
        oneshot,
    },
    task::JoinSet,
    time::{Instant, sleep_until, timeout_at},
};

use crate::ClientId;

#[derive(Debug, Clone)]
pub struct Config {
    pub client: ClientConfig,
    pub num_client: usize,
    pub duration: Duration,
}

#[derive(Debug, Clone)]
pub enum ClientConfig {
    CloseLoop,
    OpenLoop(OpenLoopClientConfig),
}

#[derive(Debug, Clone)]
pub struct OpenLoopClientConfig {
    pub sending_rate: f32,
    pub num_max_inflight: usize, // per client
}

pub struct ConcurrentClients {
    pub tasks: JoinSet<anyhow::Result<Latencies>>,
    pub invoke_senders: HashMap<ClientId, Sender<Invoke>>,
    pub commit_sender: Sender<ClientId>,
    pub commit_receiver: Receiver<ClientId>,
}

pub type Latencies = hdrhistogram::Histogram<u32>;

impl ConcurrentClients {
    pub fn new() -> Self {
        let (commit_sender, commit_receiver) = mpsc::channel(100);
        Self {
            tasks: JoinSet::new(),
            invoke_senders: Default::default(),
            commit_sender,
            commit_receiver,
        }
    }
}

pub type Invoke = (Vec<u8>, Option<Vec<u8>>);

pub trait ClientTask {
    fn run(
        self,
        client_id: ClientId,
        config: ClientConfig,
        invoke_receiver: Receiver<Invoke>,
        context: impl AbstractContext + Send,
    ) -> impl Future<Output = anyhow::Result<Latencies>> + Send;
}

pub trait AbstractContext {
    fn commit(&mut self) -> impl Future<Output = anyhow::Result<()>> + Send;
}

pub struct Context(ClientId, Sender<ClientId>);

impl AbstractContext for Context {
    async fn commit(&mut self) -> anyhow::Result<()> {
        self.1.send(self.0).await?;
        Ok(())
    }
}

impl ConcurrentClients {
    pub fn spawn(&mut self, task: impl ClientTask + 'static, config: ClientConfig) {
        let id = ClientId(random());
        let (invoke_sender, invoke_receiver) = mpsc::channel(100);
        let replaced = self.invoke_senders.insert(id, invoke_sender);
        assert!(replaced.is_none());
        self.tasks.spawn(task.run(
            id,
            config,
            invoke_receiver,
            Context(id, self.commit_sender.clone()),
        ));
    }

    pub async fn run(
        mut self,
        config: Config,
        task: impl ClientTask + Clone + 'static,
    ) -> anyhow::Result<Vec<Latencies>> {
        for _ in 0..config.num_client {
            self.spawn(task.clone(), config.client.clone())
        }
        match config.client {
            ClientConfig::CloseLoop => self.close_loop(config.duration).await,
            ClientConfig::OpenLoop(OpenLoopClientConfig { sending_rate, .. }) => {
                self.open_loop(config.duration, sending_rate).await
            }
        }
    }

    pub async fn close_loop(mut self, duration: Duration) -> anyhow::Result<Vec<Latencies>> {
        for sender in self.invoke_senders.values() {
            sender
                .send((Default::default(), Some(Default::default())))
                .await?
        }

        let deadline = Instant::now() + duration;
        enum Select {
            Commit(Option<ClientId>),
            #[allow(dead_code)]
            JoinNext(Latencies),
        }
        while let Ok(select) = {
            let task = async {
                anyhow::Ok(tokio::select! {
                    commit = self.commit_receiver.recv() => Select::Commit(commit),
                    Some(result) = self.tasks.join_next() => Select::JoinNext(result??),
                })
            };
            timeout_at(deadline, task).await
        } {
            let client_id = match select? {
                Select::JoinNext(_) | Select::Commit(None) => unreachable!(),
                Select::Commit(Some(client_id)) => client_id,
            };
            self.invoke_senders[&client_id]
                // TODO
                .send((Default::default(), Some(Default::default())))
                .await?
        }

        drop(self.invoke_senders);
        let mut latencies = Vec::new();
        while let Some(client_latencies) = self.tasks.join_next().await {
            latencies.push(client_latencies??)
        }
        Ok(latencies)
    }

    pub async fn open_loop(
        mut self,
        duration: Duration,
        sending_rate: f32,
    ) -> anyhow::Result<Vec<Latencies>> {
        let now = Instant::now();
        let mut invoke_senders = self.invoke_senders.values().cycle();
        let mut sleep = pin!(sleep_until(now));
        let interval = Exp::new(sending_rate)?;

        enum Select {
            Sleep,
            Commit(Option<ClientId>),
            JoinNext,
        }
        let deadline = now + duration;
        while let Ok(select) = {
            let task = async {
                anyhow::Ok(tokio::select! {
                    () = &mut sleep => Select::Sleep,
                    commit = self.commit_receiver.recv() => Select::Commit(commit),
                    Some(result) = self.tasks.join_next() => { result??; Select::JoinNext },
                })
            };
            timeout_at(deadline, task).await
        } {
            match select? {
                Select::JoinNext | Select::Commit(None) => unreachable!(),
                Select::Sleep => {
                    invoke_senders
                        .next()
                        .unwrap()
                        // TODO
                        .send((Default::default(), Some(Default::default())))
                        .await?;
                    let deadline = sleep.deadline();
                    sleep.as_mut().reset(
                        deadline + Duration::from_secs_f32(interval.sample(&mut rand::rng())),
                    )
                }
                Select::Commit(Some(_)) => {}
            }
        }

        drop(self.invoke_senders);
        let mut latencies = Vec::new();
        while let Some(client_latencies) = self.tasks.join_next().await {
            latencies.push(client_latencies??)
        }
        Ok(latencies)
    }
}

impl Default for ConcurrentClients {
    fn default() -> Self {
        Self::new()
    }
}

impl AbstractContext for Option<oneshot::Sender<()>> {
    async fn commit(&mut self) -> anyhow::Result<()> {
        let Some(sender) = self.take() else {
            anyhow::bail!("multiple commit")
        };
        sender
            .send(())
            .map_err(|()| anyhow::format_err!("channel closed"))
    }
}

pub fn report_latencies(client_latencies: Vec<Latencies>, duration: Duration) {
    let num_client_latencies = client_latencies.len();
    let mut latencies = Histogram::new(3).expect("valid histogram parameter");
    for (i, client_latencies) in client_latencies.into_iter().enumerate() {
        match i.cmp(&5) {
            Less => {
                let throughput = client_latencies.len() as f32 / duration.as_secs_f32();
                let latency_mean = client_latencies.mean() / 1_000_000.;
                tracing::info!(
                    "client throughput {throughput:.2} ({:.2} = inverse of mean latency {:?})",
                    1. / latency_mean,
                    Duration::from_secs_f64(latency_mean),
                )
            }
            Equal => {
                tracing::info!(
                    "(omit the remaining {} per client latencies)",
                    num_client_latencies - 5
                )
            }
            _ => {}
        }
        latencies += client_latencies
    }
    let throughput = latencies.len() as f32 / duration.as_secs_f32();
    let latency_mean = latencies.mean() / 1_000_000.;
    tracing::info!(
        "throughput {throughput:.2} ({:.2} = {num_client_latencies} * inverse of mean latency {:?})",
        num_client_latencies as f64 / latency_mean,
        Duration::from_secs_f64(latency_mean),
    );
    for (i, value) in latencies.iter_quantiles(1).enumerate() {
        if value.count_since_last_iteration() == 0 || i >= 2 && value.quantile() < 0.99 {
            continue;
        }
        tracing::info!(
            "quantile {:8.6} latency {:10?}",
            value.quantile(),
            Duration::from_micros(value.value_iterated_to()),
        )
    }
    for value in latencies.iter_log(1, 2.) {
        let count = value.count_since_last_iteration();
        if count == 0 {
            continue;
        }
        tracing::info!(
            "{:8} ({:4.2}) requests <= {:?}",
            count,
            count as f32 / latencies.len() as f32,
            Duration::from_micros(value.value_iterated_to())
        );
    }
}
