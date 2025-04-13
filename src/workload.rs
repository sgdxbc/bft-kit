use std::{
    cmp::Ordering::{Equal, Less},
    collections::HashMap,
    time::Duration,
};

use hdrhistogram::Histogram;
use rand::random;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, timeout_at},
};

use crate::common::ClientId;

pub type Invoke = (Vec<u8>, Option<Vec<u8>>);

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

    pub fn spawn<F: Future<Output = anyhow::Result<Latencies>> + Send + 'static>(
        &mut self,
        task: impl FnOnce(ClientId, Receiver<Invoke>, Sender<ClientId>) -> F,
    ) {
        let id = ClientId(random());
        let (invoke_sender, invoke_receiver) = mpsc::channel(100);
        let replaced = self.invoke_senders.insert(id, invoke_sender);
        assert!(replaced.is_none());
        self.tasks
            .spawn(task(id, invoke_receiver, self.commit_sender.clone()));
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
            JoinNext,
        }
        while let Ok(select) = {
            let task = async {
                anyhow::Ok(tokio::select! {
                    commit = self.commit_receiver.recv() => Select::Commit(commit),
                    Some(result) = self.tasks.join_next() => { result??; Select::JoinNext },
                })
            };
            timeout_at(deadline, task).await
        } {
            let client_id = match select? {
                Select::JoinNext => unreachable!(),
                Select::Commit(None) => anyhow::bail!("commit receive channel closed"),
                Select::Commit(Some(client_id)) => client_id,
            };
            self.invoke_senders[&client_id]
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
}

impl Default for ConcurrentClients {
    fn default() -> Self {
        Self::new()
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
                tracing::info!("(omit the remaining per client latencies)")
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
