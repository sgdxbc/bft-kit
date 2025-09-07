use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use rand::{Rng, rngs::StdRng};
use rand_distr::Alphanumeric;
use tokio::{
    spawn,
    sync::{mpsc::Sender, oneshot},
    task::JoinHandle,
};

use crate::task::TaskGroup;

use super::{KvOp, KvRes};

pub struct Ycsb {
    rng: StdRng,
    latencies: Arc<Mutex<Vec<(Instant, Duration)>>>,

    tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
}

impl Ycsb {
    pub fn spawn(
        group: TaskGroup,
        rng: StdRng,
        tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
    ) -> JoinHandle<()> {
        let mut ycsb = Self {
            rng,
            latencies: Default::default(),
            tx_op,
        };
        spawn(async move {
            group.wrap_fallible(ycsb.run()).await;
            let latencies = ycsb.latencies.lock().unwrap();
            let total_duration = match (latencies.first(), latencies.last()) {
                (Some(first), Some(last)) => (last.0 + last.1).duration_since(first.0),
                _ => Duration::ZERO,
            };
            let tput = latencies.len() as f64 / total_duration.as_secs_f64();
            tracing::info!(
                "YCSB done: {} ops in {:?} ({:.2} ops/sec)",
                latencies.len(),
                total_duration,
                tput
            );
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let k = format!("key{:08}", self.rng.random_range(..1000u32));
            let op = if self.rng.random_bool(0.5) {
                KvOp::Get(k)
            } else {
                let v = (&mut self.rng)
                    .sample_iter(Alphanumeric)
                    .take(100)
                    .map(char::from)
                    .collect();
                KvOp::Put(k, v)
            };

            let (tx_res, rx_res) = oneshot::channel();
            let _ = self.tx_op.send((op, tx_res)).await;
            let start = Instant::now();
            let latencies = self.latencies.clone();
            spawn(async move {
                let Ok(_res) = rx_res.await else { return };
                // TODO check result
                latencies
                    .lock()
                    .unwrap()
                    .push((Instant::now(), start.elapsed()))
            });
        }
    }
}
