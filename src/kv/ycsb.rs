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

use crate::task::SegmentedTask;

use super::{KvOp, KvRes};

pub struct Ycsb {
    rng: StdRng,
    latency_records: Arc<Mutex<Vec<(Instant, Duration)>>>,

    tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
}

impl Ycsb {
    pub fn spawn(
        group: SegmentedTask,
        rng: StdRng,
        tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
    ) -> JoinHandle<()> {
        let mut ycsb = Self {
            rng,
            latency_records: Default::default(),
            tx_op,
        };
        spawn(async move {
            group.wrap_fallible(ycsb.run()).await;
            let latency_records = ycsb.latency_records.lock().unwrap();
            let total_duration = match (latency_records.first(), latency_records.last()) {
                (Some(first), Some(last)) => (last.0 + last.1).duration_since(first.0),
                _ => Duration::ZERO,
            };
            let tput = latency_records.len() as f64 / total_duration.as_secs_f64();
            let mean_latency = latency_records
                .iter()
                .map(|&(_, dur)| dur)
                .sum::<Duration>()
                / latency_records.len() as u32;
            tracing::info!(
                "YCSB done: {} ops in {total_duration:.1?} ({tput:.2} ops/sec), mean latency {mean_latency:?}",
                latency_records.len(),
            );
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            let k = format!("key{:08}", self.rng.random_range(..100_000u32));
            let op = if self.rng.random_bool(0.5) {
                KvOp::Get(k)
            } else {
                let v = (&mut self.rng)
                    .sample_iter(Alphanumeric)
                    .take(1 << 10)
                    .map(char::from)
                    .collect();
                KvOp::Put(k, v)
            };

            let (tx_res, rx_res) = oneshot::channel();
            let _ = self.tx_op.send((op, tx_res)).await;
            let start = Instant::now();
            let latency_records = self.latency_records.clone();
            spawn(async move {
                let Ok(_res) = rx_res.await else { return };
                // TODO check result
                let latency_record = (start, start.elapsed());
                latency_records.lock().unwrap().push(latency_record)
            });
        }
    }
}
