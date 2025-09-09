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

use crate::task::SegmentedTaskHandle;

use super::{KvOp, KvRes};

pub struct Ycsb {
    config: YcsbConfig,

    rng: StdRng,
    latency_records: Arc<Mutex<Vec<(Instant, Duration)>>>,

    task_handle: SegmentedTaskHandle,
    tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
}

pub struct YcsbConfig {
    get_ratio: f64,
    num_key: u32,
    value_size: usize,
}

impl Ycsb {
    pub fn spawn(
        task_handle: SegmentedTaskHandle,
        config: YcsbConfig,
        rng: StdRng,
        tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
    ) -> JoinHandle<()> {
        let mut ycsb = Self {
            config,
            rng,
            latency_records: Default::default(),
            task_handle: task_handle.clone(),
            tx_op,
        };
        spawn(async move {
            task_handle.wrap(ycsb.run()).await;
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
            let k = format!("key{:08}", self.rng.random_range(..self.config.num_key));
            let op = if self.rng.random_bool(self.config.get_ratio) {
                KvOp::Get(k)
            } else {
                let v = (&mut self.rng)
                    .sample_iter(Alphanumeric)
                    .take(self.config.value_size)
                    .map(char::from)
                    .collect();
                KvOp::Put(k, v)
            };

            let (tx_res, rx_res) = oneshot::channel();
            let _ = self.tx_op.send((op, tx_res)).await;
            let start = Instant::now();
            let latency_records = self.latency_records.clone();
            spawn(self.task_handle.clone().wrap(async move {
                let Ok(_res) = rx_res.await else {
                    return Ok(());
                };
                // TODO check result
                let latency_record = (start, start.elapsed());
                latency_records.lock().unwrap().push(latency_record);
                Ok(())
            }));
        }
    }
}

mod parse {
    use crate::parse::{Configs, Extract};

    use super::YcsbConfig;

    impl Extract for YcsbConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                get_ratio: configs.get("ycsb.get-ratio")?,
                num_key: configs.get("ycsb.num-key")?,
                value_size: configs.get("ycsb.value-size")?,
            })
        }
    }
}
