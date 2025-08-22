use std::time::Instant;

use rand::{Rng as _, rngs::StdRng};
use rand_distr::Alphanumeric;

use crate::workload::{NanoLatencies, WorkloadState};

use super::AppProtocol;

pub enum YcsbOp {
    Insert(String, String),
    Update(String, String),
    Get(String),
    Scan(String, usize),
}

pub enum YcsbRes {
    Ok,
    Err(String),
    Get(String),
    NotFound,
    Scan(Vec<(String, String)>),
}

// we don't generally implement data sharding app for YCSB because Scan is not
// supported by current data sharding execution interfaces

pub struct YcsbWorkload {
    config: WorkloadConfig,
    rng: StdRng,
    latencies: NanoLatencies,
}

pub struct WorkloadConfig {
    num_key: u64,
    value_len: usize,
}

impl YcsbWorkload {
    pub fn new(config: WorkloadConfig, rng: StdRng) -> Self {
        Self {
            config,
            rng,
            latencies: NanoLatencies::new(3).unwrap(),
        }
    }
}

impl From<YcsbWorkload> for NanoLatencies {
    fn from(workload: YcsbWorkload) -> Self {
        workload.latencies
    }
}

impl AppProtocol for YcsbWorkload {
    type Op = YcsbOp;
    type Res = YcsbRes;
}

impl WorkloadState for YcsbWorkload {
    type Metadata = Instant;

    fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)> {
        let k = format!("key{}", self.rng.random_range(0..self.config.num_key));
        let v = (&mut self.rng)
            .sample_iter(Alphanumeric)
            .take(self.config.value_len)
            .map(char::from)
            .collect();
        Some((YcsbOp::Update(k, v), Instant::now())) // TODO
    }

    fn complete(&mut self, start: Self::Metadata, res: Self::Res) -> anyhow::Result<()> {
        if let YcsbRes::Err(err) = res {
            anyhow::bail!(err)
        }
        self.latencies += start.elapsed().as_nanos() as u64;
        Ok(())
    }
}

mod parse {
    use crate::parse::Extract;

    use super::WorkloadConfig;

    impl Extract for WorkloadConfig {
        fn extract(configs: &crate::parse::Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_key: configs.get("ycsb.num-key")?,
                value_len: configs.get("ycsb.value-len")?,
            })
        }
    }
}
