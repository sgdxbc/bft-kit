use std::time::Instant;

use rand::{Rng as _, rngs::StdRng};
use rand_distr::Alphanumeric;

use crate::{
    service::ServiceApp,
    workload::{NanoLatencies, WorkloadState},
};

pub struct Ycsb;

impl ServiceApp for Ycsb {
    type Op = YcsbOp;
    type Res = YcsbRes;
}

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

pub struct Workload {
    config: WorkloadConfig,
    rng: StdRng,
    latencies: NanoLatencies,
}

pub struct WorkloadConfig {
    num_key: u64,
    value_len: usize,
}

impl WorkloadState for Workload {
    type App = Ycsb;
    type Metadata = Instant;

    fn next_op(
        &mut self,
    ) -> Option<(
        <Self::App as crate::service::ServiceApp>::Op,
        Self::Metadata,
    )> {
        let k = format!("key{}", self.rng.random_range(0..self.config.num_key));
        let v = (&mut self.rng)
            .sample_iter(Alphanumeric)
            .take(self.config.value_len)
            .map(char::from)
            .collect();
        Some((YcsbOp::Update(k, v), Instant::now())) // TODO
    }

    fn complete(
        &mut self,
        start: Self::Metadata,
        res: <Self::App as crate::service::ServiceApp>::Res,
    ) -> anyhow::Result<()> {
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
                num_key: configs.get("ycsb.num_key")?,
                value_len: configs.get("ycsb.value_len")?,
            })
        }
    }
}
