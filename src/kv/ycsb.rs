use rand::{Rng, rngs::StdRng};
use rand_distr::Alphanumeric;
use tokio::{
    spawn,
    sync::{mpsc::Sender, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{KvOp, KvRes};

pub struct Ycsb {
    rng: StdRng,
    num_ops: u64,

    tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
}

impl Ycsb {
    pub fn spawn(
        cancel: CancellationToken,
        rng: StdRng,
        tx_op: Sender<(KvOp, oneshot::Sender<KvRes>)>,
    ) -> JoinHandle<()> {
        let mut ycsb = Self {
            rng,
            num_ops: 0,
            tx_op,
        };
        spawn(async move {
            if let Some(Err(err)) = cancel.run_until_cancelled(ycsb.run()).await {
                tracing::error!(%err);
                cancel.cancel()
            } else {
                println!("YCSB finished {} operations", ycsb.num_ops)
            }
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
            let Ok(_res) = rx_res.await else { break };
            // TODO check result
            self.num_ops += 1
        }
        Ok(())
    }
}
