use bincode::{Decode, Encode};
use tokio::{
    spawn,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::app::{AppTypeConfig, StateOp};

pub struct Kv {
    rx_op: Receiver<(KvOp, oneshot::Sender<KvRes>)>,
    tx_state_op: Sender<StateOp<String, String>>,
}

impl AppTypeConfig for Kv {
    type Op = KvOp;
    type Res = KvRes;
    type Key = String;
    type Value = String;
}

impl Kv {
    pub fn spawn(
        cancel: CancellationToken,
        rx_op: Receiver<(KvOp, oneshot::Sender<KvRes>)>,
        tx_state_op: Sender<StateOp<String, String>>,
    ) -> JoinHandle<()> {
        let mut kv = Self { rx_op, tx_state_op };
        spawn(async move {
            if let Some(Err(err)) = cancel.run_until_cancelled(kv.run()).await {
                tracing::error!(%err);
                cancel.cancel()
            }
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut op_handle = spawn(async {});
        while let Some((op, tx_res)) = self.rx_op.recv().await {
            op_handle.abort();
            op_handle = spawn(Self::handle_op(op, tx_res, self.tx_state_op.clone()));
        }
        op_handle.await?;
        Ok(())
    }
}

#[derive(Debug, Encode, Decode)]
pub enum KvOp {
    Put(String, String),
    Get(String),
    Delete(String),
}

#[derive(Debug, Encode, Decode)]
pub enum KvRes {
    Get(Option<String>),
    Ok,
}

impl Kv {
    async fn handle_op(
        op: KvOp,
        tx_res: oneshot::Sender<KvRes>,
        tx_state_op: Sender<StateOp<String, String>>,
    ) {
        let res = match op {
            KvOp::Put(key, value) => {
                let _ = tx_state_op.send(StateOp::Put(key, value)).await;
                KvRes::Ok
            }
            KvOp::Get(key) => {
                let (tx_value, rx_value) = oneshot::channel();
                let _ = tx_state_op.send(StateOp::Get(key, tx_value)).await;
                let Ok(value) = rx_value.await else { return };
                KvRes::Get(value)
            }
            KvOp::Delete(key) => {
                let _ = tx_state_op.send(StateOp::Delete(key)).await;
                KvRes::Ok
            }
        };
        let _ = tx_res.send(res);
    }
}
