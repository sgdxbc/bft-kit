use std::collections::HashMap;

use primitive_types::H256;
use tokio::{
    spawn,
    sync::{mpsc::Receiver, oneshot},
    task::JoinHandle,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

pub type StorageKey = H256;

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<StorageRes<Option<Bytes>>>),
    Bump(Bump, oneshot::Sender<StorageRes<()>>),
}

#[derive(Debug)]
pub enum StorageRes<T> {
    Ok(T),
    Forward(usize),
}

pub struct Bump {
    pub inserts: HashMap<StorageKey, Bytes>,
    pub deletes: Vec<StorageKey>,
}

pub struct Storage {
    rx_op: Receiver<StorageOp>,
}

impl Storage {
    pub fn spawn(cancel: CancellationToken, rx_op: Receiver<StorageOp>) -> JoinHandle<()> {
        let mut storage = Self { rx_op };
        spawn(async move {
            if let Some(Err(err)) = cancel.run_until_cancelled(storage.run()).await {
                tracing::error!(%err);
                cancel.cancel()
            }
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                StorageOp::Fetch(_key, tx_value) => {
                    let _ = tx_value.send(StorageRes::Ok(None));
                }
                StorageOp::Bump(_bump, tx_ok) => {
                    let _ = tx_ok.send(StorageRes::Ok(()));
                }
            }
        }
        Ok(())
    }
}
