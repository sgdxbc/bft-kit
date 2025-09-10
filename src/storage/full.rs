use std::sync::Arc;

use rocksdb::{DB, WriteBatch};
use tokio::{
    spawn,
    sync::mpsc::Receiver,
    task::{JoinHandle, spawn_blocking},
};
use tokio_util::bytes::Bytes;

use crate::{storage::StorageKey, task::SegmentedTaskHandle};

use super::{StorageOp, StorageRes};

pub struct FullStorage {
    db: Arc<DB>,

    rx_op: Receiver<StorageOp>,
}

impl FullStorage {
    pub fn spawn(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>> + Send + 'static,
        rx_op: Receiver<StorageOp>,
    ) -> JoinHandle<()> {
        let mut storage = FullStorage {
            db: db.into(),
            rx_op,
        };
        spawn(task_handle.wrap(async move { storage.run().await }))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                StorageOp::Fetch(key, tx_value) => {
                    let db = self.db.clone();
                    let value = spawn_blocking(move || db.get(key)).await??;
                    let _ = tx_value.send(StorageRes::Ok(value.map(Into::into)));
                }
                StorageOp::Bump(bump, tx_ok) => {
                    for (key, value) in bump.inserts {
                        let db = self.db.clone();
                        spawn_blocking(move || db.put(key, value)).await??;
                    }
                    for key in bump.deletes {
                        let db = self.db.clone();
                        spawn_blocking(move || db.delete(key)).await??;
                    }
                    let _ = tx_ok.send(StorageRes::Ok(()));
                }
            }
        }
        Ok(())
    }
}

pub fn preload(
    db: &DB,
    mut items: impl Iterator<Item = anyhow::Result<(StorageKey, Bytes)>>,
) -> anyhow::Result<()> {
    loop {
        let mut batch = WriteBatch::new();
        for item in items.by_ref().take(10_000) {
            let (key, value) = item?;
            batch.put(key, value)
        }
        if batch.is_empty() {
            break;
        }
        db.write(batch)?
    }
    Ok(())
}
