use std::sync::Arc;

use rand::{Rng, rngs::StdRng};
use rand_distr::Alphanumeric;
use rocksdb::{DB, WriteBatch};
use tokio::{
    spawn,
    sync::mpsc::Receiver,
    task::{JoinHandle, spawn_blocking},
};

use crate::{
    crypto::DigestHash, kv::ycsb::YcsbConfig, storage::StorageKey, task::SegmentedTaskHandle,
};

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

pub fn preload_ycsb(db: &DB, config: YcsbConfig, mut rng: StdRng) -> anyhow::Result<()> {
    let mut batch = WriteBatch::new();
    for i in 0..config.num_key {
        if i % 10_000 == 0 {
            db.write(batch)?;
            batch = WriteBatch::new();
            tracing::info!("Preloaded {i} keys")
        }

        let key = format!("key{:08}", i);
        let value = (&mut rng)
            .sample_iter(Alphanumeric)
            .take(config.value_size)
            .map(char::from)
            .collect::<String>();

        let storage_key = StorageKey::from(key.digest().0);
        let value = bincode::encode_to_vec(&value, bincode::config::standard())?;
        batch.put(storage_key, value);
    }
    db.write(batch)?;
    Ok(())
}
