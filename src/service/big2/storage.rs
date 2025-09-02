use std::sync::Arc;

use primitive_types::H256;
use rocksdb::{DB, WriteBatch};
use tokio::{
    select,
    sync::{mpsc::Receiver, oneshot},
    task::JoinSet,
};

pub type StorageKey = H256;

pub enum Invoke {
    Get(Vec<StorageKey>, oneshot::Sender<Vec<Option<Vec<u8>>>>),
    Put(Vec<(StorageKey, Vec<u8>)>, oneshot::Sender<()>),
}

pub async fn full_replication_loop(
    db: DB,
    mut invoke_receiver: Receiver<Invoke>,
) -> anyhow::Result<()> {
    let db = Arc::new(db);
    let mut db_workers = JoinSet::new();
    loop {
        enum Event<I, W> {
            Invoke(I),
            Worker(W),
        }
        match select! {
            invoke = invoke_receiver.recv() => Event::Invoke(invoke),
            Some(worker) = db_workers.join_next() => Event::Worker(worker),
        } {
            Event::Invoke(None) => break,
            Event::Invoke(Some(Invoke::Get(keys, res_sender))) => {
                let db = db.clone();
                db_workers.spawn_blocking(move || {
                    let values = db
                        .multi_get(keys)
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;
                    if res_sender.send(values).is_err() {
                        tracing::error!("result channel closed")
                    }
                    anyhow::Ok(())
                });
            }
            Event::Invoke(Some(Invoke::Put(updates, res_sender))) => {
                let db = db.clone();
                db_workers.spawn_blocking(move || {
                    let mut batch = WriteBatch::new();
                    for (key, value) in updates {
                        batch.put(key, value)
                    }
                    db.write(batch)?;
                    if res_sender.send(()).is_err() {
                        tracing::error!("result channel closed")
                    }
                    anyhow::Ok(())
                });
            }
            Event::Worker(Ok(Ok(()))) => {}
            Event::Worker(err) => err??,
        }
    }
    Ok(())
}
