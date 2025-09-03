use std::sync::Arc;

use primitive_types::H256;
use rocksdb::{DB, WriteBatch};
use tokio::{
    sync::{mpsc::Receiver, oneshot},
    task::spawn_blocking,
};

pub type StorageKey = H256;

pub enum Invoke {
    Get(Vec<StorageKey>, oneshot::Sender<Vec<Option<Vec<u8>>>>),
    Put(Vec<(StorageKey, Vec<u8>)>, oneshot::Sender<()>),
}

pub async fn full_replication_loop(
    db: impl Into<Arc<DB>>,
    mut invoke_receiver: Receiver<Invoke>,
) -> anyhow::Result<()> {
    let db = db.into();
    // though supportable, the big service does not issue concurrent invocations, i.e., the invoke
    // receiver will not receive next Invoke before sending result for the previous one. so we don't
    // spawn database tasks in a JoinSet, since we won't benefit from its concurrency
    // on the other hand, the invocation interface still need to be channel based instead of async
    // methods that taking &mut self. (while full replication implementation does not require,) this
    // is for the sharded implementation to integrate invocation rounds into a big event loop that
    // selects on more kinds of events
    while let Some(invoke) = invoke_receiver.recv().await {
        match invoke {
            Invoke::Get(keys, res_sender) => {
                let db = db.clone();
                spawn_blocking(move || {
                    let values = db.multi_get(keys).into_iter().collect::<Result<_, _>>()?;
                    if res_sender.send(values).is_err() {
                        tracing::error!("result channel closed")
                    }
                    anyhow::Ok(())
                })
                .await??
            }
            Invoke::Put(updates, res_sender) => {
                let db = db.clone();
                spawn_blocking(move || {
                    let mut batch = WriteBatch::new();
                    for (key, value) in updates {
                        batch.put(key, value)
                    }
                    db.write(batch)?;
                    if res_sender.send(()).is_err() {
                        tracing::error!("result channel closed")
                    }
                    anyhow::Ok(())
                })
                .await??
            }
        }
    }
    Ok(())
}
