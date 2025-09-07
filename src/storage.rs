use std::{
    collections::{HashMap, HashSet},
    iter::once,
    sync::Arc,
};

use primitive_types::H256;
use rand::{Rng as _, SeedableRng as _, rngs::StdRng, seq::IteratorRandom as _};
use rocksdb::DB;
use tokio::{
    spawn,
    sync::{mpsc::Receiver, oneshot},
    task::{JoinHandle, spawn_blocking},
};
use tokio_util::bytes::Bytes;

use crate::task::TaskGroup;

pub type StorageKey = H256;
type StateVersion = u64;
type NodeIndex = u16;
type ActiveGroupIndex = NodeIndex;
type StripeIndex = u16;
type StripeShardIndex = usize;

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
    db: Arc<DB>,
    config: ShardedStorageConfig,
    version: StateVersion,

    rx_op: Receiver<StorageOp>,
}

impl Storage {
    pub fn spawn(
        group: TaskGroup,
        db: impl Into<Arc<DB>>,
        config: ShardedStorageConfig,
        rx_op: Receiver<StorageOp>,
    ) -> JoinHandle<()> {
        let mut storage = Self {
            db: db.into(),
            config,
            version: 0,
            rx_op,
        };
        spawn(async move { group.wrap_fallible(storage.run()).await })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(op) = self.rx_op.recv().await {
            match op {
                StorageOp::Fetch(key, tx_value) => {
                    let value = get_versioned(
                        self.db.clone(),
                        &key,
                        self.version,
                        self.config.stripe_of(&key),
                    )
                    .await?;
                    let _ = tx_value.send(StorageRes::Ok(value));
                }
                StorageOp::Bump(bump, tx_ok) => {
                    self.version += 1;
                    for (key, value) in bump.inserts {
                        put_versioned(
                            self.db.clone(),
                            &key,
                            self.version,
                            value,
                            self.config.stripe_of(&key),
                        )
                        .await?
                    }
                    for key in bump.deletes {
                        delete_versioned(
                            self.db.clone(),
                            &key,
                            self.version,
                            self.config.stripe_of(&key),
                        )
                        .await?
                    }
                    let _ = tx_ok.send(StorageRes::Ok(()));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ShardedStorageConfig {
    num_node: NodeIndex, // virtual "storage node"
    num_faulty_node: NodeIndex,
    num_stripe: StripeIndex,
    num_active_copy: usize,
    // pub repair_threshold: NodeIndex,
    bypass_vote: bool,
}

impl ShardedStorageConfig {
    // active tier
    fn num_active_group(&self) -> ActiveGroupIndex {
        self.num_node
    }

    fn primary_node_of_group(&self, index: ActiveGroupIndex) -> NodeIndex {
        index
    }

    fn is_primary_of(
        &self,
        node_indices: &HashSet<NodeIndex>,
    ) -> impl Iterator<Item = ActiveGroupIndex> {
        node_indices.iter().copied()
    }

    fn nodes_of_group(&self, index: ActiveGroupIndex) -> impl Iterator<Item = NodeIndex> {
        let sampled = (0..self.num_node - 1).choose_multiple(
            &mut StdRng::seed_from_u64(index as _),
            self.num_active_copy - 1,
        );
        let node_index = self.primary_node_of_group(index);
        once(node_index).chain(
            sampled
                .into_iter()
                .map(move |i| if i >= node_index { i + 1 } else { i }),
        )
    }

    fn groups_of_nodes(
        &self,
        node_indices: &HashSet<NodeIndex>,
    ) -> impl Iterator<Item = ActiveGroupIndex> {
        (0..self.num_active_group()).filter(|&index| {
            self.nodes_of_group(index)
                .any(|node_index| node_indices.contains(&node_index))
        })
    }

    fn group_of(&self, key: &StorageKey) -> ActiveGroupIndex {
        StdRng::from_seed(key.0).random_range(0..self.num_active_group())
    }

    fn nodes_of(&self, key: &StorageKey) -> impl Iterator<Item = NodeIndex> {
        self.nodes_of_group(self.group_of(key))
    }

    // archive tier
    fn repair_threshold(&self) -> NodeIndex {
        self.num_faulty_node + 1 // make this configurable only if needed (probably for performance)
    }

    fn stripe_width(&self) -> NodeIndex {
        self.repair_threshold() + self.num_faulty_node * 2
    }

    fn stripe_of(&self, key: &StorageKey) -> StripeIndex {
        // assuming the keys are uniformly distributed, by hashing
        (key.to_low_u64_le() % self.num_stripe as u64) as _
    }

    fn archive_nodes_of_stripe(&self, index: StripeIndex) -> impl Iterator<Item = NodeIndex> {
        (index..)
            .take(self.stripe_width() as _)
            .map(|index| (index % self.num_node as StripeIndex) as _)
    }

    fn archive_placements(
        &self,
        node_index: NodeIndex,
    ) -> impl Iterator<Item = (StripeIndex, StripeShardIndex)> {
        (0..self.num_stripe).filter_map(move |stripe_index| {
            self.archive_nodes_of_stripe(stripe_index)
                .enumerate()
                .find_map(|(shard_index, placed_node_index)| {
                    if placed_node_index == node_index {
                        Some((stripe_index, shard_index))
                    } else {
                        None
                    }
                })
        })
    }
}

// key spec
// {stripe index:04x}.{key:x}.{version:08x}(.delete)
// {stripe index:04x}/{version:08x}-{shard index:08x}

async fn get_versioned(
    db: Arc<DB>,
    key: &StorageKey,
    version: StateVersion,
    stripe_index: StripeIndex,
) -> anyhow::Result<Option<Bytes>> {
    let prefix = format!("{stripe_index:04x}.{key:x}");
    let seek_key = format!("{prefix}.{version:08x}");
    let value = spawn_blocking(move || {
        let mut iter = db.raw_iterator();
        iter.seek_for_prev(seek_key);
        iter.status()?;
        let value = if let Some((found_key, value)) = iter.item()
            && let Some(postfix) = found_key.strip_prefix(prefix.as_bytes())
            && !postfix.ends_with(b".delete")
        {
            Some(Bytes::copy_from_slice(value))
        } else {
            None
        };
        anyhow::Ok(value)
    })
    .await??;
    Ok(value)
}

async fn put_versioned(
    db: Arc<DB>,
    key: &StorageKey,
    version: StateVersion,
    value: Bytes,
    stripe_index: StripeIndex,
) -> anyhow::Result<()> {
    let key = format!("{stripe_index:04x}.{key:x}.{version:08x}");
    spawn_blocking(move || db.put(key, value)).await??;
    Ok(())
}

async fn delete_versioned(
    db: Arc<DB>,
    key: &StorageKey,
    version: StateVersion,
    stripe_index: StripeIndex,
) -> anyhow::Result<()> {
    let key = format!("{stripe_index:04x}.{key:x}.{version:08x}.delete");
    spawn_blocking(move || db.put(key, b"")).await??;
    Ok(())
}

mod parse {
    use crate::parse::{Configs, Extract};

    use super::ShardedStorageConfig;

    impl Extract for ShardedStorageConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_node: configs.get("big.num-node")?,
                num_faulty_node: configs.get("big.num-faulty-node")?,
                num_active_copy: configs.get("big.num-active-copy")?,
                num_stripe: configs.get("big.num-stripe")?,
                bypass_vote: configs.get("big.bypass-vote")?,
            })
        }
    }
}
