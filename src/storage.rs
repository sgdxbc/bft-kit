use std::{
    collections::{HashMap, HashSet},
    iter::once,
    sync::Arc,
};

use primitive_types::H256;
use rand::{Rng as _, SeedableRng as _, rngs::StdRng, seq::IteratorRandom as _};
use rocksdb::DB;
use tokio::{
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::{JoinHandle, spawn_blocking},
};
use tokio_util::bytes::Bytes;

use crate::task::TaskGroup;

pub type StorageKey = H256;
pub type NodeIndex = u16;
type StateVersion = u64;
type ActiveGroupIndex = NodeIndex;
type StripeIndex = u16;
type StripeShardIndex = usize;

pub enum StorageOp {
    Fetch(StorageKey, oneshot::Sender<StorageRes<Option<Bytes>>>),
    Bump(Bump, oneshot::Sender<StorageRes<()>>),
    // archive
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
    tx_epoch_change: Sender<(StateVersion, oneshot::Sender<()>)>,
    rx_entered: oneshot::Receiver<()>,
}

impl Storage {
    pub fn spawn(
        group: TaskGroup,
        db: impl Into<Arc<DB>>,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        rx_op: Receiver<StorageOp>,
    ) -> Vec<JoinHandle<()>> {
        let db = db.into();

        let (tx_epoch_change, rx_epoch_change) = channel(1);
        let (tx_entered, rx_entered) = oneshot::channel();
        let _ = tx_epoch_change.try_send((0, tx_entered));
        let (tx_archived, rx_archived) = channel(1);

        let mut storage = Self {
            db: db.clone(),
            config: config.clone(),
            version: 0,
            rx_op,
            tx_epoch_change,
            rx_entered,
        };
        let storage = spawn(
            group
                .clone()
                .wrap_fallible(async move { storage.run().await }),
        );

        let archive_worker = ArchiveWorker::spawn(
            group.clone(),
            config.clone(),
            node_indices,
            rx_epoch_change,
            tx_archived,
        );
        let collect_worker = CollectWorker::spawn(db, group, config, rx_archived);
        vec![storage, archive_worker, collect_worker]
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Op(StorageOp),
                EnteredEpoch,
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Ok(()) = &mut self.rx_entered => Event::EnteredEpoch,
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await?,
                Event::EnteredEpoch => self.handle_entered_epoch().await?,
            }
        }
        while let Some(op) = self.rx_op.recv().await {
            self.handle_op(op).await?
        }
        Ok(())
    }

    async fn handle_op(&mut self, op: StorageOp) -> anyhow::Result<()> {
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
        Ok(())
    }

    async fn handle_entered_epoch(&mut self) -> anyhow::Result<()> {
        if self.config.bypass_vote {
            let (tx_entered, rx_entered) = oneshot::channel();
            let _ = self.tx_epoch_change.send((self.version, tx_entered)).await;
            self.rx_entered = rx_entered;
            return Ok(());
        }

        todo!()
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

struct ArchiveWorker {
    config: ShardedStorageConfig,
    node_indices: HashSet<NodeIndex>,
    epoch: u64,

    rx_epoch_change: Receiver<(StateVersion, oneshot::Sender<()>)>,
    tx_archived: Sender<message::Archived>,
}

impl ArchiveWorker {
    fn spawn(
        group: TaskGroup,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        rx_epoch_change: Receiver<(StateVersion, oneshot::Sender<()>)>,
        tx_archived: Sender<message::Archived>,
    ) -> JoinHandle<()> {
        let mut worker = Self {
            config,
            node_indices,
            epoch: 0,
            rx_epoch_change,
            tx_archived,
        };
        spawn(group.wrap_fallible(async move { worker.run().await }))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some((state_version, tx_entered)) = self.rx_epoch_change.recv().await {
            self.epoch += 1;
            tracing::info!(
                "entered epoch {} at state version {state_version}",
                self.epoch
            );
            // TODO
            let _ = tx_entered.send(());

            let archived = message::Archived {
                version: state_version,
                node_indices: self.node_indices.clone(),
            };
            // TODO send to network
            let _ = self.tx_archived.send(archived).await;
        }
        Ok(())
    }
}

struct CollectWorker {
    db: Arc<DB>,
    config: ShardedStorageConfig,
    node_archived_versions: Vec<StateVersion>, // [node index -> version]
    quorum_archived_version: StateVersion,

    rx_archived: Receiver<message::Archived>,
}

impl CollectWorker {
    fn spawn(
        db: Arc<DB>,
        group: TaskGroup,
        config: ShardedStorageConfig,
        rx_archived: Receiver<message::Archived>,
    ) -> JoinHandle<()> {
        let mut worker = Self {
            db,
            node_archived_versions: vec![0; config.num_node as usize],
            config,
            quorum_archived_version: 0,
            rx_archived,
        };
        spawn(group.wrap_fallible(async move { worker.run().await }))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(archived) = self.rx_archived.recv().await {
            for node_index in archived.node_indices {
                self.node_archived_versions[node_index as usize] =
                    self.node_archived_versions[node_index as usize].max(archived.version)
            }
            let mut versions = self.node_archived_versions.clone();
            versions.sort_unstable();
            let quorum_archived_version = versions[self.config.num_faulty_node as usize];
            if quorum_archived_version > self.quorum_archived_version {
                self.quorum_archived_version = quorum_archived_version;

                tracing::info!(
                    "garbage collect up to version {}",
                    self.quorum_archived_version
                );
            }
        }
        Ok(())
    }
}

pub mod message {
    use std::collections::{HashMap, HashSet};

    use bincode::{Decode, Encode};

    use crate::replica::ReplicaIndex;

    use super::{ActiveGroupIndex, NodeIndex, StateVersion, StripeIndex};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct VoteArchive {
        pub epoch: u64,
        pub node_indices: HashSet<NodeIndex>,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Query {
        pub version: StateVersion,
        pub key: [u8; 32], // `StorageKey` does not support Encode/Decode
        pub replica_index: ReplicaIndex,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct QueryOk {
        pub version: StateVersion,
        pub key: [u8; 32],
        pub value: Option<Vec<u8>>, // `Bytes` does not support Encode/Decode
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct ArchivePush {
        pub epoch: u64,
        pub stripe_index: StripeIndex,
        pub group_index: ActiveGroupIndex,
        pub shard: HashMap<[u8; 32], Vec<u8>>,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Archived {
        pub version: StateVersion,
        pub node_indices: HashSet<NodeIndex>,
    }
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
