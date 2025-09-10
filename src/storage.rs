use std::{
    collections::{HashMap, HashSet},
    iter::once,
    sync::Arc,
};

use bincode::{Decode, Encode};
use rand::{Rng as _, SeedableRng as _, rngs::StdRng, seq::IteratorRandom as _};
use rocksdb::{DB, WriteBatch};
use tokio::{
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::{JoinHandle, spawn_blocking},
};
use tokio_util::bytes::Bytes;

use crate::{crypto::Digest, network::Dest, replica::ReplicaIndex, task::SegmentedTaskHandle};

pub mod full;
pub mod trie;

pub type StorageKey = Digest;
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

#[derive(Debug, Clone)]
pub struct ShardedStorageConfig {
    num_node: NodeIndex, // virtual "storage node"
    num_faulty_node: NodeIndex,
    num_stripe: StripeIndex,
    num_active_copy: usize,
    // pub repair_threshold: NodeIndex,
    bypass_vote: bool,
}

#[allow(unused)]
impl ShardedStorageConfig {
    // active tier
    fn num_active_group(&self) -> ActiveGroupIndex {
        self.num_node
    }

    fn primary_node_of_group(&self, index: ActiveGroupIndex) -> NodeIndex {
        index
    }

    fn as_primary_in_groups(
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
        (u64::from_le_bytes([
            key.0[0], key.0[1], key.0[2], key.0[3], key.0[4], key.0[5], key.0[6], key.0[7],
        ]) % self.num_stripe as u64) as _
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

pub struct Storage {
    db: Arc<DB>,
    network_dispatcher: NetworkDispatcher,
    config: ShardedStorageConfig,

    version: StateVersion,
    active_state: HashMap<StorageKey, Active>,
    node_versions: Vec<StateVersion>,
    quorum_bumped_version: StateVersion,

    rx_op: Receiver<StorageOp>,
    rx_message: Receiver<Message>,
}

struct Active {
    version: StateVersion,
    value: Option<Bytes>,
}

impl Storage {
    pub fn spawn(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>> + Send + 'static,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        node_table: Vec<ReplicaIndex>,
        rx_op: Receiver<StorageOp>,
        tx_message: Sender<(Dest, Message)>,
        rx_message: Receiver<Message>,
    ) -> JoinHandle<()> {
        spawn(task_handle.clone().wrap(Self::start(
            task_handle,
            db,
            config,
            node_indices,
            node_table,
            rx_op,
            tx_message,
            rx_message,
        )))
    }

    async fn start(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>>,
        config: ShardedStorageConfig,
        node_indices: HashSet<NodeIndex>,
        node_table: Vec<ReplicaIndex>,
        rx_op: Receiver<StorageOp>,
        tx_message: Sender<(Dest, Message)>,
        rx_message: Receiver<Message>,
    ) -> anyhow::Result<()> {
        let db = db.into();
        let network_dispatcher = NetworkDispatcher {
            tx_message,
            node_table,
        };

        let mut storage = Self {
            db: db.clone(),
            network_dispatcher,
            config: config.clone(),

            version: 0,
            active_state: Default::default(),
            node_versions: vec![0; config.num_node as usize],
            quorum_bumped_version: 0,

            rx_op,
            rx_message,
        };
        let storage = spawn(task_handle.clone().wrap(async move { storage.run().await }));

        storage.await?;
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event {
                Op(StorageOp),
                Message(Message),
            }
            match select! {
                Some(op) = self.rx_op.recv() => Event::Op(op),
                Some(msg) = self.rx_message.recv() => Event::Message(msg),
                else => break,
            } {
                Event::Op(op) => self.handle_op(op).await?,
                Event::Message(message) => self.handle_message(message).await?,
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
                let value = db_get(self.db.clone(), &key, &self.config).await?;
                let _ = tx_value.send(StorageRes::Ok(value));
            }
            StorageOp::Bump(bump, tx_ok) => {
                self.version += 1;
                for (key, value) in bump.inserts {
                    db_put(self.db.clone(), &key, value, &self.config).await?
                }
                for key in bump.deletes {
                    db_delete(self.db.clone(), &key, &self.config).await?
                }
                let _ = tx_ok.send(StorageRes::Ok(()));
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: Message) -> anyhow::Result<()> {
        //
        Ok(())
    }
}

// key spec
// {stripe index:04x}/{key:x}.{version:08x}(+delete)
// {stripe index:04x}:{version:08x}-{shard index:08x}

async fn db_get(
    db: Arc<DB>,
    key: &StorageKey,
    config: &ShardedStorageConfig,
) -> anyhow::Result<Option<Bytes>> {
    let db_key = format!(
        "/{:04x}/{:04x}/{}",
        config.group_of(key),
        config.stripe_of(key),
        key.to_hex()
    );
    let value = spawn_blocking(move || db.get(db_key)).await??;
    Ok(value.map(Into::into))
}

async fn db_put(
    db: Arc<DB>,
    key: &StorageKey,
    value: Bytes,
    config: &ShardedStorageConfig,
) -> anyhow::Result<()> {
    let db_key = format!(
        "/{:04x}/{:04x}/{}",
        config.group_of(key),
        config.stripe_of(key),
        key.to_hex()
    );
    spawn_blocking(move || db.put(db_key, value)).await??;
    Ok(())
}

async fn db_delete(
    db: Arc<DB>,
    key: &StorageKey,
    config: &ShardedStorageConfig,
) -> anyhow::Result<()> {
    let db_key = format!(
        "/{:04x}/{:04x}/{}",
        config.group_of(key),
        config.stripe_of(key),
        key.to_hex()
    );
    spawn_blocking(move || db.delete(db_key)).await??;
    Ok(())
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum Message {
    ArchivePush(message::ArchivePush),
}

#[derive(Debug, Clone)]
struct NetworkDispatcher {
    tx_message: Sender<(Dest, Message)>,
    node_table: Vec<ReplicaIndex>,
}

impl NetworkDispatcher {
    async fn send_to_all(&self, message: Message) {
        let _ = self.tx_message.send((Dest::All, message)).await;
    }
}

pub mod message {
    use std::collections::{BTreeMap, HashSet};

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
        pub data: BTreeMap<[u8; 32], Vec<u8>>,
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

pub fn preload(
    db: &DB,
    mut items: impl Iterator<Item = anyhow::Result<(StorageKey, Bytes)>>,
    storage_config: ShardedStorageConfig,
    node_indices: HashSet<NodeIndex>,
) -> anyhow::Result<()> {
    let _group_indices = storage_config
        .groups_of_nodes(&node_indices)
        .collect::<HashSet<_>>();
    loop {
        let mut batch = WriteBatch::new();
        for item in items.by_ref().take(10_000) {
            let (key, value) = item?;
            batch.put(
                format!(
                    "{:04x}/{:04x}/{}",
                    storage_config.stripe_of(&key),
                    0,
                    key.to_hex()
                ),
                value,
            )
        }
        if batch.is_empty() {
            break;
        }
        db.write(batch)?
    }
    Ok(())
}
