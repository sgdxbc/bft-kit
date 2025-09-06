use std::{
    collections::{HashMap, HashSet},
    iter::once,
    sync::Arc,
};

use bincode::{Decode, Encode};
use primitive_types::H256;
use quinn::Connection;
use rand::{Rng as _, SeedableRng as _, rngs::StdRng, seq::IteratorRandom};
use rocksdb::{DB, WriteBatch};
use tokio::{
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::{JoinSet, spawn_blocking},
};
use tokio_util::{bytes::Bytes, sync::CancellationToken, task::TaskTracker};

use crate::{service::ServiceIndex, transport::BINCODE_CONFIG};

pub type StorageKey = H256;
pub type StateVersion = u64;

pub enum Invoke {
    Fetch(Vec<StorageKey>, oneshot::Sender<FetchResult>),
    Bump(
        Vec<(StorageKey, Vec<u8>)>,
        Vec<StorageKey>,
        oneshot::Sender<()>,
    ),
}

pub enum FetchResult {
    Values(Vec<Option<Vec<u8>>>),
    Skip(StateVersion),
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
            Invoke::Fetch(keys, res_sender) => {
                let db = db.clone();
                spawn_blocking(move || {
                    let values = db.multi_get(keys).into_iter().collect::<Result<_, _>>()?;
                    if res_sender.send(FetchResult::Values(values)).is_err() {
                        tracing::error!("result channel closed")
                    }
                    anyhow::Ok(())
                })
                .await??
            }
            Invoke::Bump(updates, deletes, res_sender) => {
                let db = db.clone();
                spawn_blocking(move || {
                    let mut batch = WriteBatch::new();
                    for (key, value) in updates {
                        batch.put(key, value)
                    }
                    for key in deletes {
                        batch.delete(key)
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

type NodeIndex = ServiceIndex;
// each active group consist of one primary node and a certain number of
// secondary nodes that mask faulty primary nodes and prevent fall back to
// recover from archive tier
// the number of active groups is set to be equal to the number of nodes, and
// every node is the primary node of one active group, balancing the load for
// responding queries and pushing. so group index range is the same as node
// index
type ActiveGroupIndex = NodeIndex;
// a stripe is a unit of recovery. each stripe is split into `repair_threshold`
// shards and the shards are stored along with `2 * num_faulty_nodes` parity
// shards
// because the nodes must load the entire stripe into memory while archiving it,
// archiving must work on one stripe at a time, and the stripe size must not
// exceed the available memory on every node. this poses a requirement on the
// minimum number of stripes. unlike the number of active groups, the number of
// stripes is not related to the number of nodes and can go as high as desired,
// but keeping it low helps improve archiving latency, which can reduce the
// storage consumption of hosting active tier
// stripes divide the key space independently to the division of active groups
// so during archiving a stripe, the values collected for that stripe evenly
// distributes across the active groups and are collected from their respective
// primary nodes (in happy path). thus, the network overhead of serving values
// for archiving is balanced across all nodes
type StripeIndex = u16;
type StripeShardIndex = usize;

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

#[derive(Debug, Clone, Encode, Decode)]
pub enum ShardedStorageMessage {
    Query(message::Query),
    QueryOk(message::QueryOk),
    ArchivePush(message::ArchivePush), // bytes piggyback
    Archived(message::Archived),
}

// rocksdb key convention
// `+{stripe index:04x}.{key:x}.{version:08x}`
// `-{stripe index:04x}.{shard index:08x}.{version:08x}`

#[derive(Debug, Clone)]
struct DbHandle {
    db: Arc<DB>,
    config: Arc<ShardedStorageConfig>,
}

type Part = HashMap<[u8; 32], Vec<u8>>;

impl DbHandle {
    async fn get(
        &self,
        key: &StorageKey,
        version: StateVersion,
    ) -> anyhow::Result<Option<Vec<u8>>> {
        let target_prefix = format!("+{:04x}.{key:x}", self.config.stripe_of(key));
        let seek_key = format!("{target_prefix}.{version:08x}");
        let db = self.db.clone();
        let value = spawn_blocking(move || {
            let mut iter = db.raw_iterator();
            iter.seek_for_prev(seek_key);
            iter.status()?;
            let value = iter
                .item()
                .filter(|(key, _)| key.starts_with(target_prefix.as_bytes()))
                .map(|(_, value)| value.to_vec());
            anyhow::Ok(value)
        })
        .await??;
        Ok(value)
    }

    async fn put(
        &self,
        items: Vec<(StorageKey, Vec<u8>)>,
        version: StateVersion,
    ) -> anyhow::Result<()> {
        let db = self.db.clone();
        let mut batch = WriteBatch::default();
        for (key, value) in items {
            let db_key = format!("+{:04x}.{key:x}.{version:08x}", self.config.stripe_of(&key));
            batch.put(db_key, value);
        }
        spawn_blocking(move || db.write(batch)).await??;
        Ok(())
    }

    async fn get_stripe_parts(
        &self,
        stripe_index: StripeIndex,
        version: StateVersion,
    ) -> anyhow::Result<HashMap<ActiveGroupIndex, Part>> {
        let target_prefix = format!("+{stripe_index:04x}.");
        let seek_key = format!("{target_prefix}{:x}.{version:08x}", H256::zero());
        let db = self.db.clone();
        let config = self.config.clone();
        let part_map = spawn_blocking(move || {
            let mut iter = db.raw_iterator();
            iter.seek(&seek_key);
            iter.status()?;
            let mut part_map = HashMap::<_, Part>::new();
            while let Some((key, value)) = iter.item()
                && key.starts_with(target_prefix.as_bytes())
            {
                let parts = std::str::from_utf8(key)?.split('.').collect::<Vec<_>>();
                anyhow::ensure!(parts.len() == 3, "invalid db key format");
                let key = parts[1].parse::<H256>()?;
                part_map
                    .entry(config.group_of(&key))
                    .or_default()
                    .insert(key.0, value.to_vec());
                iter.seek(format!(
                    "{}.{}.{:08x}",
                    parts[0],
                    parts[1],
                    StateVersion::MAX
                ));
                iter.status()?;
            }
            Ok(part_map)
        })
        .await??;
        Ok(part_map)
    }

    async fn collect(&self, archived_version: StateVersion) -> anyhow::Result<()> {
        let db = self.db.clone();
        spawn_blocking(move || {
            let mut batch = WriteBatch::new();
            let mut iter = db.raw_iterator();
            iter.seek_to_first();
            iter.status()?;
            while let Some((key, _)) = iter.item() {
                let parts = std::str::from_utf8(key)?.split('.').collect::<Vec<_>>();
                anyhow::ensure!(parts.len() == 3, "invalid db key format");
                let version = parts[2].parse::<StateVersion>()?;
                if version < archived_version {
                    batch.delete(key);
                    iter.next()
                } else {
                    iter.seek(format!(
                        "{}.{}.{:08x}",
                        parts[0],
                        parts[1],
                        StateVersion::MAX
                    ))
                }
                iter.status()?
            }
            db.write(batch)?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }
}

struct Network {
    connections: HashMap<ServiceIndex, Connection>,
    node_table: Vec<ServiceIndex>,
    service_index: ServiceIndex,
    config: Arc<ShardedStorageConfig>,
    tracker: TaskTracker,
    cancel: CancellationToken,
}

impl Network {
    fn spawn(&self, fut: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        let cancel = self.cancel.clone();
        self.tracker.spawn(async move {
            if let Err(err) = fut.await {
                tracing::error!(%err);
                cancel.cancel()
            }
        });
    }

    fn query(&self, key: StorageKey, version: StateVersion) {
        let message = message::Query {
            version,
            key: key.into(),
            service_index: self.service_index,
        };
        let message = Bytes::from(
            bincode::encode_to_vec(&ShardedStorageMessage::Query(message), BINCODE_CONFIG).unwrap(),
        );
        let service_indices = self
            .config
            .nodes_of(&key)
            .map(|node_index| self.node_table[node_index as usize])
            .collect::<HashSet<_>>();
        for service_index in service_indices {
            let connection = self.connections[&service_index].clone();
            let query = message.clone();
            self.spawn(async move {
                connection.open_uni().await?.write_all(&query).await?;
                Ok(())
            })
        }
    }

    fn send_to_service(&self, message: ShardedStorageMessage, service_index: ServiceIndex) {
        let message = Bytes::from(bincode::encode_to_vec(&message, BINCODE_CONFIG).unwrap());
        let connection = self.connections[&service_index].clone();
        self.spawn(async move {
            connection.open_uni().await?.write_all(&message).await?;
            Ok(())
        })
    }

    fn send_to_all(&self, message: ShardedStorageMessage) {
        let message = Bytes::from(bincode::encode_to_vec(&message, BINCODE_CONFIG).unwrap());
        for connection in self.connections.values() {
            let connection = connection.clone();
            let message = message.clone();
            self.spawn(async move {
                connection.open_uni().await?.write_all(&message).await?;
                Ok(())
            })
        }
    }
}

struct InvokeManager {
    db: DbHandle,
    network: Arc<Network>,
    config: Arc<ShardedStorageConfig>,
    group_indices: HashSet<ActiveGroupIndex>,

    event_receiver: Receiver<InvokeManagerEvent>,
    invoke: Option<Invoke>,
    version: StateVersion,
    loaded: HashMap<StorageKey, Option<Vec<u8>>>,
    querying_keys: HashSet<StorageKey>,
}

enum InvokeManagerEvent {
    Invoke(Invoke, StateVersion),
    QueryOk(message::QueryOk),
}

impl InvokeManager {
    async fn run(mut self, event_sender: Sender<()>) -> anyhow::Result<()> {
        while let Some(event) = self.event_receiver.recv().await {
            match event {
                InvokeManagerEvent::Invoke(invoke, version) => {
                    if let Some(prev_invoke) = self.invoke.take() {
                        assert!(!matches!(prev_invoke, Invoke::Bump(..)));
                        self.querying_keys.clear()
                    }
                    let mut get_tasks = JoinSet::new();
                    match &invoke {
                        Invoke::Fetch(keys, _) => {
                            for &key in keys {
                                self.load(version, &mut get_tasks, key)
                            }
                        }
                        Invoke::Bump(updates, deletes, res_sender) => {
                            for &(key, _) in updates {
                                self.load(version, &mut get_tasks, key)
                            }
                            for &key in deletes {
                                self.load(version, &mut get_tasks, key)
                            }
                        }
                    }
                    while let Some(result) = get_tasks.join_next().await {
                        let (key, value) = result??;
                        self.loaded.insert(key, value);
                    }
                    self.invoke = Some(invoke);
                    self.version = version;
                    //
                }
                InvokeManagerEvent::QueryOk(query_ok) => {
                    let Some(invoke) = &self.invoke else {
                        continue;
                    };
                    if query_ok.version != self.version {
                        continue;
                    }
                    if self.querying_keys.remove(&query_ok.key.into()) {
                        self.loaded.insert(query_ok.key.into(), query_ok.value);
                    }
                }
            }
        }
        Ok(())
    }

    fn load(
        &mut self,
        version: u64,
        get_tasks: &mut JoinSet<Result<(H256, Option<Vec<u8>>), anyhow::Error>>,
        key: H256,
    ) {
        let group_index = self.config.group_of(&key);
        if self.group_indices.contains(&group_index) {
            let db = self.db.clone();
            get_tasks.spawn(async move {
                let value = db.get(&key, version).await?;
                anyhow::Ok((key, value))
            });
        } else {
            self.querying_keys.insert(key);
            self.network.query(key, version)
        }
    }
}

pub mod message {
    use std::collections::{HashMap, HashSet};

    use bincode::{Decode, Encode};

    use crate::service::ServiceIndex;

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
        pub service_index: ServiceIndex,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn print_active_placement() {
        let config = ShardedStorageConfig {
            num_node: 1000,
            num_faulty_node: 3,
            num_active_copy: 7,
            num_stripe: 1000,
            bypass_vote: true,
        };
        println!("{:?}", config.nodes_of_group(0).collect::<Vec<_>>());
        println!("{:?}", config.nodes_of_group(1).collect::<Vec<_>>());
        println!("{:?}", config.nodes_of_group(2).collect::<Vec<_>>());
        println!("{:?}", config.nodes_of_group(3).collect::<Vec<_>>());

        let mut node_overheads = [0; 10];
        for index in 0..config.num_active_group() {
            for node_index in config.nodes_of_group(index) {
                if let Some(count) = node_overheads.get_mut(node_index as usize) {
                    *count += 1
                }
            }
        }
        for (node_index, num_key) in node_overheads.into_iter().enumerate() {
            println!("Node {node_index} has {num_key} shards")
        }
    }
}
