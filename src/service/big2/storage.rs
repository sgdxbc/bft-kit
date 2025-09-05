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
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::spawn_blocking,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken, task::TaskTracker};

use crate::{service::ServiceIndex, transport::BINCODE_CONFIG};

pub type StorageKey = H256;
pub type StateVersion = u64;

pub enum Invoke {
    Fetch(Vec<StorageKey>, oneshot::Sender<FetchResult>),
    Bump(Vec<(StorageKey, Vec<u8>)>, oneshot::Sender<()>),
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
            Invoke::Bump(updates, res_sender) => {
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

// rocksdb key convention
// `{stripe index:04x}.{key:x}.{version:08x}`

#[derive(Debug, Clone, Encode, Decode)]
pub enum ShardedStorageMessage {
    Query(message::Query),
    QueryOk(message::QueryOk),
    ArchivePush(message::ArchivePush), // bytes piggyback
    Archived(message::Archived),
}

pub struct ShardedLoop {
    service_index: ServiceIndex,
    node_table: Vec<ServiceIndex>, // [node index -> service index]
    connections: HashMap<ServiceIndex, Connection>,
    db: Arc<DB>,
    config: ShardedStorageConfig,

    group_indices: HashSet<ActiveGroupIndex>,
    version: StateVersion,
    fetching: Option<Fetching>,
    bumping: Option<oneshot::Sender<()>>,
    quorum_archived_version: StateVersion, // cached node_archived_versions[num_faulty]
    node_archived_versions: Vec<StateVersion>,

    pub event_sender: Sender<ShardedEvent>,
    event_receiver: Receiver<ShardedEvent>,

    tracker: TaskTracker,
    cancel: CancellationToken,
}

struct Fetching {
    keys: Vec<StorageKey>,
    values: HashMap<StorageKey, Option<Vec<u8>>>,
    res_sender: oneshot::Sender<FetchResult>,
}

pub enum ShardedEvent {
    Invoke(Invoke),
    Message(ShardedStorageMessage),
    OrderedMessage(message::VoteArchive),
    GetComplete(StorageKey, Option<Vec<u8>>),
    BumpComplete,
}

impl ShardedLoop {
    fn spawn(&self, fut: impl Future<Output = anyhow::Result<()>> + Send + 'static) {
        let cancel = self.cancel.clone();
        self.tracker.spawn(async move {
            if let Err(err) = fut.await {
                tracing::error!(%err);
                cancel.cancel()
            }
        });
    }

    fn spawn_read_loops(&self) {
        for connection in self.connections.values() {
            let connection = connection.clone();
            let event_sender = self.event_sender.clone();
            let tracker = self.tracker.clone();
            let cancel = self.cancel.clone();
            self.spawn(async move {
                loop {
                    let mut stream = connection.accept_uni().await?;

                    let event_sender = event_sender.clone();
                    let read = async move {
                        let bytes = Bytes::from(stream.read_to_end(64 << 20).await?);
                        let (message, len) = bincode::decode_from_slice::<ShardedStorageMessage, _>(
                            &bytes,
                            BINCODE_CONFIG,
                        )?;
                        anyhow::ensure!(len == bytes.len());
                        if event_sender
                            .send(ShardedEvent::Message(message))
                            .await
                            .is_err()
                        {
                            tracing::error!("event channel closed")
                        }
                        Ok(())
                    };

                    let cancel = cancel.clone();
                    tracker.spawn(async move {
                        if let Err(err) = read.await {
                            tracing::error!(%err);
                            cancel.cancel()
                        }
                    });
                }
                #[allow(unreachable_code)]
                anyhow::Ok(())
            })
        }
    }

    fn handle_invoke(&mut self, invoke: Invoke) -> anyhow::Result<()> {
        match invoke {
            Invoke::Fetch(keys, res_sender) => {
                assert!(self.fetching.is_none());
                for &key in &keys {
                    let group = self.config.group_of(&key);
                    if self.group_indices.contains(&group) {
                        let target_prefix = format!("{:04x}.{key:x}", self.config.stripe_of(&key));
                        let seek_key = format!("{target_prefix}.{:08x}", self.version);
                        let db = self.db.clone();
                        let event_sender = self.event_sender.clone();
                        self.spawn(async move {
                            let value =
                                spawn_blocking(move || get(target_prefix, seek_key, db)).await??;
                            if event_sender
                                .send(ShardedEvent::GetComplete(key, value))
                                .await
                                .is_err()
                            {
                                tracing::error!("event channel closed")
                            }
                            anyhow::Ok(())
                        })
                    } else {
                        let query = message::Query {
                            version: self.version,
                            key: key.0,
                            service_index: self.service_index,
                        };
                        let query = Bytes::from(bincode::encode_to_vec(
                            ShardedStorageMessage::Query(query),
                            BINCODE_CONFIG,
                        )?);
                        let service_indices = self
                            .config
                            .nodes_of(&key)
                            .map(|node_index| self.node_table[node_index as usize])
                            .collect::<HashSet<_>>();
                        for service_index in service_indices {
                            let connection = self.connections[&service_index].clone();
                            let query = query.clone();
                            self.spawn(async move {
                                connection.open_uni().await?.write_all(&query).await?;
                                anyhow::Ok(())
                            })
                        }
                    }
                }

                self.fetching = Some(Fetching {
                    keys,
                    values: HashMap::new(),
                    res_sender,
                })
            }
            Invoke::Bump(updates, res_sender) => {
                assert!(self.bumping.is_none());
                if let Some(_fetching) = self.fetching.take() {
                    tracing::warn!("bump while fetching in progress");
                    // implicitly close Fetch result channel
                }

                let mut batch = WriteBatch::new();
                for (key, value) in updates {
                    let db_key = format!(
                        "{:04x}.{key:x}.{:08x}",
                        self.config.stripe_of(&key),
                        self.version + 1
                    );
                    batch.put(db_key, value)
                }
                let db = self.db.clone();
                let event_sender = self.event_sender.clone();
                self.spawn(async move {
                    spawn_blocking(move || db.write(batch)).await??;
                    if event_sender.send(ShardedEvent::BumpComplete).await.is_err() {
                        tracing::error!("event channel closed")
                    }
                    anyhow::Ok(())
                });
                self.bumping = Some(res_sender)
            }
        }
        Ok(())
    }

    fn handle_message(&mut self, message: ShardedStorageMessage) -> anyhow::Result<()> {
        match message {
            ShardedStorageMessage::Query(query) => {
                if query.version > self.version {
                    // TODO
                    return Ok(());
                }
                if query.version < self.quorum_archived_version {
                    return Ok(());
                }

                let key = StorageKey::from(query.key);
                let target_prefix = format!("{:04x}.{key:x}", self.config.stripe_of(&key));
                let seek_key = format!("{target_prefix}.{:08x}", query.version);
                let db = self.db.clone();
                let connection = self.connections[&query.service_index].clone();
                self.spawn(async move {
                    let value = spawn_blocking(move || get(target_prefix, seek_key, db)).await??;
                    let query_ok = message::QueryOk {
                        version: query.version,
                        key: key.0,
                        value,
                    };
                    let bytes = bincode::encode_to_vec(&query_ok, BINCODE_CONFIG)?;
                    connection.open_uni().await?.write_all(&bytes).await?;
                    anyhow::Ok(())
                });
            }
            ShardedStorageMessage::QueryOk(query_ok) => {
                assert!(query_ok.version <= self.version);
                if query_ok.version < self.version {
                    return Ok(());
                }
                self.insert_fetched(StorageKey::from(query_ok.key), query_ok.value)
            }
            ShardedStorageMessage::ArchivePush(archive_push) => {
                // TODO
            }
            ShardedStorageMessage::Archived(archived) => {
                for node_index in archived.node_indices {
                    let saved_version = &mut self.node_archived_versions[node_index as usize];
                    *saved_version = (*saved_version).max(archived.version)
                }
                let mut versions = self.node_archived_versions.clone();
                versions.sort_unstable();
                if versions[self.config.num_faulty_node as usize] > self.quorum_archived_version {
                    self.quorum_archived_version = versions[self.config.num_faulty_node as usize];

                    if self.quorum_archived_version > self.version {
                        if let Some(fetching) = self.fetching.take() {
                            let send_err = fetching
                                .res_sender
                                .send(FetchResult::Skip(
                                    self.quorum_archived_version - self.version,
                                ))
                                .is_err();
                            if send_err {
                                tracing::error!("result channel closed");
                                self.cancel.cancel()
                            }
                        }
                        self.version = self.quorum_archived_version
                    }

                    let db = self.db.clone();
                    let quorum_archived_version = self.quorum_archived_version;
                    let collect = move || {
                        let mut iter = db.raw_iterator();
                        iter.seek_to_first();
                        iter.status()?;
                        let mut batch = WriteBatch::new();
                        while iter.valid() {
                            let key = iter.key().unwrap();
                            let key_str = std::str::from_utf8(key).unwrap();
                            let version_str = key_str.rsplit('.').next().unwrap();
                            let version: StateVersion = version_str.parse().unwrap();
                            if version < quorum_archived_version {
                                batch.delete(key)
                            }
                            iter.next();
                            iter.status()?
                        }
                        db.write(batch)?;
                        anyhow::Ok(())
                    };
                    let cancel = self.cancel.clone();
                    self.tracker.spawn_blocking(move || {
                        if let Err(err) = collect() {
                            tracing::error!(%err);
                            cancel.cancel()
                        }
                    });
                }
            }
        }
        Ok(())
    }

    fn insert_fetched(&mut self, key: StorageKey, value: Option<Vec<u8>>) {
        if let Some(fetching) = &mut self.fetching
            && fetching.keys.contains(&key)
        {
            fetching.values.insert(key, value);
            if fetching.values.len() == fetching.keys.len() {
                let mut fetching = self.fetching.take().unwrap();
                let values = fetching
                    .keys
                    .into_iter()
                    .map(|key| fetching.values.remove(&key).unwrap())
                    .collect();
                if fetching
                    .res_sender
                    .send(FetchResult::Values(values))
                    .is_err()
                {
                    tracing::error!("result channel closed");
                    self.cancel.cancel()
                }
            }
        }
    }

    fn handle_get_complete(&mut self, key: StorageKey, value: Option<Vec<u8>>) {
        self.insert_fetched(key, value)
    }

    fn handle_bump_complete(&mut self) {
        let res_sender = self.bumping.take().unwrap();
        if res_sender.send(()).is_err() {
            tracing::error!("result channel closed");
            self.cancel.cancel()
        }
        self.version += 1
    }

    pub async fn run<L>(mut self, submit_sender: Sender<L>) -> anyhow::Result<()>
    where
        message::VoteArchive: Into<L>,
    {
        self.spawn_read_loops();

        while let Some(event) = self
            .cancel
            .run_until_cancelled(self.event_receiver.recv())
            .await
            .flatten()
        {
            match event {
                ShardedEvent::Message(message) => self.handle_message(message)?,
                ShardedEvent::GetComplete(key, value) => self.handle_get_complete(key, value),
                ShardedEvent::BumpComplete => self.handle_bump_complete(),
                ShardedEvent::Invoke(invoke) => self.handle_invoke(invoke)?,
                ShardedEvent::OrderedMessage(vote_archive) => todo!(),
            }
        }

        drop(self.event_receiver);

        self.tracker.close();
        self.tracker.wait().await;
        Ok(())
    }
}

fn get(target_prefix: String, seek_key: String, db: Arc<DB>) -> anyhow::Result<Option<Vec<u8>>> {
    let mut iter = db.raw_iterator();
    iter.seek_for_prev(seek_key);
    iter.status()?;
    let value = iter
        .item()
        .filter(|(key, _)| key.starts_with(target_prefix.as_bytes()))
        .map(|(_, value)| value.to_vec());
    Ok(value)
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
