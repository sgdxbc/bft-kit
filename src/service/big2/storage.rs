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
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::{JoinSet, spawn_blocking},
};
use tokio_util::{bytes::Bytes, task::TaskTracker};

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

    // fn nodes_of(&self, key: &StorageKey) -> impl Iterator<Item = NodeIndex> {
    //     self.nodes_of_group(self.group_of(key))
    // }

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

struct VersionTable {
    map: HashMap<StripeIndex, HashMap<StorageKey, Vec<StateVersion>>>,
}

impl VersionTable {
    fn new(num_stripe: StripeIndex) -> Self {
        Self {
            map: (0..num_stripe)
                .map(|index| (index, Default::default()))
                .collect(),
        }
    }

    fn add(&mut self, key: StorageKey, version: StateVersion, config: &ShardedStorageConfig) {
        let versions = self
            .map
            .entry(config.stripe_of(&key))
            .or_default()
            .entry(key)
            .or_default();
        if let Some(&last_version) = versions.last() {
            assert!(version > last_version)
        }
        versions.push(version)
    }

    fn find(
        &self,
        key: &StorageKey,
        version: StateVersion,
        config: &ShardedStorageConfig,
    ) -> Option<StateVersion> {
        let versions = self.map[&config.stripe_of(key)].get(key)?;
        match versions.binary_search(&version) {
            Err(0) => None,
            Ok(index) => Some(versions[index]),
            Err(index) => Some(versions[index - 1]),
        }
    }

    fn snapshot(
        &self,
        stripe_index: StripeIndex,
        version: StateVersion,
        config: &ShardedStorageConfig,
    ) -> impl Iterator<Item = (&StorageKey, StateVersion)> {
        self.map[&stripe_index].keys().filter_map(move |key| {
            self.find(key, version, config)
                .map(|version| (key, version))
        })
    }

    fn collect(
        &mut self,
        version: StateVersion,
    ) -> impl Iterator<Item = (&StorageKey, StateVersion)> {
        // optimized with a binary heap if iterating all keys is too slow
        self.map.values_mut().flat_map(move |stripe| {
            stripe.iter_mut().flat_map(move |(key, versions)| {
                // versions[active_index] is the highest version that is <= version
                // this is the earliest version that should _not_ be collected
                // remarks that this version may be lower than `version`, but is not collected
                // because it is needed to serve a later `find` call with `version` as argument
                // (which should not return None)

                // doing so implies that for each key there is a version (i.e. the `version`
                // passed into this method) that is both archived and active
                // this design ensures that the system _never_ need to read from archive tier
                // as long as active tier is not compromised (by `r` random (faulty) replicas),
                // even when the system is just started or reactivated after an idle period,
                // during what the last version is archived
                let active_index = match versions.binary_search(&version) {
                    Ok(index) => index,
                    Err(0) => 0, // version < v for all v in versions; nothing to collect
                    Err(index) => index - 1,
                };
                versions
                    .drain(..active_index)
                    .map(move |version| (key, version))
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

pub async fn sharded_loop(
    service_index: ServiceIndex,
    node_table: Vec<ServiceIndex>, // [node index -> service index]
    connections: HashMap<ServiceIndex, Connection>,
    db: impl Into<Arc<DB>>,
    config: ShardedStorageConfig,
    mut invoke_receiver: Receiver<Invoke>,
    order_sender: Sender<message::VoteArchive>,
    ordered_receive_receiver: Receiver<message::VoteArchive>,
) -> anyhow::Result<()> {
    let db = db.into();
    let node_indices = node_table
        .iter()
        .enumerate()
        .filter_map(|(node_index, &index)| {
            if index == service_index {
                Some(node_index as NodeIndex)
            } else {
                None
            }
        })
        .collect::<HashSet<_>>();
    let groups = config
        .groups_of_nodes(&node_indices)
        .collect::<HashSet<_>>();

    let (message_sender, mut message_receiver) = channel(100);
    let (archive_push_sender, archive_push_receiver) = channel(100);

    let mut connection_reads = JoinSet::new();
    for connection in connections.values() {
        let connection = connection.clone();
        let message_sender = message_sender.clone();
        let archive_push_sender = archive_push_sender.clone();
        connection_reads.spawn(async move {
            let read_tracker = TaskTracker::new();
            loop {
                let mut stream = match connection.accept_uni().await {
                    Ok(stream) => stream,
                    Err(err) => {
                        tracing::error!("accepting stream failed: {err}");
                        break;
                    }
                };

                let message_sender = message_sender.clone();
                let archive_push_sender = archive_push_sender.clone();
                let read = async move {
                    let bytes = Bytes::from(stream.read_to_end(64 << 20).await?);
                    let (message, len) = bincode::decode_from_slice::<ShardedStorageMessage, _>(
                        &bytes,
                        BINCODE_CONFIG,
                    )?;
                    if let ShardedStorageMessage::ArchivePush(archive_push) = message {
                        archive_push_sender
                            .send((archive_push, bytes.slice(bytes.len() - len..)))
                            .await
                            .map_err(|_| anyhow::format_err!("archive push channel closed"))?
                    } else {
                        anyhow::ensure!(len == bytes.len());
                        message_sender
                            .send(message)
                            .await
                            .map_err(|_| anyhow::format_err!("message channel closed"))?
                    }
                    Ok(())
                };
                read_tracker.spawn(async move {
                    if let Err(err) = read.await {
                        tracing::error!(%err)
                    }
                });
            }
            read_tracker.close();
            read_tracker.wait().await;
            anyhow::Ok(())
        });
    }

    let (get_keys_sender, get_keys_receiver) = channel(100);
    let (archived_sender, mut archived_receiver) = channel(100);
    let archive = spawn(archive_loop(
        node_indices.clone(),
        config.clone(),
        connections.clone(),
        db.clone(),
        get_keys_receiver,
        archive_push_receiver,
        archived_sender,
    ));

    let mut version = 0;
    let mut version_table = VersionTable::new(config.num_stripe);

    let mut voted_epoch = 0;
    let mut ready_for_archive = true;

    let mut nodes_archived_versions = vec![0; config.num_node as _];
    let mut quorum_archived_version = 0;

    let tracker = TaskTracker::new();
    let (mut fetch_queried_sender, mut fetch_queried_receiver) = channel(100);
    let (mut fetch_skip_sender, mut fetch_skip_receiver) = channel(100);

    loop {
        enum Event<I, M, V> {
            Invoke(I),
            Message(M),
            Archived(V),
        }
        match select! {
            invoke = invoke_receiver.recv() => Event::Invoke(invoke),
            Some(message) = message_receiver.recv() => Event::Message(message),
            Some(archived) = archived_receiver.recv() => Event::Archived(archived),
        } {
            Event::Invoke(None) => break,
            Event::Invoke(Some(Invoke::Fetch(keys, res_sender))) => {
                let mut get_res = HashMap::new();
                let mut querying_keys = HashSet::new();

                let mut get_keys = Vec::new();
                for &key in &keys {
                    let group = config.group_of(&key);
                    if groups.contains(&group) {
                        match version_table.find(&key, version, &config) {
                            Some(version) => get_keys.push(format!("{version}.{key:x}")),
                            None => {
                                get_res.insert(key, None);
                            }
                        }
                        continue;
                    }

                    querying_keys.insert(key);
                    let query = message::Query {
                        version,
                        key: key.0,
                        service_index,
                    };
                    let query = Bytes::from(bincode::encode_to_vec(
                        ShardedStorageMessage::Query(query),
                        BINCODE_CONFIG,
                    )?);
                    for connection in config
                        .nodes_of_group(group)
                        .map(|node_index| &connections[&node_table[node_index as usize]])
                    {
                        let connection = connection.clone();
                        let query = query.clone();
                        let query = async move {
                            connection.open_uni().await?.write_all(&query).await?;
                            anyhow::Ok(())
                        };
                        tracker.spawn(async move {
                            if let Err(err) = query.await {
                                tracing::error!(%err)
                            }
                        });
                    }
                }

                (fetch_queried_sender, fetch_queried_receiver) = channel(100);
                (fetch_skip_sender, fetch_skip_receiver) = channel(100);
                let fetch = async move {
                    let res = loop {
                        if querying_keys.is_empty() {
                            break FetchResult::Values(
                                keys.into_iter()
                                    .map(|key| get_res.remove(&key).unwrap())
                                    .collect(),
                            );
                        }
                        enum Event<F, S> {
                            Fetched(F),
                            Skip(S),
                        }
                        match select! {
                            Some(fetched) = fetch_queried_receiver.recv() => Event::Fetched(fetched),
                            Some(skipped) = fetch_skip_receiver.recv() => Event::Skip(skipped),
                            else => anyhow::bail!("get channel(s) closed")
                        } {
                            Event::Fetched((key, value)) => {
                                get_res.insert(key, value);
                                querying_keys.remove(&key);
                            }
                            Event::Skip(entered_version) => {
                                break FetchResult::Skip(entered_version - version);
                            }
                        }
                    };
                    res_sender
                        .send(res)
                        .map_err(|_| anyhow::format_err!("result channel closed"))?;
                    anyhow::Ok(())
                };
                tracker.spawn(async move {
                    if let Err(err) = fetch.await {
                        tracing::error!(%err)
                    }
                });
            }
            Event::Invoke(Some(Invoke::Bump(updates, res_sender))) => {
                version += 1;
                for &(key, _) in &updates {
                    version_table.add(key, version, &config);
                }
                let db = db.clone();
                let bump = async move {
                    let mut batch = WriteBatch::new();
                    for (key, value) in updates {
                        batch.put(format!("{version}.{key:x}"), value)
                    }
                    spawn_blocking(move || db.write(batch)).await??;
                    res_sender
                        .send(())
                        .map_err(|_| anyhow::format_err!("result channel closed"))?;
                    anyhow::Ok(())
                };
                tracker.spawn(async move {
                    if let Err(err) = bump.await {
                        tracing::error!(%err)
                    }
                });
            }

            Event::Message(ShardedStorageMessage::Query(query)) => {
                if query.version > version {
                    // TODO
                    continue;
                }
                if query.version < quorum_archived_version {
                    continue;
                }

                let key = StorageKey::from(query.key);
                let versioned_key = version_table
                    .find(&key, query.version, &config)
                    .map(|version| format!("{version}.{key:x}"));
                let db = db.clone();
                let connection = connections[&query.service_index].clone();
                let reply = async move {
                    let value = match versioned_key {
                        None => None,
                        Some(versioned_key) => {
                            spawn_blocking(move || db.get(versioned_key)).await??
                        }
                    };
                    let query_ok = message::QueryOk {
                        version: query.version,
                        key: key.0,
                        value,
                    };
                    let bytes = bincode::encode_to_vec(&query_ok, BINCODE_CONFIG)?;
                    connection.open_uni().await?.write_all(&bytes).await?;
                    anyhow::Ok(())
                };
                tracker.spawn(async move {
                    if let Err(err) = reply.await {
                        tracing::error!(%err)
                    }
                });
            }
            Event::Message(ShardedStorageMessage::QueryOk(query_ok)) => {
                assert!(query_ok.version <= version);
                if query_ok.version < version {
                    continue;
                }
                let _ = fetch_queried_sender
                    .send((StorageKey::from(query_ok.key), query_ok.value))
                    .await;
            }
            Event::Message(ShardedStorageMessage::ArchivePush(_)) => unreachable!(),
            Event::Message(ShardedStorageMessage::Archived(archived)) => {
                for node_index in archived.node_indices {
                    let saved_version = &mut nodes_archived_versions[node_index as usize];
                    *saved_version = (*saved_version).max(version)
                }
                let mut versions = nodes_archived_versions.clone();
                versions.sort_unstable();
                if versions[config.num_faulty_node as usize] > quorum_archived_version {
                    quorum_archived_version = versions[config.num_faulty_node as usize];

                    if quorum_archived_version > version {
                        version = quorum_archived_version;
                        let _ = fetch_skip_sender.send(version).await;
                    }

                    let collect_keys = version_table
                        .collect(quorum_archived_version)
                        .map(|(key, version)| format!("{version}.{key:x}"))
                        .collect::<Vec<_>>();
                    let db = db.clone();
                    let collect = async move {
                        let mut batch = WriteBatch::new();
                        for key in collect_keys {
                            batch.delete(key);
                        }
                        spawn_blocking(move || db.write(batch)).await??;
                        anyhow::Ok(())
                    };
                    tracker.spawn(async move {
                        if let Err(err) = collect.await {
                            tracing::error!(%err)
                        }
                    });
                }
            }

            Event::Archived(archived_version) => {
                // TODO send Archived
                assert!(version >= archived_version);
                if version > archived_version {
                    // TODO vote next epoch
                } else {
                    ready_for_archive = true;
                }
            }
        }
    }
    Ok(())
}

async fn archive_loop(
    node_indices: HashSet<NodeIndex>,
    config: ShardedStorageConfig,
    connections: HashMap<ServiceIndex, Connection>,
    db: Arc<DB>,
    mut sripe_snapshots_receiver: Receiver<Vec<Vec<(StorageKey, StateVersion)>>>,
    archive_push_receiver: Receiver<(message::ArchivePush, Bytes)>,
    archived_sender: Sender<StateVersion>,
) -> anyhow::Result<()> {
    let mut epoch = 0;
    while let Some(stripe_snapshots) = sripe_snapshots_receiver.recv().await {
        epoch += 1;
        for (stripe_index, snapshot) in stripe_snapshots.into_iter().enumerate() {
            let stripe_index = stripe_index as StripeIndex;
            let db = db.clone();
            let mut keys = Vec::new();
            let mut versioned_keys = Vec::new();
            for (key, version) in snapshot {
                keys.push(key);
                versioned_keys.push(format!("{version}.{key:x}"));
            }
            let values = spawn_blocking(move || db.multi_get(versioned_keys)).await?;
            let mut data = Vec::new();
            for (key, value) in keys.into_iter().zip(values.into_iter()) {
                let Some(value) = value? else {
                    anyhow::bail!("missing key in db")
                };
                data.push((key.0, value))
            }

            let data_bytes = Bytes::from(bincode::encode_to_vec(&data, BINCODE_CONFIG)?);
            let archive_push = message::ArchivePush {
                epoch,
                stripe_index,
                group_index: 0, // filled later
            };
        }
    }
    Ok(())
}

pub mod message {
    use std::collections::HashSet;

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
