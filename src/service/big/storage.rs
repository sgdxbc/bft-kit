use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::replace,
    time::Duration,
};

use bincode::{Decode, Encode};
use primitive_types::H256;
use rand::{SeedableRng as _, rngs::StdRng, seq::IteratorRandom};
use tokio_util::bytes::Bytes;

use crate::{
    Never,
    replication::ReplicaIndex,
    service::ServiceIndex,
    state::{Proceed, State},
};

pub type StateVersion = u64;
pub type Key = H256;

pub trait StorageState: State<Output = StorageStateOutput<Self::Gossip>> {
    fn fetch(&mut self, key: Key);
    fn bump(&mut self, writes: HashMap<Key, Bytes>);
    #[allow(unused_variables)]
    fn will_fetch(&mut self, key: Key, version_ahead: StateVersion) {}

    fn read_ok(&mut self, key: String, value: Bytes);
    fn write_ok(&mut self, key: String);

    type Gossip;
    fn remote_gossip(&mut self, gossip: Self::Gossip);
}

pub enum StorageStateOutput<G> {
    Fetched(Key, Option<Bytes>),
    Bumped,
    Skipped(StateVersion), // number of versions to skip execute

    Read(String),
    Write(String, Bytes),

    Gossip(G),
}

pub struct FullReplicationStorage {
    output_buffer: VecDeque<StorageStateOutput<Never>>,
    keys: HashSet<Key>,
    writing_keys: HashSet<String>,
}

impl FullReplicationStorage {
    pub fn new() -> Self {
        Self {
            output_buffer: Default::default(),
            keys: Default::default(),
            writing_keys: Default::default(),
        }
    }
}

impl Default for FullReplicationStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageState for FullReplicationStorage {
    fn fetch(&mut self, key: Key) {
        if !self.keys.contains(&key) {
            self.output_buffer
                .push_back(StorageStateOutput::Fetched(key, None));
            return;
        }
        self.output_buffer
            .push_back(StorageStateOutput::Read(format!("{key:x}")))
    }

    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        for (key, value) in writes {
            self.keys.insert(key);
            let key = format!("{key:x}");
            self.writing_keys.insert(key.clone());
            self.output_buffer
                .push_back(StorageStateOutput::Write(key, value))
        }
        if self.writing_keys.is_empty() {
            self.output_buffer.push_back(StorageStateOutput::Bumped)
        }
    }

    fn read_ok(&mut self, key: String, value: Bytes) {
        self.output_buffer.push_back(StorageStateOutput::Fetched(
            key.parse().unwrap(),
            Some(value),
        ))
    }

    fn write_ok(&mut self, key: String) {
        let removed = self.writing_keys.remove(&key);
        assert!(removed);
        if self.writing_keys.is_empty() {
            self.output_buffer.push_back(StorageStateOutput::Bumped)
        }
    }

    type Gossip = Never;
    fn remote_gossip(&mut self, _gossip: Self::Gossip) {
        unreachable!()
    }
}

impl State for FullReplicationStorage {
    type Send = Never;
    type Output = StorageStateOutput<Never>;

    fn proceed(&mut self, _since_start: std::time::Duration) -> Proceed<Self::Send, Self::Output> {
        match self.output_buffer.pop_front() {
            Some(output) => Proceed::Output(output),
            None => Proceed::Pending(None),
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}

type NodeIndex = ServiceIndex;
type ShardIndex = u32;
type StripeIndex = u32;

pub struct ShardedStorage {
    config: ShardedStorageConfig,
    replica_index: ReplicaIndex,
    node_indices: HashSet<NodeIndex>,

    version: StateVersion,
    version_table: VersionTable,
    read_for: HashMap<(StateVersion, Key), HashMap<ReplicaIndex, StateVersion>>,
    fetching: HashSet<Key>,
    bump_writing: HashSet<String>,

    vote_archive_version: StateVersion,
    node_vote_archive_versions: Vec<StateVersion>,
    archiving_version: StateVersion,
    node_archived_versions: Vec<StateVersion>,
    quorum_archived_version: StateVersion, // cache for sorted(node_archived_versions)[f]

    proceed_buffer: VecDeque<Proceed<ShardedStorageSend, StorageStateOutput<message::VoteArchive>>>,
}

pub struct ShardedStorageConfig {
    pub num_node: NodeIndex, // virtual "storage node"
    pub num_faulty_node: NodeIndex,
    pub num_active_copy: usize,

    pub num_stripe: StripeIndex,
    pub num_shard_per_stripe: ShardIndex,

    pub bypass_vote: bool,
}

struct VersionTable {
    map: HashMap<ShardIndex, HashMap<Key, Vec<StateVersion>>>,
}

impl ShardedStorage {
    pub fn new(
        config: ShardedStorageConfig,
        replica_index: ReplicaIndex,
        node_indices: HashSet<NodeIndex>,
    ) -> Self {
        Self {
            replica_index,
            version: 0,
            version_table: VersionTable::new(config.shards_of_nodes(&node_indices)),
            node_indices,
            read_for: Default::default(),
            fetching: Default::default(),
            bump_writing: Default::default(),
            vote_archive_version: 0,
            node_vote_archive_versions: vec![0; config.num_node as _],
            archiving_version: 0,
            node_archived_versions: vec![0; config.num_node as _],
            quorum_archived_version: 0,
            proceed_buffer: Default::default(),
            config,
        }
    }
}

impl VersionTable {
    fn new(shard_indices: impl Iterator<Item = ShardIndex>) -> Self {
        Self {
            map: shard_indices
                .map(|index| (index, Default::default()))
                .collect(),
        }
    }

    fn add(&mut self, key: Key, version: StateVersion, config: &ShardedStorageConfig) {
        let versions = self
            .map
            .entry(config.shard_of(&key))
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
        key: &Key,
        version: StateVersion,
        config: &ShardedStorageConfig,
    ) -> Option<StateVersion> {
        let versions = self.map[&config.shard_of(key)].get(key)?;
        match versions.binary_search(&version) {
            Err(0) => None,
            Ok(index) => Some(versions[index]),
            Err(index) => Some(versions[index - 1]),
        }
    }

    fn find_shard(
        &self,
        index: ShardIndex,
        version: StateVersion,
        config: &ShardedStorageConfig,
    ) -> impl Iterator<Item = (&Key, StateVersion)> {
        self.map[&index].keys().filter_map(move |key| {
            self.find(key, version, config)
                .map(|version| (key, version))
        })
    }

    fn collect(&mut self, version: StateVersion) -> impl Iterator<Item = (&Key, StateVersion)> {
        // optimized with a binary heap if iterating all keys is too slow
        self.map.values_mut().flat_map(move |shard| {
            shard.iter_mut().flat_map(move |(key, versions)| {
                // versions[active_index] is the highest version that is <= version
                // this is the earliest version that should _not_ be collected
                // remarks that this version may be lower than `version`, but is not collected
                // because it is needed to serve a later `find` call with `version` as argument
                // (which should not return None)

                // doing so implies that for each key there is a version (i.e. the `version`
                // passed into this method) that is both archived and not collected by the
                // active tier (i.e. actively served)
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
    Archived(message::Archived),
}

// should we just add a Multi variant to replication::Dest?
pub enum Dest {
    One(ReplicaIndex),
    Multi(Vec<ReplicaIndex>),
    All,
}

pub type ShardedStorageSend = (Dest, ShardedStorageMessage);

impl ShardedStorageConfig {
    fn num_shard(&self) -> ShardIndex {
        self.num_stripe * self.num_shard_per_stripe
    }

    fn shard_of(&self, key: &Key) -> ShardIndex {
        // assuming the keys are uniformly distributed, by hashing
        (key.to_low_u64_le() % self.num_shard() as u64) as _
    }

    fn stripe_of_shard(&self, index: ShardIndex) -> StripeIndex {
        index / self.num_shard_per_stripe
    }

    // active tier
    pub fn nodes_of_shard(&self, index: ShardIndex) -> Vec<NodeIndex> {
        (0..self.num_node)
            .choose_multiple(&mut StdRng::seed_from_u64(index as _), self.num_active_copy)
    }

    fn designated_node_of_shard(&self, index: ShardIndex) -> NodeIndex {
        self.nodes_of_shard(index)[0]
    }

    fn nodes_of(&self, key: &Key) -> Vec<NodeIndex> {
        self.nodes_of_shard(self.shard_of(key))
    }

    fn shards_of_nodes(
        &self,
        node_indices: &HashSet<NodeIndex>,
    ) -> impl Iterator<Item = ShardIndex> {
        (0..self.num_shard()).filter(|&index| {
            self.nodes_of_shard(index)
                .into_iter()
                .any(|node_index| node_indices.contains(&node_index))
        })
    }

    // archive tier
    fn archive_node_of_shard(&self, index: ShardIndex) -> NodeIndex {
        (index % self.num_node as ShardIndex) as _
    }

    fn encoding_nodes_of_stripe(&self, index: StripeIndex) -> impl Iterator<Item = NodeIndex> {
        ((index + 1) * self.num_shard_per_stripe..)
            .take((self.num_faulty_node * 2) as _)
            .map(|index| (index % self.num_node as ShardIndex) as _)
    }
}

impl StorageState for ShardedStorage {
    fn fetch(&mut self, key: Key) {
        let inserted = self.fetching.insert(key);
        assert!(inserted);

        if self.should_store(&key) {
            self.read_key(self.replica_index, self.version, key)
        } else {
            let query = message::Query {
                version: self.version,
                key: key.0,
                replica_index: self.replica_index,
            };
            let dest = Dest::Multi(self.config.nodes_of(&key));
            self.proceed_buffer
                .push_back(Proceed::Send((dest, ShardedStorageMessage::Query(query))))
        }
    }

    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        // tracing::trace!(%self.replica_index, %self.version, "bumping");

        if !self.fetching.is_empty() {
            tracing::warn!(%self.replica_index, "bump with ongoing fetches");
            self.fetching.clear()
        }

        self.version += 1;
        for (key, bytes) in writes {
            if self.should_store(&key) {
                self.version_table.add(key, self.version, &self.config);
                let key = format!("{key:x}.{}", self.version);
                self.bump_writing.insert(key.clone());
                self.proceed_buffer
                    .push_back(Proceed::Output(StorageStateOutput::Write(key, bytes)))
            }
        }
        if self.bump_writing.is_empty() {
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Bumped))
        }
        if self.should_vote_archive() {
            self.vote_archive()
        }
    }

    fn read_ok(&mut self, key: String, value: Bytes) {
        let (key, version) = key.split_once('.').unwrap();
        let version = version.parse::<StateVersion>().unwrap();
        let key = key.parse().unwrap();

        if let Some(targets) = self.read_for.remove(&(version, key)) {
            for (replica_index, version) in targets {
                let proceed = if replica_index == self.replica_index {
                    assert_eq!(version, self.version); // or relax on this, just continue
                    let exists = self.fetching.remove(&key);
                    assert!(exists);
                    Proceed::Output(StorageStateOutput::Fetched(key, Some(value.clone())))
                } else {
                    let query_ok = message::QueryOk {
                        version,
                        key: key.0,
                        bytes: Some(value.to_vec()),
                    };
                    Proceed::Send((
                        Dest::One(replica_index),
                        ShardedStorageMessage::QueryOk(query_ok),
                    ))
                };
                self.proceed_buffer.push_back(proceed)
            }
        }
    }

    fn write_ok(&mut self, key: String) {
        let removed = self.bump_writing.remove(&key);
        assert!(removed);
        if self.bump_writing.is_empty() {
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Bumped))
        }
    }

    type Gossip = message::VoteArchive;
    fn remote_gossip(&mut self, vote_archive: Self::Gossip) {
        let mut updated = false;
        for node_index in vote_archive.node_indices {
            if self.node_vote_archive_versions[node_index as usize] < vote_archive.version {
                self.node_vote_archive_versions[node_index as usize] = vote_archive.version;
                updated = true
            }
        }
        if updated {
            self.may_enter_archiving()
        }
    }
}

impl State for ShardedStorage {
    type Send = ShardedStorageSend;
    type Output = StorageStateOutput<message::VoteArchive>;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(proceed) = self.proceed_buffer.pop_front() {
            return proceed;
        }
        Proceed::Pending(None)
    }

    type Message = ShardedStorageMessage;
    fn receive(&mut self, message: Self::Message) {
        match message {
            ShardedStorageMessage::Query(fetch) => {
                if fetch.version < self.quorum_archived_version {
                    return;
                }
                self.read_key(fetch.replica_index, fetch.version, fetch.key.into())
            }
            ShardedStorageMessage::QueryOk(fetch_ok) => {
                let key = fetch_ok.key.into();
                if fetch_ok.version == self.version && self.fetching.remove(&key) {
                    self.proceed_buffer
                        .push_back(Proceed::Output(StorageStateOutput::Fetched(
                            key,
                            fetch_ok.bytes.map(Into::into),
                        )))
                }
            }
            ShardedStorageMessage::Archived(archived) => {
                let mut updated = false;
                for node_index in archived.node_indices {
                    if archived.version > self.node_vote_archive_versions[node_index as usize] {
                        self.node_vote_archive_versions[node_index as usize] = archived.version;
                        updated = true
                    }
                }
                if !updated {
                    return;
                }
                self.may_collect();
            }
        }
    }
}

impl ShardedStorage {
    fn should_store(&self, key: &Key) -> bool {
        self.version_table
            .map
            .contains_key(&self.config.shard_of(key))
    }

    fn is_archiving(&self) -> bool {
        self.archiving_version > self.node_archived_versions[self.replica_index as usize]
    }

    fn should_vote_archive(&self) -> bool {
        self.version > self.vote_archive_version && !self.is_archiving()
    }

    fn read_key(&mut self, replica_index: ReplicaIndex, version: StateVersion, key: Key) {
        let Some(found_version) = self.version_table.find(&key, version, &self.config) else {
            let proceed = if replica_index == self.replica_index {
                let removed = self.fetching.remove(&key);
                assert!(removed);
                Proceed::Output(StorageStateOutput::Fetched(key, None))
            } else {
                let query_ok = message::QueryOk {
                    version,
                    key: key.0,
                    bytes: None,
                };
                Proceed::Send((
                    Dest::One(replica_index),
                    ShardedStorageMessage::QueryOk(query_ok),
                ))
            };
            self.proceed_buffer.push_back(proceed);
            return;
        };

        let targets = self.read_for.entry((found_version, key)).or_default();
        if targets.is_empty() {
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Read(format!(
                    "{key:x}.{found_version}"
                ))))
        }

        let previous_version = targets.entry(replica_index).or_default();
        *previous_version = (*previous_version).max(version)
    }

    fn vote_archive(&mut self) {
        if self.config.bypass_vote {
            self.archive(self.version);
            return;
        }
        let vote_archive = message::VoteArchive {
            version: self.version,
            node_indices: self.node_indices.clone(),
        };
        self.proceed_buffer
            .push_back(Proceed::Output(StorageStateOutput::Gossip(vote_archive)));
        self.node_vote_archive_versions[self.replica_index as usize] = self.version;
        self.may_enter_archiving()
    }

    fn may_enter_archiving(&mut self) {
        let mut node_vote_archive_versions = self.node_vote_archive_versions.clone();
        node_vote_archive_versions.sort_unstable();
        let quorum_vote_archive_version =
            node_vote_archive_versions[self.config.num_faulty_node as usize];
        if quorum_vote_archive_version <= self.archiving_version {
            return;
        }

        self.archive(quorum_vote_archive_version)
    }

    fn archive(&mut self, version: StateVersion) {
        self.archiving_version = version
    }

    fn may_collect(&mut self) {
        let mut node_archived_versions = self.node_archived_versions.clone();
        node_archived_versions.sort_unstable();
        let quorum_archived_version = node_archived_versions[self.config.num_faulty_node as usize];
        if quorum_archived_version <= self.quorum_archived_version {
            return;
        }

        for (key, version) in self.version_table.collect(quorum_archived_version) {
            // TODO
        }
        self.quorum_archived_version = quorum_archived_version;

        if self.quorum_archived_version > self.version {
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Skipped(
                    self.quorum_archived_version - self.version,
                )));
            self.version = self.quorum_archived_version
        }
    }
}

pub mod message {
    use std::collections::{HashMap, HashSet};

    use bincode::{Decode, Encode};

    use super::{NodeIndex, ReplicaIndex, ShardIndex, StateVersion};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct VoteArchive {
        pub version: StateVersion,
        pub node_indices: HashSet<NodeIndex>,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Query {
        pub version: StateVersion,
        pub key: [u8; 32], // `Key` does not support Encode/Decode
        pub replica_index: ReplicaIndex,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct QueryOk {
        pub version: StateVersion,
        pub key: [u8; 32],
        pub bytes: Option<Vec<u8>>, // `Bytes` does not support Encode/Decode
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct ArchivePush {
        pub version: StateVersion,
        pub shard_index: ShardIndex,
        pub values: HashMap<[u8; 32], Vec<u8>>,
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
                num_shard_per_stripe: configs.get("big.num-shard-per-stripe")?,
                bypass_vote: configs.get("big.bypass-vote")?,
            })
        }
    }
}
