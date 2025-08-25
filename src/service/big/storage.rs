use std::{
    collections::{HashMap, HashSet, VecDeque},
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

use super::BINCODE_CONFIG;

pub type StateVersion = u64;
pub type Key = H256;

pub trait StorageState: State<Output = StorageStateOutput<Self::OrderedMessage>> {
    fn fetch(&mut self, key: Key);
    fn bump(&mut self, writes: HashMap<Key, Bytes>);
    #[allow(unused_variables)]
    fn will_fetch(&mut self, key: Key, version_ahead: StateVersion) {}

    fn read_ok(&mut self, key: String, value: Bytes);
    fn write_ok(&mut self, key: String);

    type OrderedMessage;
    fn receive_ordered(&mut self, gossip: Self::OrderedMessage);
}

pub enum StorageStateOutput<G> {
    Fetched(Key, Option<Bytes>),
    Bumped,
    Skipped(StateVersion), // number of versions to skip execute

    Read(String),
    Write(String, Bytes),

    OrderedSend(G),
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

    type OrderedMessage = Never;
    fn receive_ordered(&mut self, _gossip: Self::OrderedMessage) {
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
type StripeIndex = u32;

pub struct ShardedStorage {
    config: ShardedStorageConfig,
    replica_index: ReplicaIndex,
    node_indices: HashSet<NodeIndex>,
    // cache of union of config.archive_placements(n) for n in node_indices
    archive_placements: HashMap<StripeIndex, HashSet<NodeIndex>>,

    version: StateVersion,
    version_table: VersionTable,
    read_for: HashMap<String, HashMap<ReplicaIndex, StateVersion>>,
    fetching: HashSet<Key>,
    bump_writing: HashSet<String>,

    vote_archive_version: StateVersion,
    node_vote_archive_versions: Vec<StateVersion>,
    archiving_version: StateVersion,
    node_archived_versions: Vec<StateVersion>,
    quorum_archived_version: StateVersion, // cache for sorted(node_archived_versions)[f]
    read_for_archive_push: HashMap<StripeIndex, HashSet<Key>>,
    // outgoing pushes under construction; not all keys have been read
    preparing_archive_push: HashMap<StripeIndex, HashMap<[u8; 32], Vec<u8>>>,
    waiting_archive_push: HashSet<StripeIndex>,
    archive_writing: HashSet<String>,
    reorder_archive_pushes: HashMap<StateVersion, Vec<message::ArchivePush>>,

    proceed_buffer: VecDeque<Proceed<ShardedStorageSend, StorageStateOutput<message::VoteArchive>>>,
}

pub struct ShardedStorageConfig {
    pub num_node: NodeIndex, // virtual "storage node"
    pub num_faulty_node: NodeIndex,
    pub num_stripe: StripeIndex,
    pub num_active_copy: usize,
    pub repair_threshold: NodeIndex,

    pub bypass_vote: bool,
}

struct VersionTable {
    map: HashMap<StripeIndex, HashMap<Key, Vec<StateVersion>>>,
}

impl ShardedStorage {
    pub fn new(
        config: ShardedStorageConfig,
        replica_index: ReplicaIndex,
        node_indices: HashSet<NodeIndex>,
    ) -> Self {
        let mut archive_placements = HashMap::<_, HashSet<_>>::new();
        for node_index in &node_indices {
            for (stripe_index, offset) in config.archive_placements(*node_index) {
                let inserted = archive_placements
                    .entry(stripe_index)
                    .or_default()
                    .insert(offset);
                assert!(inserted)
            }
        }
        Self {
            replica_index,
            archive_placements,
            version: 0,
            version_table: VersionTable::new(config.stripes_of_nodes(&node_indices)),
            node_indices,
            read_for: Default::default(),
            fetching: Default::default(),
            bump_writing: Default::default(),
            vote_archive_version: 0,
            node_vote_archive_versions: vec![0; config.num_node as _],
            archiving_version: 0,
            node_archived_versions: vec![0; config.num_node as _],
            quorum_archived_version: 0,
            read_for_archive_push: Default::default(),
            preparing_archive_push: Default::default(),
            waiting_archive_push: Default::default(),
            archive_writing: Default::default(),
            reorder_archive_pushes: Default::default(),
            proceed_buffer: Default::default(),
            config,
        }
    }
}

impl VersionTable {
    fn new(stripe_indices: impl Iterator<Item = StripeIndex>) -> Self {
        Self {
            map: stripe_indices
                .map(|index| (index, Default::default()))
                .collect(),
        }
    }

    fn add(&mut self, key: Key, version: StateVersion, config: &ShardedStorageConfig) {
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
        key: &Key,
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
    ) -> impl Iterator<Item = (&Key, StateVersion)> {
        self.map[&stripe_index].keys().filter_map(move |key| {
            self.find(key, version, config)
                .map(|version| (key, version))
        })
    }

    fn collect(&mut self, version: StateVersion) -> impl Iterator<Item = (&Key, StateVersion)> {
        // optimized with a binary heap if iterating all keys is too slow
        self.map.values_mut().flat_map(move |stripe| {
            stripe.iter_mut().flat_map(move |(key, versions)| {
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
    ArchivePush(message::ArchivePush),
    Archived(message::Archived),
}

// should we just add a Multi variant to replication::Dest?
pub enum Dest {
    One(NodeIndex),
    Multi(Vec<NodeIndex>),
    All,
}

pub type ShardedStorageSend = (Dest, ShardedStorageMessage);

impl ShardedStorageConfig {
    fn stripe_of(&self, key: &Key) -> StripeIndex {
        // assuming the keys are uniformly distributed, by hashing
        (key.to_low_u64_le() % self.num_stripe as u64) as _
    }

    // active tier
    pub fn nodes_of_stripe(&self, index: StripeIndex) -> Vec<NodeIndex> {
        (0..self.num_node)
            .choose_multiple(&mut StdRng::seed_from_u64(index as _), self.num_active_copy)
    }

    fn designated_node_of_stripe(&self, index: StripeIndex) -> NodeIndex {
        self.nodes_of_stripe(index)[0]
    }

    fn nodes_of(&self, key: &Key) -> Vec<NodeIndex> {
        self.nodes_of_stripe(self.stripe_of(key))
    }

    fn stripes_of_nodes(
        &self,
        node_indices: &HashSet<NodeIndex>,
    ) -> impl Iterator<Item = StripeIndex> {
        (0..self.num_stripe).filter(|&index| {
            self.nodes_of_stripe(index)
                .into_iter()
                .any(|node_index| node_indices.contains(&node_index))
        })
    }

    // archive tier
    fn stripe_width(&self) -> NodeIndex {
        self.repair_threshold + self.num_faulty_node * 2
    }

    fn archive_nodes_of_stripe(&self, index: StripeIndex) -> impl Iterator<Item = NodeIndex> {
        (index * self.stripe_width() as StripeIndex..)
            .take(self.stripe_width() as _)
            .map(|index| (index % self.num_node as StripeIndex) as _)
    }

    fn archive_placements(
        &self,
        node_index: NodeIndex,
    ) -> impl Iterator<Item = (StripeIndex, NodeIndex)> {
        (node_index as StripeIndex..self.num_stripe * self.stripe_width() as StripeIndex)
            .step_by(self.num_node as _)
            .map(move |index| {
                (
                    index / self.stripe_width() as StripeIndex,
                    (index % self.stripe_width() as StripeIndex) as NodeIndex,
                )
            })
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
        let read_for = self.read_for.remove(&key);
        let (key, _version) = key.split_once('.').unwrap();
        let key = key.parse().unwrap();

        if let Some(targets) = read_for {
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

        let stripe_index = self.config.stripe_of(&key);
        // use Entry API makes this very messy, so avoiding
        if let Some(keys) = self.read_for_archive_push.get_mut(&stripe_index)
            && keys.remove(&key)
        {
            self.preparing_archive_push
                .get_mut(&stripe_index)
                .unwrap()
                .insert(key.into(), value.into());
            if keys.is_empty() {
                self.read_for_archive_push.remove(&stripe_index);
                let values = self.preparing_archive_push.remove(&stripe_index).unwrap();
                let mut dest = self
                    .config
                    .archive_nodes_of_stripe(stripe_index)
                    .collect::<Vec<_>>();
                if let Some(pos) = dest.iter().position(|&index| index == self.replica_index) {
                    dest.swap_remove(pos);
                    self.insert_archive_push(stripe_index, values.clone())
                }
                if !dest.is_empty() {
                    let archive_push = message::ArchivePush {
                        version: self.archiving_version,
                        stripe_index,
                        values,
                    };
                    self.proceed_buffer.push_back(Proceed::Send((
                        Dest::Multi(dest),
                        ShardedStorageMessage::ArchivePush(archive_push),
                    )))
                }

                self.may_finish_archive()
            }
        }
    }

    fn write_ok(&mut self, key: String) {
        if self.bump_writing.remove(&key) && self.bump_writing.is_empty() {
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Bumped))
        }
        if self.archive_writing.remove(&key) && self.archive_writing.is_empty() {
            self.may_finish_archive()
        }
    }

    type OrderedMessage = message::VoteArchive;
    fn receive_ordered(&mut self, vote_archive: Self::OrderedMessage) {
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
            ShardedStorageMessage::ArchivePush(archive_push) => {
                if archive_push.version < self.archiving_version {
                    return;
                }
                if archive_push.version > self.archiving_version {
                    self.reorder_archive_pushes
                        .entry(archive_push.version)
                        .or_default()
                        .push(archive_push);
                    return;
                }
                self.insert_archive_push(archive_push.stripe_index, archive_push.values)
            }
            ShardedStorageMessage::Archived(archived) => {
                let mut updated = false;
                for node_index in archived.node_indices {
                    if archived.version > self.node_vote_archive_versions[node_index as usize] {
                        self.node_vote_archive_versions[node_index as usize] = archived.version;
                        updated = true
                    }
                }
                if updated {
                    self.may_collect()
                }
            }
        }
    }
}

impl ShardedStorage {
    fn should_store(&self, key: &Key) -> bool {
        self.version_table
            .map
            .contains_key(&self.config.stripe_of(key))
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

        let key = format!("{key:x}.{found_version}");
        let targets = self.read_for.entry(key.clone()).or_default();
        if targets.is_empty() {
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Read(key)))
        }

        let previous_version = targets.entry(replica_index).or_default();
        *previous_version = (*previous_version).max(version)
    }

    fn vote_archive(&mut self) {
        if self.config.bypass_vote {
            self.prepare_archive(self.version);
            return;
        }
        let vote_archive = message::VoteArchive {
            version: self.version,
            node_indices: self.node_indices.clone(),
        };
        self.proceed_buffer
            .push_back(Proceed::Output(StorageStateOutput::OrderedSend(
                vote_archive,
            )));
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

        self.prepare_archive(quorum_vote_archive_version);

        if let Some(pushes) = self.reorder_archive_pushes.remove(&self.archiving_version) {
            for push in pushes {
                self.insert_archive_push(push.stripe_index, push.values)
            }
        }
    }

    fn prepare_archive(&mut self, version: StateVersion) {
        self.archiving_version = version;
        if !self.preparing_archive_push.is_empty() {
            tracing::warn!(%self.replica_index, "start archive while preparing previous pushes");
            self.preparing_archive_push.clear();
            self.read_for_archive_push.clear()
        }
        for &stripe_index in self.version_table.map.keys() {
            if self.replica_index != self.config.designated_node_of_stripe(stripe_index) {
                continue;
            }
            self.preparing_archive_push
                .insert(stripe_index, Default::default());
            let mut keys = HashSet::new();
            for (&key, version) in self
                .version_table
                .snapshot(stripe_index, version, &self.config)
            {
                keys.insert(key);
                self.proceed_buffer
                    .push_back(Proceed::Output(StorageStateOutput::Read(format!(
                        "{key:x}.{version}"
                    ))))
            }
            self.read_for_archive_push.insert(stripe_index, keys);
        }
        if !self.waiting_archive_push.is_empty() {
            tracing::warn!(%self.replica_index, "start archive while waiting for previous stripes")
        }
        self.waiting_archive_push = self.archive_placements.keys().cloned().collect();
        if !self.archive_writing.is_empty() {
            tracing::warn!(%self.replica_index, "start archive while writing for previous archive")
        }

        self.may_finish_archive()
    }

    fn insert_archive_push(
        &mut self,
        stripe_index: StripeIndex,
        values: HashMap<[u8; 32], Vec<u8>>,
    ) {
        if !self.waiting_archive_push.remove(&stripe_index) {
            return; // duplicated ArchivePush
        }
        let mut stripe_bytes = bincode::encode_to_vec(values, BINCODE_CONFIG).unwrap();
        let shard_size = stripe_bytes
            .len()
            .next_multiple_of(self.config.repair_threshold as _);
        stripe_bytes.resize(shard_size * self.config.repair_threshold as usize, 0);
        let shards = stripe_bytes.chunks_exact(shard_size).collect::<Vec<_>>();
        for &offset in &self.archive_placements[&stripe_index] {
            let shard = if offset < self.config.repair_threshold {
                Bytes::copy_from_slice(shards[offset as usize])
            } else {
                todo!()
            };
            let key = format!("archive.{}-{stripe_index}-{offset}", self.archiving_version);
            self.archive_writing.insert(key.clone());
            self.proceed_buffer
                .push_back(Proceed::Output(StorageStateOutput::Write(key, shard)))
        }
    }

    fn may_finish_archive(&mut self) {
        if self.preparing_archive_push.is_empty()
            && self.read_for_archive_push.is_empty()
            && self.waiting_archive_push.is_empty()
            && self.archive_writing.is_empty()
        {
            self.node_archived_versions[self.replica_index as usize] = self.archiving_version;
            self.proceed_buffer.push_back(Proceed::Send((
                Dest::All,
                ShardedStorageMessage::Archived(message::Archived {
                    version: self.archiving_version,
                    node_indices: self.node_indices.clone(),
                }),
            )));

            self.may_enter_archiving()
        }
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

    use super::{NodeIndex, ReplicaIndex, StateVersion, StripeIndex};

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
        pub stripe_index: StripeIndex,
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
                repair_threshold: configs.get("big.repair-threshold")?,
                bypass_vote: configs.get("big.bypass-vote")?,
            })
        }
    }
}
