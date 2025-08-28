use std::{
    cmp::Ordering,
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    iter::once,
    mem::take,
    time::Duration,
};

use bincode::{Decode, Encode};
use primitive_types::H256;
use rand::{Rng, SeedableRng as _, rngs::StdRng, seq::IteratorRandom};
use reed_solomon_simd::ReedSolomonEncoder;
use tokio_util::bytes::Bytes;

use crate::{
    Never,
    replication::ReplicaIndex,
    service::{ServiceIndex, Store},
    state::{Action, State},
};

use super::BINCODE_CONFIG;

pub type StateVersion = u64;
pub type Key = H256;

pub trait StorageState:
    State<
        Effect = StorageStateEffect<Self::StorageSend>,
        Output = StorageStateOutput<Self::OrderedMessage>,
    >
{
    type StorageSend;

    fn fetch(&mut self, key: Key);
    fn bump(&mut self, writes: HashMap<Key, Bytes>);
    #[allow(unused_variables)]
    fn prefetch(&mut self, key: Key) {}

    fn get_complete(&mut self, key: String, value: Bytes);
    fn put_complete(&mut self, key: String);

    type OrderedMessage;
    fn receive_ordered(&mut self, message: Self::OrderedMessage);
}

pub enum StorageStateEffect<S> {
    Send(S),
    Store(Store),
}

pub enum StorageStateOutput<M> {
    Fetched(Key, Option<Bytes>),
    Bumped,
    Skipped(StateVersion), // number of versions to skip execute
    Order(M),
}

pub struct FullReplicationStorage {
    outputs: VecDeque<StorageStateOutput<Never>>,
    effects: Vec<StorageStateEffect<Never>>,
    keys: HashSet<Key>,
    writing_keys: HashSet<String>,
}

impl FullReplicationStorage {
    pub fn new() -> Self {
        Self {
            outputs: Default::default(),
            effects: Default::default(),
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
    type StorageSend = Never;

    fn fetch(&mut self, key: Key) {
        if !self.keys.contains(&key) {
            self.outputs
                .push_back(StorageStateOutput::Fetched(key, None));
            return;
        }
        self.effects
            .push(StorageStateEffect::Store(Store::Get(format!("{key:x}"))))
    }

    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        for (key, value) in writes {
            self.keys.insert(key);
            let key = format!("{key:x}");
            self.writing_keys.insert(key.clone());
            self.effects
                .push(StorageStateEffect::Store(Store::Put(key, value)))
        }
        if self.writing_keys.is_empty() {
            self.outputs.push_back(StorageStateOutput::Bumped)
        }
    }

    fn get_complete(&mut self, key: String, value: Bytes) {
        self.outputs.push_back(StorageStateOutput::Fetched(
            key.parse().unwrap(),
            Some(value),
        ))
    }

    fn put_complete(&mut self, key: String) {
        let removed = self.writing_keys.remove(&key);
        assert!(removed);
        if self.writing_keys.is_empty() {
            self.outputs.push_back(StorageStateOutput::Bumped)
        }
    }

    type OrderedMessage = Never;
    fn receive_ordered(&mut self, _gossip: Self::OrderedMessage) {
        unreachable!()
    }
}

impl State for FullReplicationStorage {
    type Effect = StorageStateEffect<<Self as StorageState>::StorageSend>;
    type Output = StorageStateOutput<<Self as StorageState>::OrderedMessage>;

    fn proceed(&mut self, _since_start: std::time::Duration) -> Action<Self::Effect, Self::Output> {
        if let Some(output) = self.outputs.pop_front() {
            return Action::Output(output);
        }
        match self.effects.pop() {
            Some(effect) => Action::Perform(effect),
            None => Action::Pending(None),
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
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

pub struct ShardedStorage {
    config: ShardedStorageConfig,
    node_indices: HashSet<NodeIndex>,
    // cache of config.groups_of_nodes(node_indices)
    groups: HashSet<ActiveGroupIndex>,
    // cache of union of config.archive_placements(n) for n in node_indices
    archive_placements: HashMap<StripeIndex, BTreeSet<StripeShardIndex>>,

    version: StateVersion,
    version_table: VersionTable,
    read_for: HashMap<String, HashMap<ReplicaIndex, StateVersion>>,
    querying: HashSet<Key>, // for tolerating multiple QueryOk
    bump_writing: HashSet<String>,
    reorder_queries: HashMap<StateVersion, Vec<message::Query>>,

    node_vote_archive_versions: Vec<StateVersion>,
    archiving_version: StateVersion,
    node_archived_versions: Vec<StateVersion>,
    quorum_archived_version: StateVersion, // cache for sorted(node_archived_versions)[f]

    archiving_stripe_index: StripeIndex, // == config.num_stripe if not archiving
    // the values of active groups whose primary nodes are local are directly
    // inserted, values of other groups are pushed by the corresponded primary nodes
    stripe_group_values: HashMap<ActiveGroupIndex, HashMap<[u8; 32], Vec<u8>>>,
    archive_reading: HashSet<String>, // in `{version}.{key:x}` format
    archive_writing: HashSet<String>, // in `{stripe_index}-{stripe_shard_index}` format
    reorder_archive_pushes: HashMap<StripeIndex, Vec<message::ArchivePush>>,

    actions: VecDeque<
        Action<StorageStateEffect<ShardedStorageSend>, StorageStateOutput<message::VoteArchive>>,
    >,
}

#[derive(Debug, Clone)]
pub struct ShardedStorageConfig {
    pub num_node: NodeIndex, // virtual "storage node"
    pub num_faulty_node: NodeIndex,
    pub num_stripe: StripeIndex,
    pub num_active_copy: usize,
    // pub repair_threshold: NodeIndex,
    pub bypass_vote: bool,
}

struct VersionTable {
    map: HashMap<StripeIndex, HashMap<Key, Vec<StateVersion>>>,
}

impl ShardedStorage {
    pub fn new(config: ShardedStorageConfig, node_indices: HashSet<NodeIndex>) -> Self {
        let mut archive_placements = HashMap::<_, BTreeSet<_>>::new();
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
            groups: config.groups_of_nodes(&node_indices).collect(),
            archive_placements,
            archiving_stripe_index: config.num_stripe, // not archiving
            version: 0,
            version_table: VersionTable::new(config.num_stripe),
            node_indices,
            read_for: Default::default(),
            querying: Default::default(),
            bump_writing: Default::default(),
            reorder_queries: Default::default(),
            node_vote_archive_versions: vec![0; config.num_node as _],
            archiving_version: 0,
            node_archived_versions: vec![0; config.num_node as _],
            quorum_archived_version: 0,
            stripe_group_values: Default::default(),
            archive_reading: Default::default(),
            archive_writing: Default::default(),
            reorder_archive_pushes: Default::default(),
            actions: Default::default(),
            config,
        }
    }
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

    fn group_of(&self, key: &Key) -> ActiveGroupIndex {
        StdRng::from_seed(key.0).random_range(0..self.num_active_group())
    }

    fn nodes_of(&self, key: &Key) -> impl Iterator<Item = NodeIndex> {
        self.nodes_of_group(self.group_of(key))
    }

    // archive tier
    fn repair_threshold(&self) -> NodeIndex {
        self.num_faulty_node + 1 // make this configurable only if needed (probably for performance)
    }

    fn stripe_width(&self) -> NodeIndex {
        self.repair_threshold() + self.num_faulty_node * 2
    }

    fn stripe_of(&self, key: &Key) -> StripeIndex {
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

impl VersionTable {
    fn new(num_stripe: StripeIndex) -> Self {
        Self {
            map: (0..num_stripe)
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

impl StorageState for ShardedStorage {
    type StorageSend = ShardedStorageSend;

    fn fetch(&mut self, key: Key) {
        if self.should_store(&key) {
            self.read_key(self.node_index(), self.version, key)
        } else {
            let inserted = self.querying.insert(key);
            assert!(inserted);
            let query = message::Query {
                version: self.version,
                key: key.0,
                node_index: self.node_index(),
            };
            let dest = Dest::Multi(self.config.nodes_of(&key).collect());
            self.actions
                .push_back(Action::Perform(StorageStateEffect::Send((
                    dest,
                    ShardedStorageMessage::Query(query),
                ))))
        }
    }

    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        tracing::trace!(?self.node_indices, %self.version, "bumping");

        if !self.querying.is_empty() {
            tracing::warn!(?self.node_indices, "bump with ongoing fetches");
            self.querying.clear()
        }

        self.version += 1;
        for (key, bytes) in writes {
            if self.should_store(&key) {
                self.version_table.add(key, self.version, &self.config);
                let key = format!("{}.{key:x}", self.version);
                self.bump_writing.insert(key.clone());
                self.actions
                    .push_back(Action::Perform(StorageStateEffect::Store(Store::Put(
                        key, bytes,
                    ))))
            }
        }
        self.may_bumped();
    }

    // by looking at the following two 2-part methods, the second thought is to have
    // a sub state machine dedicated for archiving
    // well, if major revision is unfortunately taking place, will do so

    fn get_complete(&mut self, key: String, value: Bytes) {
        let read_for = self.read_for.remove(&key);
        let archive_reading = self.archive_reading.remove(&key);
        let (_version, key) = key.split_once('.').unwrap();
        let key = key.parse().unwrap();

        if let Some(targets) = read_for {
            for (node_index, version) in targets {
                let action = if self.node_indices.contains(&node_index) {
                    assert_eq!(version, self.version); // or relax on this, just continue?
                    Action::Output(StorageStateOutput::Fetched(key, Some(value.clone())))
                } else {
                    let query_ok = message::QueryOk {
                        version,
                        key: key.0,
                        bytes: Some(value.to_vec()),
                    };
                    Action::Perform(StorageStateEffect::Send((
                        Dest::One(node_index),
                        ShardedStorageMessage::QueryOk(query_ok),
                    )))
                };
                self.actions.push_back(action)
            }
        }

        if archive_reading {
            let group_index = self.config.group_of(&key);
            assert!(self.groups.contains(&group_index));
            let stripe_index = self.config.stripe_of(&key);
            assert_eq!(stripe_index, self.archiving_stripe_index);
            self.stripe_group_values
                .get_mut(&group_index)
                .unwrap()
                .insert(key.into(), value.into());

            self.may_archive_push()
        }
    }

    fn put_complete(&mut self, key: String) {
        if self.bump_writing.remove(&key) {
            self.may_bumped()
        }
        if self.archive_writing.remove(&key) {
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
    type Effect = StorageStateEffect<<Self as StorageState>::StorageSend>;
    type Output = StorageStateOutput<<Self as StorageState>::OrderedMessage>;

    fn proceed(&mut self, _since_start: Duration) -> Action<Self::Effect, Self::Output> {
        if let Some(proceed) = self.actions.pop_front() {
            return proceed;
        }
        Action::Pending(None)
    }

    type Message = ShardedStorageMessage;
    fn receive(&mut self, message: Self::Message) {
        match message {
            ShardedStorageMessage::Query(fetch) => {
                if fetch.version < self.quorum_archived_version {
                    return;
                }
                if fetch.version > self.version {
                    self.reorder_queries
                        .entry(fetch.version)
                        .or_default()
                        .push(fetch);
                    return;
                }
                self.read_key(fetch.node_index, fetch.version, fetch.key.into())
            }
            ShardedStorageMessage::QueryOk(fetch_ok) => {
                let key = fetch_ok.key.into();
                if fetch_ok.version == self.version && self.querying.remove(&key) {
                    self.actions
                        .push_back(Action::Output(StorageStateOutput::Fetched(
                            key,
                            fetch_ok.bytes.map(Into::into),
                        )))
                }
            }
            ShardedStorageMessage::ArchivePush(archive_push) => {
                self.handle_archive_push(archive_push);
                // check for archive_reading is very nonlocal. any better flow?
                if self.is_archiving() && self.archive_reading.is_empty() {
                    self.may_finish_archive_stripe()
                }
            }
            ShardedStorageMessage::Archived(archived) => {
                let mut updated = false;
                for node_index in archived.node_indices {
                    if archived.version > self.node_archived_versions[node_index as usize] {
                        self.node_archived_versions[node_index as usize] = archived.version;
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
    fn node_index(&self) -> NodeIndex {
        self.node_indices.iter().copied().next().unwrap()
    }

    fn should_store(&self, key: &Key) -> bool {
        self.groups.contains(&self.config.group_of(key))
    }

    fn read_key(&mut self, node_index: NodeIndex, version: StateVersion, key: Key) {
        let Some(found_version) = self.version_table.find(&key, version, &self.config) else {
            let action = if self.node_indices.contains(&node_index) {
                Action::Output(StorageStateOutput::Fetched(key, None))
            } else {
                let query_ok = message::QueryOk {
                    version,
                    key: key.0,
                    bytes: None,
                };
                Action::Perform(StorageStateEffect::Send((
                    Dest::One(node_index),
                    ShardedStorageMessage::QueryOk(query_ok),
                )))
            };
            self.actions.push_back(action);
            return;
        };

        let key = format!("{found_version}.{key:x}");
        let targets = self.read_for.entry(key.clone()).or_default();
        if targets.is_empty() {
            self.actions
                .push_back(Action::Perform(StorageStateEffect::Store(Store::Get(key))))
        }

        let previous_version = targets.entry(node_index).or_default();
        *previous_version = (*previous_version).max(version)
    }

    fn may_bumped(&mut self) {
        if !self.bump_writing.is_empty() {
            return;
        }

        self.actions
            .push_back(Action::Output(StorageStateOutput::Bumped));

        if let Some(queries) = self.reorder_queries.remove(&self.version) {
            for query in queries {
                self.read_key(query.node_index, query.version, query.key.into())
            }
        }

        self.may_vote_archive()
    }

    fn is_archiving(&mut self) -> bool {
        self.archiving_stripe_index != self.config.num_stripe
    }

    fn may_vote_archive(&mut self) {
        if self.is_archiving() {
            return;
        }
        // if we cannot serve a new version for archiving
        if !self.config.bypass_vote && self.version == self.archiving_version
        // if unsynced, other nodes may have archived on some lower (but >= quorum 
        // archived) version while we were archiving on archiving_version. so assist
        // their archiving with same pushes again
        // remark: if a node has nothing to do during an archiving, i.e., it is not
        // among the placement of any stripe, this may become an endless `proceed` loop:
        // the node vote for `self.version`, bypass the voting and start to archive,
        // immediately finish the archive and go back here again. currently just don't
        // use the triggering configuration combinations: either set num_node to 3f + 1
        // or set num_stripe to >= num_node
            || self.version == self.quorum_archived_version
        {
            return;
        }

        for &node_index in &self.node_indices {
            self.node_vote_archive_versions[node_index as usize] = self.version
        }

        if self.config.bypass_vote {
            self.enter_archiving(self.version);
            return;
        }

        let vote_archive = message::VoteArchive {
            version: self.version,
            node_indices: self.node_indices.clone(),
        };
        self.actions
            .push_back(Action::Output(StorageStateOutput::Order(vote_archive)));
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

        self.enter_archiving(quorum_vote_archive_version)
    }

    fn enter_archiving(&mut self, version: StateVersion) {
        tracing::info!(?self.node_indices, %version, "enter archiving");
        if self.archiving_stripe_index != self.config.num_stripe {
            tracing::warn!(
                ?self.node_indices, %version, %self.archiving_version, %self.archiving_stripe_index,
                "enter archiving without finishing previous round");
            self.stripe_group_values.clear();
            self.archive_reading.clear();
            self.archive_writing.clear()
        }
        self.archiving_version = version;
        self.archiving_stripe_index = 0;

        self.prepare_archive_stripe()
    }

    fn prepare_archive_stripe(&mut self) {
        tracing::debug!(
            ?self.node_indices, %self.archiving_version, %self.archiving_stripe_index,
            "archiving stripe");
        assert!(self.stripe_group_values.is_empty());
        for &group_index in &self.groups {
            self.stripe_group_values
                .insert(group_index, Default::default());
        }
        for (key, version) in self.version_table.snapshot(
            self.archiving_stripe_index,
            self.archiving_version,
            &self.config,
        ) {
            let key = format!("{version}.{key:x}");
            self.archive_reading.insert(key.clone());
            self.actions
                .push_back(Action::Perform(StorageStateEffect::Store(Store::Get(key))))
        }
        self.may_archive_push()
    }

    fn may_archive_push(&mut self) {
        if !self.archive_reading.is_empty() {
            return;
        }

        tracing::debug!(
            ?self.node_indices, %self.archiving_version, %self.archiving_stripe_index,
            "archive push");
        for group_index in self.config.is_primary_of(&self.node_indices) {
            let archive_push = message::ArchivePush {
                version: self.archiving_version,
                stripe_index: self.archiving_stripe_index,
                group_index,
                values: self.stripe_group_values[&group_index].clone(),
            };
            let push_node_indices = &self
                .config
                .archive_nodes_of_stripe(self.archiving_stripe_index)
                .collect::<HashSet<_>>()
                - &self
                    .config
                    .nodes_of_group(group_index)
                    .collect::<HashSet<_>>();
            self.actions
                .push_back(Action::Perform(StorageStateEffect::Send((
                    Dest::Multi(push_node_indices.into_iter().collect()),
                    ShardedStorageMessage::ArchivePush(archive_push),
                ))))
        }

        if let Some(pushes) = self
            .reorder_archive_pushes
            .remove(&self.archiving_stripe_index)
        {
            for push in pushes {
                self.handle_archive_push(push)
            }
        }
        self.may_finish_archive_stripe()
    }

    fn handle_archive_push(&mut self, archive_push: message::ArchivePush) {
        if !self.config.bypass_vote {
            match (archive_push.version, archive_push.stripe_index)
                .cmp(&(self.archiving_version, self.archiving_stripe_index))
            {
                Ordering::Less => (),
                Ordering::Greater => self
                    .reorder_archive_pushes
                    .entry(archive_push.stripe_index)
                    .or_default()
                    .push(archive_push),
                Ordering::Equal => {
                    self.insert_archive_push(archive_push.group_index, archive_push.values)
                }
            }
        } else if !self.is_archiving()
            || self
                .stripe_group_values
                .contains_key(&archive_push.group_index)
        {
            self.reorder_archive_pushes
                .entry(archive_push.stripe_index)
                .or_default()
                .push(archive_push)
        } else {
            self.insert_archive_push(archive_push.group_index, archive_push.values)
        }
    }

    // can inline actually
    fn insert_archive_push(
        &mut self,
        group_index: ActiveGroupIndex,
        values: HashMap<[u8; 32], Vec<u8>>,
    ) {
        let replaced = self.stripe_group_values.insert(group_index, values);
        if replaced.is_some() {
            tracing::warn!(
                ?self.node_indices, %self.archiving_version, %self.archiving_stripe_index, %group_index,
                "duplicate insertion of ArchivePush")
        }
    }

    fn may_finish_archive_stripe(&mut self) {
        assert!(self.is_archiving());
        if self
            .archive_placements
            .contains_key(&self.archiving_stripe_index)
            && (self.stripe_group_values.len() as ActiveGroupIndex) < self.config.num_active_group()
        {
            return;
        }

        tracing::debug!(
            ?self.node_indices, %self.archiving_version, %self.archiving_stripe_index,
            "stripe archived");
        if let Some(shard_indices) = self.archive_placements.get(&self.archiving_stripe_index) {
            let mut stripe =
                bincode::encode_to_vec(take(&mut self.stripe_group_values), BINCODE_CONFIG)
                    .unwrap();
            assert!(!stripe.is_empty());
            // make sure that stripe_size / repair_threshold is even; required by RS encoder
            let stripe_size = stripe
                .len()
                .next_multiple_of((self.config.repair_threshold() * 2) as _);
            stripe.resize(stripe_size, 0);
            let shard_size = stripe_size / self.config.repair_threshold() as usize;
            let shards = stripe.chunks_exact(shard_size).collect::<Vec<_>>();

            let mut shard_indices = shard_indices.clone();
            let parity_indices = shard_indices.split_off(&(self.config.repair_threshold() as _));
            for shard_index in shard_indices {
                let key = format!("{}-{shard_index}", self.archiving_stripe_index);
                self.archive_writing.insert(key.clone());
                self.actions
                    .push_back(Action::Perform(StorageStateEffect::Store(Store::Put(
                        key,
                        Bytes::copy_from_slice(shards[shard_index]),
                    ))))
            }
            if !parity_indices.is_empty() {
                let mut encoder = ReedSolomonEncoder::new(
                    self.config.repair_threshold() as _,
                    (self.config.stripe_width() - self.config.repair_threshold()) as _,
                    shard_size,
                )
                .unwrap();
                for shard in shards {
                    encoder.add_original_shard(shard).unwrap()
                }
                let parity_shards = encoder.encode().unwrap();
                for parity_index in parity_indices {
                    let parity_shard = parity_shards
                        .recovery(parity_index - self.config.repair_threshold() as usize)
                        .unwrap();
                    let key = format!("{}-{parity_index}", self.archiving_stripe_index);
                    self.archive_writing.insert(key.clone());
                    self.actions
                        .push_back(Action::Perform(StorageStateEffect::Store(Store::Put(
                            key,
                            Bytes::copy_from_slice(parity_shard),
                        ))))
                }
            }
        } else {
            // archive placement does not assign shards to local nodes
            self.stripe_group_values.clear()
        }

        self.archiving_stripe_index += 1;
        if self.archiving_stripe_index == self.config.num_stripe {
            self.may_finish_archive()
        } else {
            self.prepare_archive_stripe()
        }
    }

    fn may_finish_archive(&mut self) {
        if !self.archive_writing.is_empty() {
            return;
        }

        tracing::info!(?self.node_indices, %self.archiving_version, "archived");
        let archived = message::Archived {
            version: self.archiving_version,
            node_indices: self.node_indices.clone(),
        };
        self.actions
            .push_back(Action::Perform(StorageStateEffect::Send((
                Dest::All,
                ShardedStorageMessage::Archived(archived),
            ))));
        for &node_index in &self.node_indices {
            self.node_archived_versions[node_index as usize] = self.archiving_version
        }
        self.may_collect();
        self.may_vote_archive()
    }

    fn may_collect(&mut self) {
        let mut node_archived_versions = self.node_archived_versions.clone();
        node_archived_versions.sort_unstable();
        let quorum_archived_version = node_archived_versions[self.config.num_faulty_node as usize];
        if quorum_archived_version <= self.quorum_archived_version {
            return;
        }

        tracing::info!(?self.node_indices, %quorum_archived_version, "collect");
        for (key, version) in self.version_table.collect(quorum_archived_version) {
            let key = format!("{version}.{key:x}");
            self.actions
                .push_back(Action::Perform(StorageStateEffect::Store(Store::Delete(
                    key,
                ))))
        }
        self.quorum_archived_version = quorum_archived_version;

        if self.quorum_archived_version > self.version {
            self.actions
                .push_back(Action::Output(StorageStateOutput::Skipped(
                    self.quorum_archived_version - self.version,
                )));
            self.version = self.quorum_archived_version
        }
    }
}

pub mod message {
    use std::collections::{HashMap, HashSet};

    use bincode::{Decode, Encode};

    use super::{ActiveGroupIndex, NodeIndex, StateVersion, StripeIndex};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct VoteArchive {
        pub version: StateVersion,
        pub node_indices: HashSet<NodeIndex>,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Query {
        pub version: StateVersion,
        pub key: [u8; 32], // `Key` does not support Encode/Decode
        pub node_index: NodeIndex,
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
        pub group_index: ActiveGroupIndex,
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
