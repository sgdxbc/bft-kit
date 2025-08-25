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

pub struct ShardedStorage {
    config: ShardedStorageConfig,
    replica_index: ReplicaIndex,
    node_indices: HashSet<NodeIndex>,

    version: StateVersion,
    key_versions: HashMap<Key, Vec<StateVersion>>,
    read_for: HashMap<(StateVersion, Key), HashMap<ReplicaIndex, StateVersion>>,
    fetching: HashSet<Key>,
    bump_writing: HashSet<String>,
    node_versions: Vec<StateVersion>,
    active_version: StateVersion,

    proceed_buffer:
        VecDeque<Proceed<ShardedStorageSend, StorageStateOutput<ShardedStorageStatusMessage>>>,
}

pub struct ShardedStorageConfig {
    pub num_node: NodeIndex, // virtual "storage node"
    pub num_faulty_node: NodeIndex,
    pub num_active_copy: usize,
}

impl ShardedStorage {
    pub fn new(
        config: ShardedStorageConfig,
        replica_index: ReplicaIndex,
        node_indices: HashSet<NodeIndex>,
    ) -> Self {
        Self {
            replica_index,
            node_indices,
            version: 0,
            key_versions: Default::default(),
            read_for: Default::default(),
            fetching: Default::default(),
            bump_writing: Default::default(),
            node_versions: vec![0; config.num_node as _],
            active_version: 0,
            proceed_buffer: Default::default(),
            config,
        }
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

pub struct ShardedStorageStatusMessage {
    version: StateVersion,
    node_indices: HashSet<NodeIndex>,
}

impl ShardedStorageConfig {
    pub fn node_indices_of(&self, key: Key) -> Vec<NodeIndex> {
        // there should be some more efficient way to bypass rng. currently play for
        // safe as long as it is not too slow
        (0..self.num_node).choose_multiple(&mut StdRng::from_seed(key.0), self.num_active_copy)
    }

    fn should_store(&self, node_indices: &HashSet<NodeIndex>, key: Key) -> bool {
        self.node_indices_of(key)
            .into_iter()
            .any(|node_index| node_indices.contains(&node_index))
    }
}

impl StorageState for ShardedStorage {
    fn fetch(&mut self, key: Key) {
        let inserted = self.fetching.insert(key);
        assert!(inserted);

        if self.config.should_store(&self.node_indices, key) {
            self.read_key(self.replica_index, self.version, key)
        } else {
            let query = message::Query {
                version: self.version,
                key: key.0,
                replica_index: self.replica_index,
            };
            let dest = Dest::Multi(self.config.node_indices_of(key));
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
            if self.config.should_store(&self.node_indices, key) {
                self.key_versions.entry(key).or_default().push(self.version);
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

    type Gossip = ShardedStorageStatusMessage;
    fn remote_gossip(&mut self, gossip: Self::Gossip) {
        todo!()
    }
}

impl State for ShardedStorage {
    type Send = ShardedStorageSend;
    type Output = StorageStateOutput<ShardedStorageStatusMessage>;

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
                for node_index in archived.node_indices {
                    self.node_versions[node_index as usize] =
                        self.node_versions[node_index as usize].max(archived.version)
                }
                let mut node_versions = self.node_versions.clone();
                node_versions.sort_unstable();
                let active_version = node_versions[self.config.num_faulty_node as usize];
                if active_version > self.active_version {
                    // optimized with a binary heap if iterating all keys is too slow
                    for (key, versions) in &mut self.key_versions {
                        let active_index = match versions.binary_search(&active_version) {
                            Ok(index) => index + 1,
                            Err(index) => index,
                        };
                        for version in versions.drain(..active_index) {
                            // TODO output delete
                        }
                    }
                    self.active_version = active_version;

                    if self.active_version > self.version {
                        self.proceed_buffer.push_back(Proceed::Output(
                            StorageStateOutput::Skipped(self.active_version - self.version),
                        ));
                        self.version = self.active_version
                    }

                    self.gossip_status()
                }
            }
        }
    }
}

impl ShardedStorage {
    fn find_version(&self, key: Key, version: StateVersion) -> Option<StateVersion> {
        let shard_versions = self.key_versions.get(&key)?;
        match shard_versions.binary_search(&version) {
            Err(0) => None,
            Ok(index) => Some(shard_versions[index]),
            Err(index) => Some(shard_versions[index - 1]),
        }
    }

    fn read_key(&mut self, replica_index: ReplicaIndex, version: StateVersion, key: Key) {
        let Some(found_version) = self.find_version(key, version) else {
            let proceed = if replica_index == self.replica_index {
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
        // can we use entry api here?
        if let Some(&previous_version) = targets.get(&replica_index)
            && previous_version >= version
        {
        } else {
            targets.insert(replica_index, version);
        }
    }

    fn gossip_status(&mut self) {
        //
    }
}

pub mod message {
    use std::collections::HashSet;

    use bincode::{Decode, Encode};

    use super::{NodeIndex, ReplicaIndex, StateVersion};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Query {
        pub version: StateVersion,
        pub key: [u8; 32],
        pub replica_index: ReplicaIndex,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct QueryOk {
        pub version: StateVersion,
        pub key: [u8; 32],
        pub bytes: Option<Vec<u8>>, // `Bytes` does not support Encode/Decode
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
            })
        }
    }
}
