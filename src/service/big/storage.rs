use std::{
    collections::{HashMap, HashSet},
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

type StateVersion = u64;
type Key = H256;

pub trait StorageState: State<Output = StorageStateOutput> {
    fn fetch(&mut self, key: Key);
    fn bump(&mut self, writes: HashMap<Key, Bytes>);
    #[allow(unused_variables)]
    fn will_fetch(&mut self, key: Key, version_ahead: StateVersion) {}

    fn read_ok(&mut self, key: String, value: Bytes);
    fn write_ok(&mut self, key: String);
}

pub enum StorageStateOutput {
    Fetched(Key, Option<Bytes>),
    Skipped(StateVersion), // number of versions to skip execute

    Read(String),
    Write(String, Bytes),
}

pub struct FullReplicationStorage {
    output_buffer: Vec<StorageStateOutput>,
    keys: HashSet<Key>,
}

impl FullReplicationStorage {
    pub fn new() -> Self {
        Self {
            output_buffer: Default::default(),
            keys: Default::default(),
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
                .push(StorageStateOutput::Fetched(key, None));
            return;
        }
        self.output_buffer
            .push(StorageStateOutput::Read(format!("{key:x}")))
    }
    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        for (key, value) in writes {
            self.keys.insert(key);
            self.output_buffer
                .push(StorageStateOutput::Write(format!("{key:x}"), value))
        }
    }
    fn read_ok(&mut self, key: String, value: Bytes) {
        self.output_buffer.push(StorageStateOutput::Fetched(
            key.parse().unwrap(),
            Some(value),
        ))
    }
    fn write_ok(&mut self, _key: String) {}
}

impl State for FullReplicationStorage {
    type Send = Never;
    type Output = StorageStateOutput;

    fn proceed(&mut self, _since_start: std::time::Duration) -> Proceed<Self::Send, Self::Output> {
        match self.output_buffer.pop() {
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
    fetching: HashSet<Key>,
    read_for: HashMap<(StateVersion, Key), HashMap<ReplicaIndex, StateVersion>>,
    reorder_queries: HashMap<StateVersion, HashMap<Key, HashSet<ReplicaIndex>>>,

    proceed_buffer: Vec<Proceed<ShardedStorageSend, StorageStateOutput>>,
}

pub struct ShardedStorageConfig {
    num_node: NodeIndex, // virtual "storage node"
    num_active_copy: usize,
}

impl ShardedStorageConfig {
    fn node_indices_of(&self, key: Key) -> Vec<NodeIndex> {
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
            fetching: Default::default(),
            read_for: Default::default(),
            reorder_queries: Default::default(),
            proceed_buffer: Default::default(),
            config,
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ShardedStorageMessage {
    Query(message::Query),
    QueryOk(message::QueryOk),
}

// should we just add a Multi variant to replication::Dest?
pub enum Dest {
    One(ReplicaIndex),
    Multi(Vec<ReplicaIndex>),
    All,
}

type ShardedStorageSend = (Dest, ShardedStorageMessage);

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
                .push(Proceed::Send((dest, ShardedStorageMessage::Query(query))))
        }
    }

    // fn fetch_ahead(&mut self, index: ShardIndex, _version_ahead: StateVersion) {
    //     if self.stored_shards.last().unwrap().contains_key(&index) {
    //         return;
    //     }
    //     let fetch = message::Fetch {
    //         version: None,
    //         shard_index: index,
    //         replica_index: self.replica_index,
    //     };
    //     let dest = Dest::Multi(self.config.node_indices_of(index));
    //     self.proceed_buffer
    //         .push(Proceed::Send((dest, ShardedStorageMessage::Fetch(fetch))))
    // }

    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        // tracing::trace!(%self.replica_index, %self.version, "bumping");

        if !self.fetching.is_empty() {
            tracing::warn!(%self.replica_index, "bump with ongoing fetches");
            self.fetching.clear()
        }

        self.version += 1;
        for (&key, bytes) in &writes {
            if self.config.should_store(&self.node_indices, key) {
                self.proceed_buffer
                    .push(Proceed::Output(StorageStateOutput::Write(
                        format!("{key:x}.{}", self.version),
                        bytes.clone(),
                    )));
                self.key_versions.get_mut(&key).unwrap().push(self.version)
            }
        }

        if let Some(fetches) = self.reorder_queries.remove(&self.version) {
            for (key, service_indices) in fetches {
                let query_ok = message::QueryOk {
                    version: self.version,
                    key: key.0,
                    bytes: Some(writes[&key].to_vec()),
                };
                self.proceed_buffer.push(Proceed::Send((
                    Dest::Multi(service_indices.into_iter().collect()),
                    ShardedStorageMessage::QueryOk(query_ok),
                )))
            }
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
                self.proceed_buffer.push(proceed)
            }
        }
    }

    fn write_ok(&mut self, _key: String) {}
}

impl State for ShardedStorage {
    type Send = ShardedStorageSend;
    type Output = StorageStateOutput;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(proceed) = self.proceed_buffer.pop() {
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
                        .push(Proceed::Output(StorageStateOutput::Fetched(
                            key,
                            fetch_ok.bytes.map(Into::into),
                        )))
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
            self.proceed_buffer.push(proceed);
            return;
        };
        let targets = self.read_for.entry((found_version, key)).or_default();
        if targets.is_empty() {
            self.proceed_buffer
                .push(Proceed::Output(StorageStateOutput::Read(format!(
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
}

pub mod message {
    use bincode::{Decode, Encode};

    use super::{ReplicaIndex, StateVersion};

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
}
