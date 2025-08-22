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
    Fetched(Key, Bytes),
    Skipped(StateVersion), // number of versions to skip execute

    Read(String),
    Write(String, Bytes),
}

pub struct FullReplicationStorage {
    output_buffer: Vec<StorageStateOutput>,
}

impl FullReplicationStorage {
    pub fn new() -> Self {
        Self {
            output_buffer: Default::default(),
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
        self.output_buffer
            .push(StorageStateOutput::Read(format!("{key:x}")))
    }
    fn bump(&mut self, writes: HashMap<Key, Bytes>) {
        for (key, value) in writes {
            self.output_buffer
                .push(StorageStateOutput::Write(format!("{key:x}"), value))
        }
    }
    fn read_ok(&mut self, key: String, value: Bytes) {
        self.output_buffer
            .push(StorageStateOutput::Fetched(key.parse().unwrap(), value))
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
        // tracing::trace!(%self.replica_index, shard_index = %index);

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
                        format!("{key}.{}", self.version),
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
                    bytes: writes[&key].to_vec(),
                };
                self.proceed_buffer.push(Proceed::Send((
                    Dest::Multi(service_indices.into_iter().collect()),
                    ShardedStorageMessage::QueryOk(query_ok),
                )))
            }
        }
    }

    fn read_ok(&mut self, key: String, value: Bytes) {
        let (version, shard_index) = key.split_once('.').unwrap();
        let version = version.parse::<StateVersion>().unwrap();
        let key = shard_index.parse().unwrap();

        if let Some(targets) = self.read_for.remove(&(version, key)) {
            for (replica_index, version) in targets {
                let proceed = if replica_index == self.replica_index {
                    assert_eq!(version, self.version); // or relax on this, just continue
                    let exists = self.fetching.remove(&key);
                    assert!(exists);
                    Proceed::Output(StorageStateOutput::Fetched(key, value.clone()))
                } else {
                    let query_ok = message::QueryOk {
                        version,
                        key: key.0,
                        bytes: value.to_vec(),
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
                            fetch_ok.bytes.into(),
                        )))
                }
            }
        }
    }
}

impl ShardedStorage {
    fn read_key(&mut self, replica_index: ReplicaIndex, version: StateVersion, key: Key) {
        let shard_versions = &self.key_versions[&key];
        let found_version = match shard_versions.binary_search(&version) {
            Ok(index) => shard_versions[index],
            Err(0) => {
                // the version to read has garbage collected
                // the remote replica will progress when it collects a bump quorum
                assert_ne!(replica_index, self.replica_index);
                return;
            }
            Err(index) => shard_versions[index - 1],
        };
        let targets = self.read_for.entry((found_version, key)).or_default();
        if targets.is_empty() {
            self.proceed_buffer
                .push(Proceed::Output(StorageStateOutput::Read(format!(
                    "{key}.{found_version}"
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
        pub bytes: Vec<u8>, // `Bytes` does not support Encode/Decode
    }
}
