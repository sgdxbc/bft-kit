use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    time::{Duration, Instant},
};

use bincode::{Decode, Encode, decode_from_slice};
use lru::LruCache;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};
use tokio_util::bytes::Bytes;

use crate::{
    Never,
    replication::{ReplicaIndex, ReplicationState},
    state::{Proceed, State, earliest},
    workload::NanoLatencies,
};

use self::app::{
    DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState as _, InitDataShard,
    ShardIndex,
};

use super::{
    AppProtocol, ClientId, ClientSeq, Message, Output, Reply, Request, Send, ServiceIndex,
    ServiceState,
};

pub mod app;
pub mod transport;

#[cfg(test)]
mod tests;

type StateVersion = u64;

pub trait StorageState: State<Output = StorageStateOutput> {
    fn fetch(&mut self, index: ShardIndex);
    fn bump(&mut self, shards: HashMap<ShardIndex, Bytes>);
    #[allow(unused_variables)]
    fn will_fetch(&mut self, index: ShardIndex, version_ahead: StateVersion) {}

    fn read_ok(&mut self, key: String, value: Bytes);
    fn write_ok(&mut self, key: String);
}

pub enum StorageStateOutput {
    Fetched(ShardIndex, Bytes),
    Skipped(StateVersion), // number of versions to skip execute

    Read(String),
    Write(String, Bytes),
}

pub struct BigService<
    A: DataShardingApp,
    R: ReplicationState<Request<A::Op>>,
    S: State = ShardedStorage,
> {
    // config: ServiceConfig,
    // generic sub states
    app: A,
    replication: R,
    storage: S,
    // essential state data
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    replicated: VecDeque<Replicated<A, R>>,
    fetched_shards: HashMap<ShardIndex, A::Shard>,
    fetch_indices: HashSet<ShardIndex>,
    num_skip: StateVersion,
    // additional data for optimization
    shard_cache: Option<LruCache<ShardIndex, Bytes>>,
    // cross interface buffers
    submit_buffer: Vec<Request<A::Op>>,
    #[allow(clippy::type_complexity)] // this matches <Self as State>::Send
    send_buffer: Vec<Send<Reply<A::Res, R::Metadata>, ServiceSend<R, S>>>,
    // stats
    execute_latencies: NanoLatencies,
}

pub struct ServiceConfig {
    num_cached_shard: usize,
}

type Replicated<A, R> = (
    VecDeque<Executing<A>>,
    <R as ReplicationState<Request<<A as AppProtocol>::Op>>>::Metadata,
);

struct Executing<A: DataShardingApp> {
    execute: A::Execute,
    client_id: ClientId,
    client_seq: ClientSeq,
    start: Instant,
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: State> BigService<A, R, S> {
    pub fn new(app: A, replication: R, storage: S, config: ServiceConfig) -> Self {
        Self {
            app,
            replication,
            storage,
            replies: Default::default(),
            replicated: Default::default(),
            fetched_shards: Default::default(),
            fetch_indices: Default::default(),
            num_skip: 0,
            shard_cache: config.num_cached_shard.try_into().ok().map(LruCache::new),
            submit_buffer: Default::default(),
            send_buffer: Default::default(),
            execute_latencies: NanoLatencies::new(3).unwrap(),
            // config,
        }
    }
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: State + InitStore<A::Shard>>
    BigService<A, R, S>
{
    pub fn init_store(
        &mut self,
        init_shard: &impl InitDataShard<A::Shard>,
        store: &mut impl Store,
    ) -> anyhow::Result<()> {
        self.storage.init(init_shard, store)
    }
}

pub enum ServiceSend<R: State, S: State> {
    Replication(R::Send),
    Storage(S::Send),
}

// direct generics (instead of R::Message, S::Message) because derive Encode and
// Decode only works with this form (and derive_where does not support arbitrary
// traits)
// maybe change ServiceSend to this form as well? seems not strictly unnecessary
#[derive(Debug, Encode, Decode)]
pub enum ServiceMessage<RM, SM> {
    Replication(RM),
    Storage(SM),
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: StorageState> ServiceState<A>
    for BigService<A, R, S>
where
    A::Shard: Encode + Decode<()>,
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type ServiceSend = ServiceSend<R, S>;
    type ServiceMessage = ServiceMessage<R::Message, S::Message>;
    type Metadata = R::Metadata;

    fn read_ok(&mut self, key: String, value: Bytes) {
        self.storage.read_ok(key, value)
    }

    fn write_ok(&mut self, key: String) {
        self.storage.write_ok(key)
    }
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: StorageState> State
    for BigService<A, R, S>
where
    A::Shard: Encode + Decode<()>,
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type Send = Send<Reply<A::Res, R::Metadata>, <Self as ServiceState<A>>::ServiceSend>;
    type Output = Output;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(send) = self.send_buffer.pop() {
            return Proceed::Send(send);
        }

        if let Some((executing_buffer, metadata)) = self.replicated.front_mut() {
            let Some(executing) = executing_buffer.front_mut() else {
                self.replicated.pop_front();
                return self.proceed(since_start);
            };
            // it would be better if we can filter before constructing `A::Execute`
            // anyway should be rare
            if let Some(reply) = self.replies.get(&executing.client_id)
                && reply.client_seq >= executing.client_seq
            {
                return self.proceed(since_start);
            }

            if self.num_skip > 0 {
                self.num_skip -= 1;
                executing_buffer.pop_front();
                return self.proceed(since_start);
            }

            if self.fetched_shards.len() == self.fetch_indices.len() {
                match executing.execute.proceed(&mut self.fetched_shards) {
                    DataShardingExecuteOutput::RequireAccess(required_indices) => {
                        for shard_index in required_indices {
                            assert!(!self.fetched_shards.contains_key(&shard_index));
                            if let Some(bytes) = self
                                .shard_cache
                                .as_mut()
                                .and_then(|shard_cache| shard_cache.get(&shard_index))
                            {
                                let (shard, _len) =
                                    decode_from_slice(bytes, BINCODE_CONFIG).unwrap();
                                self.fetched_shards.insert(shard_index, shard);
                            } else {
                                self.storage.fetch(shard_index)
                            }
                            let inserted = self.fetch_indices.insert(shard_index);
                            assert!(inserted)
                        }
                        if self.fetched_shards.len() == self.fetch_indices.len() {
                            return self.proceed(since_start);
                        }
                    }
                    DataShardingExecuteOutput::Complete(res) => {
                        self.execute_latencies += executing.start.elapsed().as_nanos() as u64;

                        let shards = take(&mut self.fetched_shards)
                            .into_iter()
                            .map(|(index, shard)| {
                                let bytes = bincode::encode_to_vec(shard, BINCODE_CONFIG)
                                    .unwrap()
                                    .into();
                                (index, bytes)
                            })
                            .collect::<HashMap<_, Bytes>>();
                        if let Some(shard_cache) = &mut self.shard_cache {
                            for (&shard_index, shard) in &shards {
                                shard_cache.put(shard_index, shard.clone());
                            }
                        }
                        self.storage.bump(shards);
                        self.fetch_indices.clear();

                        let reply = Reply {
                            client_seq: executing.client_seq,
                            res,
                            metadata: metadata.clone(),
                        };
                        self.replies.insert(executing.client_id, reply.clone());
                        let proceed = Proceed::Send(Send::Reply(executing.client_id, reply));
                        executing_buffer.pop_front();
                        return proceed;
                    }
                }
            }
        }

        let storage_tick_after = loop {
            match self.storage.proceed(since_start) {
                Proceed::Pending(tick_after) => break tick_after,
                Proceed::Send(send) => {
                    return Proceed::Send(Send::Intermediate(ServiceSend::Storage(send)));
                }
                Proceed::Output(StorageStateOutput::Read(key)) => {
                    return Proceed::Output(Output::Read(key));
                }
                Proceed::Output(StorageStateOutput::Write(key, value)) => {
                    return Proceed::Output(Output::Write(key, value));
                }
                Proceed::Output(StorageStateOutput::Fetched(shard_index, bytes)) => {
                    let (shard, _len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG).unwrap();
                    self.fetched_shards.insert(shard_index, shard);
                    if self.fetched_shards.len() == self.fetch_indices.len() {
                        return self.proceed(since_start);
                    }
                }
                Proceed::Output(StorageStateOutput::Skipped(num_skipped)) => {
                    self.num_skip += num_skipped;
                    return self.proceed(since_start);
                }
            }
        };

        while let Some(request) = self.submit_buffer.pop() {
            self.replication.submit(request)
        }
        if self
            .replicated
            .iter()
            .map(|(buffer, _)| buffer.len())
            .sum::<usize>()
            // TODO configurable
            > 0
        {
            return Proceed::Pending(storage_tick_after);
        }
        match self.replication.proceed(since_start) {
            Proceed::Pending(tick_after) => {
                Proceed::Pending(earliest([storage_tick_after, tick_after]))
            }
            Proceed::Send(send) => {
                Proceed::Send(Send::Intermediate(ServiceSend::Replication(send)))
            }
            Proceed::Output(replicated) => {
                let mut executing_buffer = VecDeque::new();
                let start = Instant::now();
                let logs_version_ahead = self
                    .replicated
                    .iter()
                    .map(|(buffer, _)| buffer.len())
                    .sum::<usize>();
                for (i, request) in replicated.logs.into_iter().enumerate() {
                    // may query ahead here as an optimization
                    let mut execute = self.app.new_execute(request.op);
                    if let DataShardingExecuteOutput::RequireAccess(required_indices) =
                        execute.proceed(&mut Default::default())
                    {
                        for index in required_indices {
                            self.storage
                                .will_fetch(index, (logs_version_ahead + i) as _)
                        }
                    }
                    executing_buffer.push_back(Executing {
                        execute,
                        client_id: request.client_id,
                        client_seq: request.client_seq,
                        start,
                    })
                }
                self.replicated
                    .push_back((executing_buffer, replicated.metadata));
                self.proceed(since_start)
            }
        }
    }

    type Message = Message<Request<A::Op>, <Self as ServiceState<A>>::ServiceMessage>;
    fn receive(&mut self, message: Self::Message) {
        match message {
            Message::Request(request) => match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => {}
                Some(reply) if reply.client_seq == request.client_seq => self
                    .send_buffer
                    .push(Send::Reply(request.client_id, reply.clone())),
                _ => self.submit_buffer.push(request),
            },
            Message::Intermediate(ServiceMessage::Storage(message)) => {
                self.storage.receive(message)
            }
            Message::Intermediate(ServiceMessage::Replication(message)) => {
                self.replication.receive(message)
            }
        }
    }
}

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

type NodeIndex = ServiceIndex;

pub trait InitStore<S> {
    fn init(
        &mut self,
        init_shard: &impl InitDataShard<S>,
        store: &mut impl Store,
    ) -> anyhow::Result<()>;
}

pub trait Store {
    fn write(&mut self, key: String, value: Bytes) -> anyhow::Result<()>;
}

pub struct ShardedStorage {
    config: ShardedStorageConfig,
    replica_index: ReplicaIndex,
    node_indices: HashSet<NodeIndex>,

    version: StateVersion,
    shard_versions: HashMap<ShardIndex, Vec<StateVersion>>,
    fetching: HashSet<ShardIndex>,
    read_for: HashMap<(StateVersion, ShardIndex), HashMap<ReplicaIndex, StateVersion>>,
    reorder_queries: HashMap<StateVersion, HashMap<ShardIndex, HashSet<ReplicaIndex>>>,

    proceed_buffer: Vec<Proceed<ShardedStorageSend, StorageStateOutput>>,
}

pub struct ShardedStorageConfig {
    num_node: NodeIndex, // virtual "storage node"
    num_shard: ShardIndex,
    num_active_copy: usize,
}

impl ShardedStorageConfig {
    fn node_indices_of(&self, index: ShardIndex) -> Vec<NodeIndex> {
        (0..self.num_node)
            .choose_multiple(&mut StdRng::seed_from_u64(index as _), self.num_active_copy)
    }

    fn should_store(&self, node_indices: &HashSet<NodeIndex>, index: ShardIndex) -> bool {
        self.node_indices_of(index)
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
            shard_versions: Default::default(),
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
    fn fetch(&mut self, index: ShardIndex) {
        // tracing::trace!(%self.replica_index, shard_index = %index);

        let inserted = self.fetching.insert(index);
        assert!(inserted);

        if self.config.should_store(&self.node_indices, index) {
            self.read_shard(self.replica_index, self.version, index)
        } else {
            let query = message::Query {
                version: self.version,
                shard_index: index,
                replica_index: self.replica_index,
            };
            let dest = Dest::Multi(self.config.node_indices_of(index));
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

    fn bump(&mut self, shards: HashMap<ShardIndex, Bytes>) {
        // tracing::trace!(%self.replica_index, %self.version, "bumping");

        if !self.fetching.is_empty() {
            tracing::warn!(%self.replica_index, "bump with ongoing fetches");
            self.fetching.clear()
        }

        self.version += 1;
        for (&index, bytes) in &shards {
            if self.config.should_store(&self.node_indices, index) {
                self.proceed_buffer
                    .push(Proceed::Output(StorageStateOutput::Write(
                        format!("{}.{index}", self.version),
                        bytes.clone(),
                    )));
                self.shard_versions
                    .get_mut(&index)
                    .unwrap()
                    .push(self.version)
            }
        }

        if let Some(fetches) = self.reorder_queries.remove(&self.version) {
            for (shard_index, service_indices) in fetches {
                let query_ok = message::QueryOk {
                    version: self.version,
                    shard_index,
                    bytes: shards[&shard_index].to_vec(),
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
        let shard_index = shard_index.parse().unwrap();

        if let Some(targets) = self.read_for.remove(&(version, shard_index)) {
            for (replica_index, version) in targets {
                let proceed = if replica_index == self.replica_index {
                    assert_eq!(version, self.version); // or relax on this, just continue
                    let exists = self.fetching.remove(&shard_index);
                    assert!(exists);
                    Proceed::Output(StorageStateOutput::Fetched(shard_index, value.clone()))
                } else {
                    let query_ok = message::QueryOk {
                        version,
                        shard_index,
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
                self.read_shard(fetch.replica_index, fetch.version, fetch.shard_index)
            }
            ShardedStorageMessage::QueryOk(fetch_ok) => {
                if fetch_ok.version == self.version && self.fetching.remove(&fetch_ok.shard_index) {
                    self.proceed_buffer
                        .push(Proceed::Output(StorageStateOutput::Fetched(
                            fetch_ok.shard_index,
                            fetch_ok.bytes.into(),
                        )))
                }
            }
        }
    }
}

impl ShardedStorage {
    fn read_shard(
        &mut self,
        replica_index: ReplicaIndex,
        version: StateVersion,
        shard_index: ShardIndex,
    ) {
        let shard_versions = &self.shard_versions[&shard_index];
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
        let targets = self
            .read_for
            .entry((found_version, shard_index))
            .or_default();
        if targets.is_empty() {
            self.proceed_buffer
                .push(Proceed::Output(StorageStateOutput::Read(format!(
                    "{found_version}.{shard_index}"
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

impl<S: Encode> InitStore<S> for ShardedStorage {
    fn init(
        &mut self,
        init_shard: &impl InitDataShard<S>,
        store: &mut impl Store,
    ) -> anyhow::Result<()> {
        for index in 0..self.config.num_shard {
            if self.config.should_store(&self.node_indices, index) {
                let shard = init_shard.init(index);
                let bytes = bincode::encode_to_vec(shard, BINCODE_CONFIG)?;
                store.write(format!("{}.{index}", self.version), bytes.into())?;

                self.shard_versions.insert(index, vec![self.version]);
            }
        }
        Ok(())
    }
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
    fn fetch(&mut self, index: ShardIndex) {
        self.output_buffer
            .push(StorageStateOutput::Read(index.to_string()))
    }

    fn bump(&mut self, shards: HashMap<ShardIndex, Bytes>) {
        for (index, bytes) in shards {
            self.output_buffer
                .push(StorageStateOutput::Write(index.to_string(), bytes))
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

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(output) = self.output_buffer.pop() {
            return Proceed::Output(output);
        }
        Proceed::Pending(None)
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}

impl<S: Encode> InitStore<S> for FullReplicationStorage {
    fn init(
        &mut self,
        init_shard: &impl InitDataShard<S>,
        store: &mut impl Store,
    ) -> anyhow::Result<()> {
        for index in 0..init_shard.num_shard() {
            let shard = init_shard.init(index as ShardIndex);
            let bytes = bincode::encode_to_vec(shard, BINCODE_CONFIG)?;
            store.write(index.to_string(), bytes.into())?;
        }
        Ok(())
    }
}

pub mod message {
    use bincode::{Decode, Encode};

    use super::{ServiceIndex, ShardIndex, StateVersion};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Query {
        pub version: StateVersion,
        pub shard_index: ShardIndex,
        pub replica_index: ServiceIndex,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct QueryOk {
        pub version: StateVersion,
        pub shard_index: ShardIndex,
        pub bytes: Vec<u8>, // `Bytes` does not support Encode/Decode
    }
}

mod parse {
    use crate::parse::{Configs, Extract};

    use super::{ServiceConfig, ShardedStorageConfig};

    impl Extract for ServiceConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_cached_shard: configs.get("big.num-cached-shard")?,
            })
        }
    }

    impl Extract for ShardedStorageConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_shard: configs.get("big.num-shard")?,
                num_node: configs.get("big.num-node")?,
                num_active_copy: configs.get("big.num-active-copy")?,
            })
        }
    }
}
