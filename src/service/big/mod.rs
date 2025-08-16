use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};

use crate::{
    Never,
    replication::{ReplicaIndex, ReplicationState},
    state::{Proceed, State, earliest},
    workload::NanoLatencies,
};

use super::{
    ClientId, ClientSeq, Message, Reply, Request, Send, ServiceApp, ServiceIndex, ServiceState,
};

pub mod app;
pub mod transport;

#[cfg(test)]
mod tests;

type ShardIndex = u32;
type StateVersion = u64;

pub trait DataShardingApp: ServiceApp + Sized {
    type Shard;
    fn new_shard(&self, index: ShardIndex) -> Self::Shard;

    type Execute: DataShardingExecuteState<Self>;
    fn new_execute(&self, op: Self::Op) -> Self::Execute;
}

pub trait DataShardingExecuteState<A: DataShardingApp> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, A::Shard>,
    ) -> DataShardingExecuteOutput<A::Res>;
}

#[derive(Debug)]
pub enum DataShardingExecuteOutput<R> {
    RequireAccess(HashSet<ShardIndex>),
    Complete(R),
}

pub trait StorageState<S>: State<Output = StorageStateOutput<S>> {
    fn fetch(&mut self, index: ShardIndex);
    // TODO an interface for fetch shard ahead
    fn bump(&mut self, shards: HashMap<ShardIndex, S>);
}

pub enum StorageStateOutput<S> {
    Fetched(ShardIndex, S),
    Skipped(StateVersion), // number of versions to skip execute
}

pub struct Service<
    A: DataShardingApp,
    R: ReplicationState<Request<A::Op>>,
    S: State = ShardedStorage<<A as DataShardingApp>::Shard>,
> {
    app: A,
    replication: R,
    storage: S,

    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    replicated: VecDeque<Replicated<A, R>>,
    shards: HashMap<ShardIndex, A::Shard>,
    num_skip: StateVersion,

    submit_buffer: Vec<Request<A::Op>>,
    #[allow(clippy::type_complexity)] // this matches <Self as State>::Send
    send_buffer: Vec<Send<Reply<A::Res, R::Metadata>, ServiceSend<R, S>>>,

    execute_latencies: NanoLatencies,
}

type Replicated<A, R> = (
    VecDeque<Executing<A>>,
    <R as ReplicationState<Request<<A as ServiceApp>::Op>>>::Metadata,
);

struct Executing<A: DataShardingApp> {
    execute: A::Execute,
    client_id: ClientId,
    client_seq: ClientSeq,
    start: Instant,
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: StorageState<A::Shard>>
    Service<A, R, S>
{
    pub fn new(app: A, replication: R, storage: S) -> Self {
        Self {
            app,
            replication,
            storage,
            replies: Default::default(),
            replicated: Default::default(),
            shards: Default::default(),
            num_skip: 0,
            submit_buffer: Default::default(),
            send_buffer: Default::default(),
            execute_latencies: NanoLatencies::new(3).unwrap(),
        }
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

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: StorageState<A::Shard>>
    ServiceState<A> for Service<A, R, S>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type ServiceSend = ServiceSend<R, S>;
    type ServiceMessage = ServiceMessage<R::Message, S::Message>;
    type Metadata = R::Metadata;
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: StorageState<A::Shard>> State
    for Service<A, R, S>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type Send = Send<Reply<A::Res, R::Metadata>, <Self as ServiceState<A>>::ServiceSend>;
    type Output = Never;
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

            match executing.execute.proceed(&mut self.shards) {
                DataShardingExecuteOutput::RequireAccess(required_indices) => {
                    for shard_index in required_indices {
                        assert!(!self.shards.contains_key(&shard_index));
                        self.storage.fetch(shard_index)
                    }
                }
                DataShardingExecuteOutput::Complete(res) => {
                    self.execute_latencies += executing.start.elapsed().as_nanos() as u64;
                    self.storage.bump(take(&mut self.shards));
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

        let storage_tick_after = match self.storage.proceed(since_start) {
            Proceed::Pending(tick_after) => tick_after,
            Proceed::Send(send) => {
                return Proceed::Send(Send::Intermediate(ServiceSend::Storage(send)));
            }
            Proceed::Output(StorageStateOutput::Fetched(shard_index, shard)) => {
                self.shards.insert(shard_index, shard);
                return self.proceed(since_start);
            }
            Proceed::Output(StorageStateOutput::Skipped(num_skipped)) => {
                self.num_skip += num_skipped;
                return self.proceed(since_start);
            }
        };

        while let Some(request) = self.submit_buffer.pop() {
            self.replication.submit(request)
        }
        if self.replicated.len() >= 10 {
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
                for request in replicated.logs {
                    // may query ahead here as an optimization
                    executing_buffer.push_back(Executing {
                        execute: self.app.new_execute(request.op),
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

type NodeIndex = ServiceIndex;

pub struct ShardedStorage<S> {
    config: ShardedStorageConfig,
    replica_index: ReplicaIndex,

    version: StateVersion, // of shards[-1]
    shards: Vec<HashMap<ShardIndex, S>>,
    fetching: HashSet<ShardIndex>,
    fetched_table: HashMap<ShardIndex, S>,
    reordering_fetch_table: HashMap<StateVersion, HashMap<ShardIndex, HashSet<ReplicaIndex>>>,

    proceed_buffer: Vec<Proceed<ShardedStorageSend<S>, StorageStateOutput<S>>>,
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

impl<S> ShardedStorage<S> {
    pub fn new(
        config: ShardedStorageConfig,
        replica_index: ReplicaIndex,
        node_indices: HashSet<NodeIndex>,
        app: &impl DataShardingApp<Shard = S>,
    ) -> Self {
        let mut shards = HashMap::new();
        for shard_index in 0..config.num_shard {
            if config.should_store(&node_indices, shard_index) {
                shards.insert(shard_index, app.new_shard(shard_index));
            }
        }
        Self {
            config,
            replica_index,
            // stored_indices,
            version: 0,
            shards: vec![shards],
            fetching: Default::default(),
            fetched_table: Default::default(),
            reordering_fetch_table: Default::default(),
            proceed_buffer: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ShardedStorageMessage<S> {
    Fetch(message::Fetch),
    FetchOk(message::FetchOk<S>),
}

// should we just add a Multi variant to replication::Dest?
pub enum Dest {
    One(ReplicaIndex),
    Multi(Vec<ReplicaIndex>),
    All,
}

type ShardedStorageSend<S> = (Dest, ShardedStorageMessage<S>);

impl<S: Clone> StorageState<S> for ShardedStorage<S> {
    fn fetch(&mut self, index: ShardIndex) {
        // tracing::trace!(%self.replica_index, shard_index = %index);

        if let Some(shard) = self.shards.last().unwrap().get(&index) {
            self.proceed_buffer
                .push(Proceed::Output(StorageStateOutput::Fetched(
                    index,
                    shard.clone(),
                )));
            return;
        }
        if !self.fetching.insert(index) {
            return;
        }
        let fetch = message::Fetch {
            version: self.version,
            shard_index: index,
            service_index: self.replica_index,
        };
        let recipient = Dest::Multi(self.config.node_indices_of(index));
        self.proceed_buffer.push(Proceed::Send((
            recipient,
            ShardedStorageMessage::Fetch(fetch),
        )))
    }

    fn bump(&mut self, mut shards: HashMap<ShardIndex, S>) {
        tracing::trace!(%self.replica_index, %self.version, "bumping");
        let mut stored_shards = self.shards.last().unwrap().clone();
        for (shard_index, shard) in &mut stored_shards {
            if let Some(new_shard) = shards.remove(shard_index) {
                *shard = new_shard;
            }
        }

        self.version += 1;
        if let Some(fetches) = self.reordering_fetch_table.remove(&self.version) {
            for (shard_index, service_indices) in fetches {
                let fetch_ok = message::FetchOk {
                    version: self.version,
                    shard_index,
                    shard: stored_shards[&shard_index].clone(),
                };
                self.proceed_buffer.push(Proceed::Send((
                    Dest::Multi(service_indices.into_iter().collect()),
                    ShardedStorageMessage::FetchOk(fetch_ok),
                )))
            }
        }

        self.shards.push(stored_shards);
        self.fetching.clear()
    }
}

impl<S: Clone> State for ShardedStorage<S> {
    type Send = ShardedStorageSend<S>;
    type Output = StorageStateOutput<S>;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(proceed) = self.proceed_buffer.pop() {
            return proceed;
        }
        if let Some(&index) = self.fetched_table.keys().next() {
            self.fetching.remove(&index);
            return Proceed::Output(StorageStateOutput::Fetched(
                index,
                self.fetched_table.remove(&index).unwrap(),
            ));
        }
        Proceed::Pending(None)
    }

    type Message = ShardedStorageMessage<S>;
    fn receive(&mut self, message: Self::Message) {
        match message {
            ShardedStorageMessage::Fetch(fetch) => {
                if fetch.version > self.version {
                    self.reordering_fetch_table
                        .entry(fetch.version)
                        .or_default()
                        .entry(fetch.shard_index)
                        .or_default()
                        .insert(fetch.service_index);
                    return;
                }

                // TODO reply with bump certificate >= fetch.version if available

                let first_version = self.version - (self.shards.len() - 1) as StateVersion;
                assert!(fetch.version >= first_version);
                let Some(shard) =
                    self.shards[(fetch.version - first_version) as usize].get(&fetch.shard_index)
                else {
                    // should not happen (except faulty fetching)
                    return;
                };
                let fetch_ok = message::FetchOk {
                    version: fetch.version,
                    shard_index: fetch.shard_index,
                    shard: shard.clone(),
                };
                self.proceed_buffer.push(Proceed::Send((
                    Dest::One(fetch.service_index),
                    ShardedStorageMessage::FetchOk(fetch_ok),
                )))
            }
            ShardedStorageMessage::FetchOk(fetch_ok) => {
                if fetch_ok.version > self.version {
                    // currently not happen
                    return;
                }
                if fetch_ok.version < self.version || !self.fetching.contains(&fetch_ok.shard_index)
                {
                    return;
                }
                self.fetched_table
                    .insert(fetch_ok.shard_index, fetch_ok.shard);
            }
        }
    }
}

pub struct FullReplicationStorage<S> {
    shards: HashMap<ShardIndex, S>,
    output_buffer: Vec<StorageStateOutput<S>>,
}

impl<S> FullReplicationStorage<S> {
    pub fn new(num_shard: ShardIndex, app: &impl DataShardingApp<Shard = S>) -> Self {
        Self {
            shards: (0..num_shard)
                .map(|shard_index| (shard_index, app.new_shard(shard_index)))
                .collect(),
            output_buffer: Default::default(),
        }
    }
}

impl<S: Clone> StorageState<S> for FullReplicationStorage<S> {
    fn fetch(&mut self, index: ShardIndex) {
        self.output_buffer.push(StorageStateOutput::Fetched(
            index,
            self.shards[&index].clone(),
        ))
    }

    fn bump(&mut self, shards: HashMap<ShardIndex, S>) {
        self.shards.extend(shards)
    }
}

impl<S: Clone> State for FullReplicationStorage<S> {
    type Send = Never;
    type Output = StorageStateOutput<S>;

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

pub mod message {
    use bincode::{Decode, Encode};

    use super::{ServiceIndex, ShardIndex, StateVersion};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Fetch {
        pub version: StateVersion,
        pub shard_index: ShardIndex,
        pub service_index: ServiceIndex,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct FetchOk<S> {
        pub version: StateVersion,
        pub shard_index: ShardIndex,
        pub shard: S,
    }
}

mod parse {
    use crate::parse::{Extract, Settings};

    use super::ShardedStorageConfig;

    impl Extract for ShardedStorageConfig {
        fn extract(settings: &Settings) -> anyhow::Result<Self> {
            Ok(Self {
                num_shard: settings.get("big.num-shard")?,
                num_node: settings.get("big.num-node")?,
                num_active_copy: settings.get("big.num-active-copy")?,
            })
        }
    }
}
