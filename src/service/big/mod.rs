use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    time::{Duration, Instant},
};

use bincode::{Decode, Encode};
use lru::LruCache;
use tokio_util::bytes::Bytes;

use crate::{
    app::DataShardingExecuteState,
    crypto::{DigestHash, UpdateHash},
    replication::ReplicationState,
    state::{Action, State, earliest},
    workload::NanoLatencies,
};

use crate::app::{DataShardingApp, DataShardingExecuteOutput};

use self::storage::{Key, ShardedStorage, StateVersion, StorageState, StorageStateOutput};

use super::{
    AppProtocol, ClientId, ClientSeq, Message, Output, Reply, Request, Send, ServiceState,
};

pub mod storage;
pub mod transport;

#[cfg(test)]
mod tests;

const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

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
    fetch_keys: HashMap<Key, A::Key>,
    num_skip: StateVersion,
    // additional data for optimization
    value_cache: Option<LruCache<A::Key, A::Value>>,
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
    execute: A::ExecuteState,
    client_id: ClientId,
    client_seq: ClientSeq,
    start: Instant,
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: State> BigService<A, R, S> {
    pub fn new(app: A, replication: R, storage: S, config: ServiceConfig) -> Self
    where
        A::Key: Hash + Eq,
    {
        Self {
            app,
            replication,
            storage,
            replies: Default::default(),
            replicated: Default::default(),
            fetch_keys: Default::default(),
            num_skip: 0,
            value_cache: config.num_cached_shard.try_into().ok().map(LruCache::new),
            submit_buffer: Default::default(),
            send_buffer: Default::default(),
            execute_latencies: NanoLatencies::new(3).unwrap(),
            // config,
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

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>, S: StorageState> ServiceState<A>
    for BigService<A, R, S>
where
    A::Key: Hash + Eq + UpdateHash,
    A::Value: Encode + Decode<()> + Clone,
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
    A::Key: Hash + Eq + UpdateHash,
    A::Value: Encode + Decode<()> + Clone,
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type Send = Send<Reply<A::Res, R::Metadata>, <Self as ServiceState<A>>::ServiceSend>;
    type Output = Output;
    fn proceed(&mut self, since_start: Duration) -> Action<Self::Send, Self::Output> {
        if let Some(send) = self.send_buffer.pop() {
            return Action::Send(send);
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

            if self.fetch_keys.is_empty() {
                match executing.execute.proceed() {
                    DataShardingExecuteOutput::Pending(keys) => {
                        for key in keys {
                            if let Some(value) =
                                self.value_cache.as_mut().and_then(|cache| cache.get(&key))
                            {
                                executing.execute.install(key, Some(value.clone()));
                                continue;
                            }
                            let storage_key = Key::from(key.digest().0);
                            self.fetch_keys.insert(storage_key, key);
                            self.storage.fetch(storage_key)
                        }
                    }
                    DataShardingExecuteOutput::Complete(res, writes) => {
                        self.execute_latencies += executing.start.elapsed().as_nanos() as u64;

                        let mut bump_writes = HashMap::new();
                        for (key, value) in writes {
                            let storage_key = Key::from(key.digest().0);
                            let bytes = bincode::encode_to_vec(&value, BINCODE_CONFIG)
                                .unwrap()
                                .into();
                            bump_writes.insert(storage_key, bytes);
                            if let Some(value_cache) = &mut self.value_cache {
                                value_cache.put(key, value);
                            }
                        }
                        self.storage.bump(bump_writes);

                        let reply = Reply {
                            client_seq: executing.client_seq,
                            res,
                            metadata: metadata.clone(),
                        };
                        self.replies.insert(executing.client_id, reply.clone());
                        let proceed = Action::Send(Send::Reply(executing.client_id, reply));
                        executing_buffer.pop_front();
                        return proceed;
                    }
                }
            }
        }

        #[allow(clippy::diverging_sub_expression)] // TODO
        let storage_tick_after = 'storage: {
            return match self.storage.proceed(since_start) {
                Action::Pending(tick_after) => break 'storage tick_after,
                Action::Send(send) => {
                    Action::Send(Send::Intermediate(ServiceSend::Storage(send)))
                }
                Action::Output(StorageStateOutput::Read(key)) => {
                    Action::Output(Output::Read(key))
                }
                Action::Output(StorageStateOutput::Write(key, value)) => {
                    Action::Output(Output::Write(key, value))
                }
                Action::Output(StorageStateOutput::Fetched(key, bytes)) => {
                    let value = bytes.map(|bytes| {
                        let (value, _len) =
                            bincode::decode_from_slice(&bytes, BINCODE_CONFIG).unwrap();
                        value
                    });
                    let key = self.fetch_keys.remove(&key).unwrap();
                    self.replicated
                        .front_mut()
                        .unwrap()
                        .0
                        .front_mut()
                        .unwrap()
                        .execute
                        .install(key, value);
                    self.proceed(since_start)
                }
                Action::Output(StorageStateOutput::Skipped(num_skipped)) => {
                    self.num_skip += num_skipped;
                    self.proceed(since_start)
                }
            };
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
            return Action::Pending(storage_tick_after);
        }
        match self.replication.proceed(since_start) {
            Action::Pending(tick_after) => {
                Action::Pending(earliest([storage_tick_after, tick_after]))
            }
            Action::Send(send) => {
                Action::Send(Send::Intermediate(ServiceSend::Replication(send)))
            }
            Action::Output(replicated) => {
                let mut executing_buffer = VecDeque::new();
                let start = Instant::now();
                let logs_version_ahead = self
                    .replicated
                    .iter()
                    .map(|(buffer, _)| buffer.len())
                    .sum::<usize>();
                for (i, request) in replicated.logs.into_iter().enumerate() {
                    let mut execute = self.app.new_execute(request.op);
                    if let DataShardingExecuteOutput::Pending(keys) = execute.proceed() {
                        for key in keys {
                            self.storage
                                .will_fetch(key.digest().0.into(), (logs_version_ahead + i) as _)
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

mod parse {
    use crate::parse::{Configs, Extract};

    use super::ServiceConfig;

    impl Extract for ServiceConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                num_cached_shard: configs.get("big.num-cached-shard")?,
            })
        }
    }
}
