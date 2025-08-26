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
    state::{Proceed, State, earliest},
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
    R: ReplicationState<BigServiceLog<A, S>>,
    S: StorageState = ShardedStorage,
> {
    config: ServiceConfig,
    // generic sub states
    app: A,
    replication: R,
    storage: S,
    // essential state data
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    executing: VecDeque<Executing<A::ExecuteState, R::Metadata>>,
    fetch_keys: HashMap<Key, A::Key>,
    bumping: bool,
    num_skip: StateVersion,
    // additional data for optimization
    value_cache: Option<LruCache<A::Key, A::Value>>,
    // cross interface buffers
    #[allow(clippy::type_complexity)]
    resend_replies: Vec<(ClientId, Reply<A::Res, R::Metadata>)>,
    // stats
    execute_latencies: NanoLatencies,
}

pub struct ServiceConfig {
    num_cached_value: usize,
    executing_buffer_size: usize,
}

pub enum BigServiceLog<A: AppProtocol, S: StorageState> {
    Request(Request<A::Op>),
    StorageOrderedMessage(S::OrderedMessage),
}

struct Executing<E, M> {
    execute: E,
    client_id: ClientId,
    client_seq: ClientSeq,
    metadata: M,
    start: Instant,
}

impl<A: DataShardingApp, R: ReplicationState<BigServiceLog<A, S>>, S: StorageState>
    BigService<A, R, S>
{
    pub fn new(app: A, replication: R, storage: S, config: ServiceConfig) -> Self
    where
        A::Key: Hash + Eq,
    {
        Self {
            app,
            replication,
            storage,
            replies: Default::default(),
            executing: Default::default(),
            fetch_keys: Default::default(),
            bumping: false,
            num_skip: 0,
            value_cache: config.num_cached_value.try_into().ok().map(LruCache::new),
            resend_replies: Default::default(),
            execute_latencies: NanoLatencies::new(3).unwrap(),
            config,
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

impl<A: DataShardingApp, R: ReplicationState<BigServiceLog<A, S>>, S: StorageState> ServiceState<A>
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

impl<A: DataShardingApp, R: ReplicationState<BigServiceLog<A, S>>, S: StorageState> State
    for BigService<A, R, S>
where
    A::Key: Hash + Eq + UpdateHash,
    A::Value: Encode + Decode<()> + Clone,
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type Send = Send<Reply<A::Res, R::Metadata>, <Self as ServiceState<A>>::ServiceSend>;
    type Output = Output;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some((client_id, reply)) = self.resend_replies.pop() {
            return Proceed::Send(Send::Reply(client_id, reply));
        }

        let mut earliest_tick_after = None;

        while self.executing.len() <= self.config.executing_buffer_size {
            match self.replication.proceed(since_start) {
                Proceed::Pending(tick_after) => {
                    earliest_tick_after = earliest([tick_after, earliest_tick_after]);
                    break;
                }
                Proceed::Send(send) => {
                    return Proceed::Send(Send::Intermediate(ServiceSend::Replication(send)));
                }
                Proceed::Output(replicated) => {
                    let start = Instant::now();
                    for log in replicated.logs {
                        match log {
                            BigServiceLog::Request(request) => {
                                let mut execute = self.app.new_execute(request.op);
                                if let DataShardingExecuteOutput::Pending(keys) = execute.proceed()
                                {
                                    for key in keys {
                                        self.storage.will_fetch(key.digest().0.into())
                                    }
                                }
                                self.executing.push_back(Executing {
                                    execute,
                                    client_id: request.client_id,
                                    client_seq: request.client_seq,
                                    metadata: replicated.metadata.clone(),
                                    start,
                                })
                            }
                            BigServiceLog::StorageOrderedMessage(message) => {
                                self.storage.receive_ordered(message)
                            }
                        }
                    }
                }
            }
        }

        loop {
            match self.storage.proceed(since_start) {
                Proceed::Pending(tick_after) => {
                    earliest_tick_after = earliest([tick_after, earliest_tick_after]);
                    break;
                }
                Proceed::Send(send) => {
                    return Proceed::Send(Send::Intermediate(ServiceSend::Storage(send)));
                }
                Proceed::Output(StorageStateOutput::Read(key)) => {
                    return Proceed::Output(Output::Read(key));
                }
                Proceed::Output(StorageStateOutput::Write(key, value)) => {
                    return Proceed::Output(Output::Write(key, value));
                }
                Proceed::Output(StorageStateOutput::OrderedSend(gossip)) => {
                    self.replication
                        .submit(BigServiceLog::StorageOrderedMessage(gossip));
                    return self.proceed(since_start);
                }
                Proceed::Output(StorageStateOutput::Fetched(key, bytes)) => {
                    let value = bytes.map(|bytes| {
                        let (value, _len) =
                            bincode::decode_from_slice(&bytes, BINCODE_CONFIG).unwrap();
                        value
                    });
                    let key = self.fetch_keys.remove(&key).unwrap();
                    self.executing
                        .front_mut()
                        .unwrap()
                        .execute
                        .install(key, value)
                }
                Proceed::Output(StorageStateOutput::Bumped) => {
                    assert!(self.bumping);
                    self.bumping = false
                }
                Proceed::Output(StorageStateOutput::Skipped(num_skipped)) => {
                    self.num_skip += num_skipped
                }
            }
        }

        while let Some(executing) = self.executing.front_mut() {
            // it would be better if we can filter before constructing `A::Execute`
            // anyway should be rare
            if let Some(reply) = self.replies.get(&executing.client_id)
                && reply.client_seq >= executing.client_seq
            {
                self.executing.pop_front();
                continue;
            }

            if self.num_skip > 0 {
                self.num_skip -= 1;
                self.executing.pop_front();
                continue;
            }

            if !self.fetch_keys.is_empty() || self.bumping {
                break;
            }

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
                    return self.proceed(since_start);
                }
                DataShardingExecuteOutput::Complete(res, writes) => {
                    self.execute_latencies += executing.start.elapsed().as_nanos() as u64;
                    let executing = self.executing.pop_front().unwrap();

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
                    assert!(!self.bumping);
                    self.bumping = true;

                    let reply = Reply {
                        client_seq: executing.client_seq,
                        res,
                        metadata: executing.metadata,
                    };
                    self.replies.insert(executing.client_id, reply.clone());
                    return Proceed::Send(Send::Reply(executing.client_id, reply));
                }
            }
        }

        Proceed::Pending(earliest_tick_after)
    }

    type Message = Message<Request<A::Op>, <Self as ServiceState<A>>::ServiceMessage>;
    fn receive(&mut self, message: Self::Message) {
        match message {
            Message::Request(request) => match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => {}
                Some(reply) if reply.client_seq == request.client_seq => {
                    self.resend_replies.push((request.client_id, reply.clone()))
                }
                _ => self.replication.submit(BigServiceLog::Request(request)),
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
                num_cached_value: configs.get("big.num-cached-value")?,
                executing_buffer_size: configs.get("big.num-max-will-fetch")?,
            })
        }
    }
}
