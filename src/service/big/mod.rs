use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    time::Duration,
};

use derive_where::derive_where;
use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};

use crate::{
    Never,
    replication::ReplicationState,
    state::{Proceed, State},
};

use super::{
    ClientId, ClientSeq, Message, Reply, Request, Send, ServiceApp, ServiceIndex, ServiceRecipient,
    ServiceState,
};

pub mod app;
#[cfg(test)]
mod tests;

pub type ShardIndex = u32;

pub trait DataShardingApp: ServiceApp + Sized {
    type Shard;
    type Execute: DataShardingExecuteState<Self>;
    fn new_shard(&self, index: ShardIndex) -> Self::Shard;
    fn new_execute(&self, op: Self::Op) -> Self::Execute;
    fn num_shard(&self) -> ShardIndex;
}

pub trait DataShardingExecuteState<A: DataShardingApp> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, A::Shard>,
    ) -> DataShardingExecuteOutput<A::Res>;
}

pub enum DataShardingExecuteOutput<R> {
    RequireAccess(HashSet<ShardIndex>),
    Complete(R),
}

type StateVersion = u64;

pub struct Service<A: DataShardingApp, R: ReplicationState<Request<A::Op>>> {
    app: A,
    replication: R,

    state: StateManager<A>,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    executing: VecDeque<Executing<R::Metadata>>,
    querying: HashMap<StateVersion, HashSet<ShardIndex>>,

    request_buffer: Vec<Request<A::Op>>,
    query_shard_buffer: Vec<message::QueryShard>,
    query_shard_ok_buffer: Vec<message::QueryShardOk<A::Shard>>,
    send_buffer: Vec<Send<Reply<A::Res, R::Metadata>, ServiceSend<A, R>>>,
}

struct Executing<RD> {
    client_id: ClientId,
    client_seq: ClientSeq,
    metadata: RD,
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>> Service<A, R> {
    pub fn new(replication: R, app: A, index: ServiceIndex, state_config: StateConfig) -> Self {
        Self {
            replication,
            state: StateManager::new(index, state_config, &app),
            app,
            replies: Default::default(),
            executing: Default::default(),
            querying: Default::default(),
            request_buffer: Default::default(),
            query_shard_buffer: Default::default(),
            query_shard_ok_buffer: Default::default(),
            send_buffer: Default::default(),
        }
    }
}

pub enum ServiceSend<A: DataShardingApp, R: ReplicationState<Request<A::Op>>> {
    Service(ServiceRecipient, ServiceMessage<A>),
    Replication(R::Send),
}

#[derive_where(Debug, Clone; A::Shard)]
pub enum ServiceMessage<A: DataShardingApp> {
    QueryShard(message::QueryShard),
    QueryShardOk(message::QueryShardOk<A::Shard>),
}

pub enum ToServiceMessage<A: DataShardingApp, R: ReplicationState<Request<A::Op>>> {
    Service(ServiceMessage<A>),
    Replication(R::Message),
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>> ServiceState<A> for Service<A, R>
where
    Reply<A::Res, R::Metadata>: Clone,
    A::Shard: Clone,
    R::Metadata: Clone,
{
    type ServiceSend = ServiceSend<A, R>;
    type ServiceMessage = ToServiceMessage<A, R>;
    type Metadata = R::Metadata;
}

impl<A: DataShardingApp, R: ReplicationState<Request<A::Op>>> State for Service<A, R>
where
    Reply<A::Res, R::Metadata>: Clone,
    A::Shard: Clone,
    R::Metadata: Clone,
{
    type Send = Send<Reply<A::Res, R::Metadata>, ServiceSend<A, R>>;
    type Output = Never;

    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(send) = self.send_buffer.pop() {
            return Proceed::Send(send);
        }
        if let Some(proceed) = self.state.proceed() {
            return match proceed {
                DataShardingExecuteOutput::RequireAccess(required_indices) => {
                    let querying = self.querying.entry(self.state.version).or_default();
                    for shard_index in required_indices {
                        if querying.insert(shard_index) {
                            let query_shard = message::QueryShard {
                                state_version: self.state.version,
                                shard_index,
                                service_index: self.state.index,
                            };
                            self.send_buffer.push(Send::Service(ServiceSend::Service(
                                ServiceRecipient::Multi(
                                    self.state.config.service_indices_of(shard_index),
                                ),
                                ServiceMessage::QueryShard(query_shard),
                            )));
                        }
                    }
                    self.proceed(since_start)
                }
                DataShardingExecuteOutput::Complete(res) => {
                    let executing = self.executing.pop_front().unwrap();
                    Proceed::Send(Send::Reply(
                        executing.client_id,
                        Reply {
                            client_seq: executing.client_seq,
                            metadata: executing.metadata,
                            res,
                        },
                    ))
                }
            };
        }
        while let Some(query_shard_ok) = self.query_shard_ok_buffer.pop() {
            assert!(query_shard_ok.state_version == self.state.version);
            if query_shard_ok.state_version > self.state.version {
                todo!()
            }
            self.state
                .install_shard(query_shard_ok.shard_index, query_shard_ok.shard);
            let querying = self
                .querying
                .get_mut(&query_shard_ok.state_version)
                .unwrap();
            querying.remove(&query_shard_ok.shard_index);
            if querying.is_empty() {
                self.querying.remove(&query_shard_ok.state_version);
                return self.proceed(since_start); // `state` will proceed
            }
        }
        if let Some(query_shard) = self.query_shard_buffer.pop() {
            // TODO check whether a higher bump certificate is available
            return match self
                .state
                .query_shard(query_shard.state_version, query_shard.shard_index)
            {
                None => self.proceed(since_start),
                Some(shard) => {
                    let query_shard_ok = message::QueryShardOk {
                        state_version: query_shard.state_version,
                        shard_index: query_shard.shard_index,
                        shard: shard.clone(),
                    };
                    Proceed::Send(Send::Service(ServiceSend::Service(
                        ServiceRecipient::Uni(query_shard.service_index),
                        ServiceMessage::QueryShardOk(query_shard_ok),
                    )))
                }
            };
        }
        while let Some(request) = self.request_buffer.pop() {
            self.replication.submit(request)
        }
        match self.replication.proceed(since_start) {
            Proceed::Send(message) => {
                Proceed::Send(Send::Service(ServiceSend::Replication(message)))
            }
            Proceed::Pending(tick_after) => Proceed::Pending(tick_after), // TODO
            Proceed::Output(output) => {
                for request in output.logs {
                    self.state.push_execute(self.app.new_execute(request.op));
                    let executing = Executing {
                        client_id: request.client_id,
                        client_seq: request.client_seq,
                        metadata: output.metadata.clone(),
                    };
                    self.executing.push_back(executing)
                }
                self.proceed(since_start)
            }
        }
    }

    type Message = Message<Request<A::Op>, ToServiceMessage<A, R>>;
    fn receive(&mut self, message: Self::Message) {
        match message {
            Message::Request(request) => match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => {}
                Some(reply) if reply.client_seq == request.client_seq => self
                    .send_buffer
                    .push(Send::Reply(request.client_id, reply.clone())),
                _ => self.request_buffer.push(request),
            },
            Message::Service(ToServiceMessage::Replication(metadata)) => {
                self.replication.receive(metadata)
            }
            Message::Service(ToServiceMessage::Service(ServiceMessage::QueryShard(
                query_shard,
            ))) => {
                if query_shard.state_version > self.state.version {
                    // TODO buffer it?
                    return;
                }
                self.query_shard_buffer.push(query_shard)
            }
            Message::Service(ToServiceMessage::Service(ServiceMessage::QueryShardOk(
                query_shard_ok,
            ))) => {
                if query_shard_ok.state_version < self.state.version {
                    return;
                }
                if let Some(querying) = self.querying.get(&query_shard_ok.state_version)
                    && querying.contains(&query_shard_ok.shard_index)
                {
                    self.query_shard_ok_buffer.push(query_shard_ok);
                }
            }
        };
    }
}

struct StateManager<A: DataShardingApp> {
    index: ServiceIndex,
    config: StateConfig,

    version: StateVersion,                           // of shard_store[-1]
    shard_store: Vec<HashMap<ShardIndex, A::Shard>>, // [version -> shards]
    executes: VecDeque<A::Execute>,
    // shards of `version` that are required by executions[0]
    // if executions[0].proceed() returns Complete, these shards become shards of
    // `version + 1`
    // for the shards that this service `should_serve`, this map keeps copies of
    // them for the `execute` to mutate
    execute: HashMap<ShardIndex, A::Shard>,
}

pub struct StateConfig {
    num_service: ServiceIndex,
    num_active_copy: usize,
}

impl StateConfig {
    fn service_indices_of(&self, shard_index: ShardIndex) -> Vec<ServiceIndex> {
        let mut rng = StdRng::seed_from_u64(shard_index as _);
        (0..self.num_service).choose_multiple(&mut rng, self.num_active_copy)
    }

    fn should_serve(&self, service_index: ServiceIndex, shard_index: ShardIndex) -> bool {
        self.service_indices_of(shard_index)
            .contains(&service_index)
    }
}

impl<A: DataShardingApp> StateManager<A> {
    fn new(index: ServiceIndex, config: StateConfig, app: &A) -> Self {
        let shards = (0..app.num_shard())
            .filter(|&shard_index| config.should_serve(index, shard_index))
            .map(|shard_index| (shard_index, app.new_shard(shard_index)))
            .collect();
        Self {
            index,
            config,
            version: 0,
            shard_store: vec![shards],
            executes: Default::default(),
            execute: Default::default(),
        }
    }

    fn push_execute(&mut self, execute: A::Execute) {
        self.executes.push_back(execute)
    }

    fn install_shard(&mut self, index: ShardIndex, shard: A::Shard) {
        self.execute.insert(index, shard);
    }

    fn proceed(&mut self) -> Option<DataShardingExecuteOutput<A::Res>>
    where
        A::Shard: Clone,
    {
        let execute = self.executes.front_mut()?;
        match execute.proceed(&mut self.execute) {
            DataShardingExecuteOutput::RequireAccess(required_indices) => {
                let mut missing_indices = HashSet::new();
                for index in required_indices {
                    if self.config.should_serve(self.index, index) {
                        let shard = self.query_shard(self.version, index).unwrap().clone();
                        self.execute.insert(index, shard);
                    } else {
                        missing_indices.insert(index);
                    }
                }
                if !missing_indices.is_empty() {
                    Some(DataShardingExecuteOutput::RequireAccess(missing_indices))
                } else {
                    self.proceed() // the `execute` will proceed in this recursion
                }
            }
            DataShardingExecuteOutput::Complete(res) => {
                self.executes.pop_front();
                self.execute
                    .retain(|&shard_index, _| self.config.should_serve(self.index, shard_index));
                self.shard_store.push(take(&mut self.execute));
                self.version += 1;
                Some(DataShardingExecuteOutput::Complete(res))
            }
        }
    }

    // the lowest version that has not been cleared
    fn first_version(&self) -> StateVersion {
        self.version - (self.shard_store.len() - 1) as StateVersion
    }

    fn query_shard(&self, version: StateVersion, index: ShardIndex) -> Option<&A::Shard> {
        assert!(version <= self.version);
        assert!(self.config.should_serve(self.index, index));
        if version < self.first_version() {
            return None;
        }
        for shards in self.shard_store[..=(version - self.first_version()) as usize]
            .iter()
            .rev()
        {
            if let Some(shard) = shards.get(&index) {
                return Some(shard);
            }
        }
        unimplemented!()
    }
}

mod message {
    use super::{ServiceIndex, ShardIndex, StateVersion};

    #[derive(Debug, Clone)]
    pub struct QueryShard {
        pub state_version: StateVersion,
        pub shard_index: ShardIndex,
        pub service_index: ServiceIndex,
    }

    #[derive(Debug, Clone)]
    pub struct QueryShardOk<S> {
        pub state_version: StateVersion,
        pub shard_index: ShardIndex,
        pub shard: S,
    }
}
