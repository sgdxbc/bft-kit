use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    time::Duration,
};

use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};

use crate::{
    Never,
    replication::ReplicationState,
    state::{Proceed, State},
};

use super::{ClientId, ClientSeq, Reply, Request};

pub mod app;

pub type ShardIndex = u32;

pub trait DataShardingApp: Sized {
    type Op;
    type Res;
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

pub type ServiceIndex = u16;
type StateVersion = u64;

pub struct Service<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> {
    replication: R,

    state: StateManager<A>,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    executing: VecDeque<Executing<R::Metadata>>,
    querying: HashMap<StateVersion, HashSet<ShardIndex>>,

    request_buffer: Vec<Request<A::Op>>,
    query_shard_buffer: Vec<message::QueryShard>,
    query_shard_ok_buffer: Vec<message::QueryShardOk<A::Shard>>,
    output_buffer: Vec<Proceed<ServiceSend<R, A>, Never>>,
}

struct Executing<RD> {
    client_id: ClientId,
    client_seq: ClientSeq,
    metadata: RD,
}

impl<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> Service<R, A> {
    pub fn new(replication: R, app: A, index: ServiceIndex, state_config: StateConfig) -> Self {
        Self {
            replication,
            state: StateManager::new(app, index, state_config),
            replies: Default::default(),
            executing: Default::default(),
            querying: Default::default(),
            request_buffer: Default::default(),
            query_shard_buffer: Default::default(),
            query_shard_ok_buffer: Default::default(),
            output_buffer: Default::default(),
        }
    }
}

pub enum ServiceSend<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> {
    Service(ServiceRecipient, Message<A>),
    Reply(ClientId, Reply<A::Res, R::Metadata>),
    Replication(R::Send),
}

pub enum ServiceRecipient {
    // some message is only interested by 2f+k services. in that case just send to
    // every other service use All. difference should not be much
    All,
    Index(ServiceIndex),
}

pub enum ServiceMessage<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> {
    Request(Request<A::Op>),
    Service(Message<A>),
    Replication(R::Message),
}

pub enum Message<A: DataShardingApp> {
    QueryShard(message::QueryShard),
    QueryShardOk(message::QueryShardOk<A::Shard>),
}

impl<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> State for Service<R, A>
where
    Reply<A::Res, R::Metadata>: Clone,
    A::Shard: Clone,
    R::Metadata: Clone,
{
    type Send = ServiceSend<R, A>;
    type Output = Never;

    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(output) = self.output_buffer.pop() {
            return output;
        }
        if let Some(proceed) = self.state.proceed() {
            return match proceed {
                DataShardingExecuteOutput::RequireAccess(required_indices) => {
                    self.querying
                        .entry(self.state.version)
                        .or_default()
                        .extend(required_indices.clone());
                    self.output_buffer
                        .extend(required_indices.into_iter().map(|shard_index| {
                            let query_shard = message::QueryShard {
                                state_version: self.state.version,
                                shard_index,
                                service_index: self.state.index,
                            };
                            Proceed::Send(ServiceSend::Service(
                                ServiceRecipient::All,
                                Message::QueryShard(query_shard),
                            ))
                        }));
                    self.proceed(since_start)
                }
                DataShardingExecuteOutput::Complete(res) => {
                    let executing = self.executing.pop_front().unwrap();
                    Proceed::Send(ServiceSend::Reply(
                        executing.client_id,
                        Reply {
                            client_seq: executing.client_seq,
                            replication_metadata: executing.metadata,
                            res,
                        },
                    ))
                }
            };
        }
        if let Some(query_shard_ok) = self.query_shard_ok_buffer.pop() {
            assert!(query_shard_ok.state_version == self.state.version);
            self.state
                .install_shard(query_shard_ok.shard_index, query_shard_ok.shard);
            let querying = self
                .querying
                .get_mut(&query_shard_ok.state_version)
                .unwrap();
            querying.remove(&query_shard_ok.shard_index);
            if querying.is_empty() {
                self.querying.remove(&query_shard_ok.state_version);
                return self.proceed(since_start);
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
                    Proceed::Send(ServiceSend::Service(
                        ServiceRecipient::Index(query_shard.service_index),
                        Message::QueryShardOk(query_shard_ok),
                    ))
                }
            };
        }
        if let Some(request) = self.request_buffer.pop() {
            return match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => self.proceed(since_start),
                Some(reply) if reply.client_seq == request.client_seq => {
                    Proceed::Send(ServiceSend::Reply(request.client_id, reply.clone()))
                }
                _ => {
                    self.replication.submit(request);
                    self.proceed(since_start)
                }
            };
        }
        match self.replication.proceed(since_start) {
            Proceed::Send(message) => Proceed::Send(ServiceSend::Replication(message)),
            Proceed::Pending(tick_after) => Proceed::Pending(tick_after), // TODO
            Proceed::Output(output) => {
                for request in output.logs {
                    self.state.push_op(request.op);
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

    type Message = ServiceMessage<R, A>;
    fn receive(&mut self, message: Self::Message) {
        match message {
            ServiceMessage::Replication(metadata) => self.replication.receive(metadata),
            ServiceMessage::Request(request) => self.request_buffer.push(request),
            ServiceMessage::Service(message) => match message {
                Message::QueryShard(query_shard) => {
                    if query_shard.state_version > self.state.version {
                        // TODO buffer it?
                        return;
                    }
                    self.query_shard_buffer.push(query_shard)
                }
                Message::QueryShardOk(query_shard_ok) => {
                    if query_shard_ok.state_version < self.state.version {
                        return;
                    }
                    if query_shard_ok.state_version > self.state.version {
                        // should not happen
                        return;
                    }
                    self.query_shard_ok_buffer.push(query_shard_ok);
                }
            },
        };
    }
}

struct StateManager<A: DataShardingApp> {
    app: A,

    index: ServiceIndex,
    config: StateConfig,

    version: StateVersion,                           // of shard_store[-1]
    shard_store: Vec<HashMap<ShardIndex, A::Shard>>, // [version -> shards]
    executions: VecDeque<A::Execute>,
    // shards of `version` that are required by executions[0]
    // if executions[0].proceed() returns Complete, these shards become shards of
    // `version + 1`
    // for the shards that this service `should_serve`, this map keeps copies of
    // them for the execution to mutate
    execution_shards: HashMap<ShardIndex, A::Shard>,
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
    fn new(app: A, index: ServiceIndex, config: StateConfig) -> Self {
        let shards = (0..app.num_shard())
            .filter(|&shard_index| config.should_serve(index, shard_index))
            .map(|shard_index| (shard_index, app.new_shard(shard_index)))
            .collect();
        Self {
            app,
            index,
            config,
            version: 0,
            shard_store: vec![shards],
            executions: Default::default(),
            execution_shards: Default::default(),
        }
    }

    fn push_op(&mut self, op: A::Op) -> (StateVersion, HashSet<ShardIndex>) {
        let mut execution = self.app.new_execute(op);
        let DataShardingExecuteOutput::RequireAccess(required_indices) =
            execution.proceed(&mut Default::default())
        else {
            unimplemented!()
        };
        self.executions.push_back(execution);
        (
            self.version + self.executions.len() as StateVersion,
            required_indices
                .into_iter()
                .filter(|&index| !self.config.should_serve(self.index, index))
                .collect(),
        )
    }

    fn install_shard(&mut self, index: ShardIndex, shard: A::Shard) {
        self.execution_shards.insert(index, shard);
    }

    fn proceed(&mut self) -> Option<DataShardingExecuteOutput<A::Res>>
    where
        A::Shard: Clone,
    {
        let execution = self.executions.front_mut()?;
        match execution.proceed(&mut self.execution_shards) {
            DataShardingExecuteOutput::RequireAccess(required_indices) => {
                let mut missing_indices = HashSet::new();
                for index in required_indices {
                    if self.config.should_serve(self.index, index) {
                        let shard = self.query_shard(self.version, index).unwrap().clone();
                        self.execution_shards.insert(index, shard);
                    } else {
                        missing_indices.insert(index);
                    }
                }
                if !missing_indices.is_empty() {
                    Some(DataShardingExecuteOutput::RequireAccess(missing_indices))
                } else {
                    self.proceed() // the execution will proceed in this recursion
                }
            }
            DataShardingExecuteOutput::Complete(res) => {
                self.executions.pop_front();
                self.execution_shards
                    .retain(|&shard_index, _| self.config.should_serve(self.index, shard_index));
                self.shard_store.push(take(&mut self.execution_shards));
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

    pub struct QueryShard {
        pub state_version: StateVersion,
        pub shard_index: ShardIndex,
        pub service_index: ServiceIndex,
    }

    pub struct QueryShardOk<S> {
        pub state_version: StateVersion,
        pub shard_index: ShardIndex,
        pub shard: S,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_service_indices_of() {
        let config = StateConfig {
            num_service: 100,
            num_active_copy: 7,
        };
        println!("{:2?}", config.service_indices_of(0));
        println!("{:2?}", config.service_indices_of(1));
        println!("{:2?}", config.service_indices_of(2));
        println!("{:2?}", config.service_indices_of(3))
    }
}
