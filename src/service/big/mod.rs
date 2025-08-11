use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use rand::{SeedableRng, rngs::StdRng, seq::IteratorRandom};

use crate::{
    Never,
    replication::ReplicationState,
    state::{Proceed, State},
};

use super::{ClientId, Reply, Request};

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
    ) -> PartialStateExecuteOutput<A::Res>;
}

pub enum PartialStateExecuteOutput<R> {
    RequireAccess(HashSet<ShardIndex>),
    Complete(R),
}

pub type ServiceIndex = u16;
type StateVersion = u64;

pub struct Service<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> {
    replication: R,

    state: StateManager<A>,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    request_buffer: VecDeque<(Request<A::Op>, R::Metadata)>,

    send_buffer: Vec<ServiceSend<R, A>>,
}

impl<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> Service<R, A> {
    pub fn new(replication: R, app: A, index: ServiceIndex, state_config: StateConfig) -> Self {
        Self {
            replication,
            state: StateManager::new(app, index, state_config),
            replies: Default::default(),
            request_buffer: Default::default(),
            send_buffer: Default::default(),
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
    Service(ServiceIndex),
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
{
    type Send = ServiceSend<R, A>;
    type Output = Never;

    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        Proceed::Pending(None)
    }

    type Message = ServiceMessage<R, A>;
    fn receive(&mut self, message: Self::Message) {
        match message {
            ServiceMessage::Replication(metadata) => self.replication.receive(metadata),
            ServiceMessage::Request(request) => match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => {}
                Some(reply) if reply.client_seq == request.client_seq => self
                    .send_buffer
                    .push(ServiceSend::Reply(request.client_id, reply.clone())),
                _ => self.replication.submit(request),
            },
            ServiceMessage::Service(message) => match message {
                Message::QueryShard(query_shard) => {}
                Message::QueryShardOk(query_shard_ok) => {
                    if query_shard_ok.state_version < self.state.version {
                        return;
                    }
                    if query_shard_ok.state_version > self.state.version {
                        // should not happen
                        return;
                    }
                    //
                    self.update_app()
                }
            },
        };
    }
}

impl<R: ReplicationState<Request<A::Op>>, A: DataShardingApp> Service<R, A>
where
    Reply<A::Res, R::Metadata>: Clone,
{
    fn update_app(&mut self) {
        while let Some((request, _)) = self.request_buffer.front() {
            //
        }
    }
}

struct StateManager<A: DataShardingApp> {
    app: A,

    index: ServiceIndex,
    config: StateConfig,

    version: StateVersion,                           // of shards[-1]
    shard_store: Vec<HashMap<ShardIndex, A::Shard>>, // [version -> shards]
}

pub struct StateConfig {
    num_service: ServiceIndex,
    num_active_copy: usize,
}

impl<A: DataShardingApp> StateManager<A> {
    fn new(app: A, index: ServiceIndex, config: StateConfig) -> Self {
        let mut state = Self {
            app,
            index,
            config,
            shard_store: Vec::new(),
            version: 0,
        };
        let shards = (0..state.app.num_shard())
            .filter(|&index| state.should_serve(index))
            .map(|index| (index, state.app.new_shard(index)))
            .collect();
        state.shard_store.push(shards);
        state
    }

    fn service_indices_of(&self, shard_index: ShardIndex) -> Vec<ServiceIndex> {
        let mut rng = StdRng::seed_from_u64(shard_index as _);
        (0..self.config.num_service).choose_multiple(&mut rng, self.config.num_active_copy)
    }

    fn should_serve(&self, shard_index: ShardIndex) -> bool {
        self.service_indices_of(shard_index).contains(&self.index)
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
        let state = StateManager::<app::DataShardingSchema<app::Null>>::new(
            app::DataShardingSchema::new(4),
            0,
            StateConfig {
                num_service: 100,
                num_active_copy: 7,
            },
        );
        println!("{:2?}", state.service_indices_of(0));
        println!("{:2?}", state.service_indices_of(1));
        println!("{:2?}", state.service_indices_of(2));
        println!("{:2?}", state.service_indices_of(3))
    }
}
