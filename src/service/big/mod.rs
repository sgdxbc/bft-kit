use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use crate::{
    Never,
    replication::ReplicationState,
    state::{Proceed, State},
};

use super::{ClientId, Reply, Request};

pub mod app;

pub type ShardIndex = u32;

pub trait ShardedStateApp: Sized {
    type Op;
    type Res;
    type Shard;
    type Execute: PartialStateExecuteState<Self>;
    fn new_shard(&self, index: ShardIndex) -> Self::Shard;
    fn new_execute(&self, op: Self::Op) -> Self::Execute;
}

pub trait PartialStateExecuteState<A: ShardedStateApp> {
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

pub struct Service<R: ReplicationState<Request<A::Op>>, A: ShardedStateApp> {
    replication: R,
    app: A,

    state: StateManager<A>,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    request_buffer: VecDeque<(Request<A::Op>, R::Metadata)>,

    send_buffer: Vec<ServiceSend<R, A>>,
}

impl<R: ReplicationState<Request<A::Op>>, A: ShardedStateApp> Service<R, A> {
    pub fn new(replication: R, app: A, index: ServiceIndex) -> Self {
        Self {
            replication,
            app,
            state: StateManager {
                version: 0,
                shards: Default::default(),
                index,
            },
            replies: Default::default(),
            request_buffer: Default::default(),
            send_buffer: Default::default(),
        }
    }
}

pub enum ServiceSend<R: ReplicationState<Request<A::Op>>, A: ShardedStateApp> {
    Service(ServiceRecipient, Message<A>),
    Reply(ClientId, Reply<A::Res, R::Metadata>),
    Replication(R::Send),
}

pub enum ServiceRecipient {
    // some message is only interested by 2f+k services. in that case just send to
    // every other service use All
    All,
    Service(ServiceIndex),
}

pub enum ServiceMessage<R: ReplicationState<Request<A::Op>>, A: ShardedStateApp> {
    Request(Request<A::Op>),
    Service(Message<A>),
    Replication(R::Message),
}

pub enum Message<A: ShardedStateApp> {
    QueryShard(message::QueryShard),
    QueryShardOk(message::QueryShardOk<A::Shard>),
}

impl<R: ReplicationState<Request<A::Op>>, A: ShardedStateApp> State for Service<R, A>
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

impl<R: ReplicationState<Request<A::Op>>, A: ShardedStateApp> Service<R, A>
where
    Reply<A::Res, R::Metadata>: Clone,
{
    fn update_app(&mut self) {
        while let Some((request, _)) = self.request_buffer.front() {
            //
        }
    }
}

struct StateManager<S> {
    version: StateVersion, // of shards[0]
    shards: Vec<HashMap<ShardIndex, S>>,
    index: ServiceIndex,
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
