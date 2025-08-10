use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::Duration,
};

use crate::{
    Never,
    replication::ReplicationState,
    service::{ClientId, Reply},
    state::{Proceed, State},
};

use super::Request;

pub mod app;

pub type ShardIndex = u32;

pub trait ShardedStateApp: Sized {
    type Op;
    type Res;
    type Shard;
    type Execute: PartialStateExecute<Self>;
    fn new_shard(&self, index: ShardIndex) -> Self::Shard;
    fn new_execute(&self, op: Self::Op) -> Self::Execute;
}

pub trait PartialStateExecute<A: ShardedStateApp> {
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
    reordered_pushes: HashMap<StateVersion, Vec<message::PushShard<A>>>,
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
            reordered_pushes: Default::default(),
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
    PushShard(message::PushShard<A>),
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
                Message::PushShard(push_shard) => {
                    if push_shard.state_version < self.state.version {
                        return;
                    }
                    if push_shard.state_version > self.state.version {
                        self.reordered_pushes
                            .entry(push_shard.state_version)
                            .or_default()
                            .push(push_shard);
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
    use super::{ShardIndex, StateVersion};

    pub struct PushShard<S> {
        pub state_version: StateVersion,
        pub shard_index: ShardIndex,
        pub shard: S,
    }
}
