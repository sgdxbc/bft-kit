use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use crate::{
    Never,
    app::{ShardIndex, ShardedAppState},
    replication::ReplicationState,
    service::{ClientId, Reply},
    state::{Proceed, State},
};

use super::Request;

pub type ServiceIndex = u16;
type StateVersion = u64;

pub struct Service<R: ReplicationState<A::Op>, A: ShardedAppState> {
    replication: R,
    app: A,

    index: ServiceIndex,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    state_version: StateVersion,
    state_shards: HashMap<ShardIndex, A::Shard>,
    request_buffer: VecDeque<(Request<A::Op>, R::Metadata)>,

    send_buffer: Vec<ServiceSend<R, A>>,
}

impl<R: ReplicationState<A::Op>, A: ShardedAppState> Service<R, A> {
    pub fn new(replication: R, app: A, index: ServiceIndex) -> Self {
        Self {
            replication,
            app,
            index,
            replies: Default::default(),
            state_version: 0,
            state_shards: Default::default(),
            request_buffer: Default::default(),
            send_buffer: Default::default(),
        }
    }
}

pub enum ServiceSend<R: ReplicationState<A::Op>, A: ShardedAppState> {
    Service(ServiceRecipient, ServiceMessage<A>),
    Reply(ClientId, Reply<A::Res, R::Metadata>),
    Replication(R::Send),
}

pub enum ServiceRecipient {
    // some message is only interested by 2f+k services. in that case just send to
    // every other service use All
    All,
    Service(ServiceIndex),
}

pub enum ServiceMessage<A: ShardedAppState> {
    Push(message::Push<A::Shard>),
}

impl<R: ReplicationState<A::Op>, A: ShardedAppState> State for Service<R, A> {
    type Send = ServiceSend<R, A>;
    type Output = Never;

    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        Proceed::Pending(None)
    }

    type Message = ServiceMessage<A>;
    fn receive(&mut self, message: Self::Message) {}
}

mod message {
    use crate::app::ShardIndex;

    use super::StateVersion;

    pub struct Push<S> {
        pub state_version: StateVersion,
        pub shard_index: ShardIndex,
        pub shard: S,
    }
}
