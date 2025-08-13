use std::{
    collections::{HashMap, HashSet, VecDeque},
    mem::take,
    time::Duration,
};

use crate::{
    Never,
    replication::ReplicationState,
    state::{Proceed, State, earliest},
};

use super::{
    ClientId, ClientSeq, Message, Reply, Request, Send, ServiceApp, ServiceIndex, ServiceState,
};

pub mod app;

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

pub struct Service<A: DataShardingApp, S: State, R: ReplicationState<Request<A::Op>>> {
    app: A,
    storage: S,
    replication: R,

    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    replicated: VecDeque<Replicated<A, R>>,
    shards: HashMap<ShardIndex, A::Shard>,

    submit_buffer: Vec<Request<A::Op>>,
    send_buffer: Vec<Send<Reply<A::Res, R::Metadata>, ServiceSend<S, R>>>,
}

type Replicated<A, R> = (
    VecDeque<Executing<A>>,
    <R as ReplicationState<Request<<A as ServiceApp>::Op>>>::Metadata,
);

struct Executing<A: DataShardingApp> {
    execute: A::Execute,
    client_id: ClientId,
    client_seq: ClientSeq,
}

pub enum ServiceSend<S: State, R: State> {
    Storage(S::Send),
    Replication(R::Send),
}

pub enum ServiceMessage<S: State, R: State> {
    Storage(S::Message),
    Replication(R::Message),
}

impl<A: DataShardingApp, S: StorageState<A::Shard>, R: ReplicationState<Request<A::Op>>>
    ServiceState<A> for Service<A, S, R>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type ServiceSend = ServiceSend<S, R>;
    type ServiceMessage = ServiceMessage<S, R>;
    type Metadata = R::Metadata;
}

impl<A: DataShardingApp, S: StorageState<A::Shard>, R: ReplicationState<Request<A::Op>>> State
    for Service<A, S, R>
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

            match executing.execute.proceed(&mut self.shards) {
                DataShardingExecuteOutput::RequireAccess(required_indices) => {
                    for shard_index in required_indices {
                        assert!(!self.shards.contains_key(&shard_index));
                        self.storage.fetch(shard_index);
                    }
                }
                DataShardingExecuteOutput::Complete(res) => {
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

        let storage_tick_after;
        match self.storage.proceed(since_start) {
            Proceed::Pending(tick_after) => storage_tick_after = tick_after,
            Proceed::Send(send) => {
                return Proceed::Send(Send::Intermediate(ServiceSend::Storage(send)));
            }
            Proceed::Output(StorageStateOutput::Fetched(shard_index, shard)) => {
                self.shards.insert(shard_index, shard);
                return self.proceed(since_start);
            }
            Proceed::Output(StorageStateOutput::Skipped(_)) => {
                todo!()
            }
        }

        while let Some(request) = self.submit_buffer.pop() {
            self.replication.submit(request)
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
                for request in replicated.logs {
                    // may query ahead here as an optimization
                    executing_buffer.push_back(Executing {
                        execute: self.app.new_execute(request.op),
                        client_id: request.client_id,
                        client_seq: request.client_seq,
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

type StateVersion = u64;

pub mod message {
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
