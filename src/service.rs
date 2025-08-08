use std::{collections::HashMap, time::Duration};

use bincode::{Decode, Encode};

use crate::{
    Never,
    app::AppState,
    replication::ReplicationState,
    state::{Proceed, State},
};

pub mod big;
pub mod transport;

// id is randomly assigned while index is continuously assigned
// index is statically assigned while sequence monotonically increases

pub type ClientId = u32;
pub type ClientSeq = u64;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Request<Op> {
    pub client_id: ClientId,
    pub client_seq: ClientSeq,
    pub op: Op,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Reply<Res, M> {
    pub client_seq: ClientSeq,
    pub res: Res,
    pub replication_metadata: M,
}

pub struct Service<R: ReplicationState<Request<A::Op>>, A: AppState> {
    replication: R,
    app: A,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    #[allow(clippy::type_complexity)]
    send_buffer: Vec<(ClientId, Reply<A::Res, R::Metadata>)>,
}

impl<R: ReplicationState<Request<A::Op>>, A: AppState> Service<R, A> {
    pub fn new(replication: R, app: A) -> Self {
        Self {
            replication,
            app,
            replies: Default::default(),
            send_buffer: Default::default(),
        }
    }
}

pub enum ServiceSend<Res, M, S> {
    Reply(ClientId, Reply<Res, M>),
    Replication(S),
}

pub enum ServiceMessage<Op, M> {
    Request(Request<Op>),
    Replication(M),
}

impl<R: ReplicationState<Request<A::Op>>, A: AppState> State for Service<R, A>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
    // ServiceMessage<R, A>: std::fmt::Debug,
{
    type Send = ServiceSend<A::Res, R::Metadata, R::Send>;
    type Output = Never;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some((client_id, reply)) = self.send_buffer.pop() {
            return Proceed::Send(ServiceSend::Reply(client_id, reply));
        }
        match self.replication.proceed(since_start) {
            Proceed::Pending(tick_after) => Proceed::Pending(tick_after),
            Proceed::Send(send) => Proceed::Send(ServiceSend::Replication(send)),
            Proceed::Output(replicated) => {
                for request in replicated.block {
                    if self
                        .replies
                        .get(&request.client_id)
                        .is_some_and(|reply| reply.client_seq >= request.client_seq)
                    {
                        continue;
                    }
                    let reply = Reply {
                        client_seq: request.client_seq,
                        res: self.app.update(&request.op),
                        replication_metadata: replicated.metadata.clone(),
                    };
                    self.replies.insert(request.client_id, reply.clone());
                    self.send_buffer.push((request.client_id, reply))
                }
                self.proceed(since_start)
            }
        }
    }

    type Message = ServiceMessage<A::Op, R::Message>;
    fn receive(&mut self, message: Self::Message) {
        // dbg!(&message);
        let request = match message {
            ServiceMessage::Request(request) => request,
            ServiceMessage::Replication(metadata) => return self.replication.receive(metadata),
        };
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.client_seq > request.client_seq => {}
            Some(reply) if reply.client_seq == request.client_seq => {
                self.send_buffer.push((request.client_id, reply.clone()))
            }
            _ => self.replication.submit(request),
        }
    }
}
