use std::{collections::HashMap, time::Duration};

use crate::state::{AppState, Never, Proceed, State};

pub type ClientId = u32;
pub type ClientSeq = u64;

#[derive(Debug, Clone)]
pub struct Request<Op> {
    pub client_id: ClientId,
    pub seq: ClientSeq,
    pub op: Op,
}

#[derive(Debug, Clone)]
pub struct Reply<Res, M> {
    pub seq: ClientSeq,
    pub res: Res,
    pub replication_metadata: M,
}

pub struct ReplicationOutput<Op, M> {
    pub requests: Vec<Request<Op>>,
    pub metadata: M,
}

pub trait ReplicationState<Op>: State<Output = ReplicationOutput<Op, Self::Metadata>> {
    type Metadata;

    fn submit(&mut self, request: Request<Op>);
}

pub struct ServiceState<R: ReplicationState<A::Op>, A: AppState> {
    replication: R,
    app: A,
    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    send_buffer: Vec<(ClientId, Reply<A::Res, R::Metadata>)>,
}

pub enum ServiceSend<Res, M, S> {
    Reply(ClientId, Reply<Res, M>),
    Replication(S),
}

pub enum ServiceMessage<Op, M> {
    Request(Request<Op>),
    Replication(M),
}

impl<R: ReplicationState<A::Op>, A: AppState> State for ServiceState<R, A>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type Send = ServiceSend<A::Res, R::Metadata, R::Send>;
    type Output = Never;
    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        if let Some((client_id, reply)) = self.send_buffer.pop() {
            return Proceed::Send(ServiceSend::Reply(client_id, reply));
        }
        match self.replication.proceed() {
            Proceed::Pending => Proceed::Pending,
            Proceed::Send(send) => Proceed::Send(ServiceSend::Replication(send)),
            Proceed::Output(output) => {
                for request in output.requests {
                    let reply = Reply {
                        seq: request.seq,
                        res: self.app.update(request.op),
                        replication_metadata: output.metadata.clone(),
                    };
                    self.replies.insert(request.client_id, reply.clone());
                    self.send_buffer.push((request.client_id, reply))
                }
                self.proceed()
            }
        }
    }

    type Message = ServiceMessage<A::Op, R::Message>;
    fn receive(&mut self, message: Self::Message) {
        let request = match message {
            ServiceMessage::Request(request) => request,
            ServiceMessage::Replication(metadata) => {
                self.replication.receive(metadata);
                return;
            }
        };
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.seq < request.seq => {}
            Some(reply) if reply.seq == request.seq => {
                self.send_buffer.push((request.client_id, reply.clone()))
            }
            _ => self.replication.submit(request),
        }
    }

    fn tick(&mut self, elapsed: Duration) {
        self.replication.tick(elapsed)
    }
}
