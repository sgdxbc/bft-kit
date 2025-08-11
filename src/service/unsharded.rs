use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};

use crate::{
    Never,
    app::AppState,
    replication::ReplicationState,
    state::{Proceed, State},
};

use super::{ClientId, Reply, Request, ServiceMessage, ServiceSend, ServiceState};

pub struct Service<A: AppState, R: ReplicationState<Request<A::Op>>> {
    app: A,
    replication: R,

    replies: HashMap<ClientId, Reply<A::Res, R::Metadata>>,
    replicated: Option<(ReplicatedRequests<A::Op>, R::Metadata)>,

    request_buffer: Vec<Request<A::Op>>,
    output_buffer: Vec<Proceed<ServiceSend<Never, A::Res, R::Metadata, R::Send>, Never>>,
}

type ReplicatedRequests<Op> = VecDeque<Request<Op>>;

impl<A: AppState, R: ReplicationState<Request<A::Op>>> Service<A, R> {
    pub fn new(app: A, replication: R) -> Self {
        Self {
            replication,
            app,
            replies: Default::default(),
            replicated: None,
            request_buffer: Default::default(),
            output_buffer: Default::default(),
        }
    }
}

impl<A: AppState, R: ReplicationState<Request<A::Op>>> ServiceState<A, R> for Service<A, R>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
{
    type Log = Request<A::Op>;
    type ServiceMessage = Never;
}

impl<A: AppState, R: ReplicationState<Request<A::Op>>> State for Service<A, R>
where
    R::Metadata: Clone,
    Reply<A::Res, R::Metadata>: Clone,
    // ServiceMessage<R, A>: std::fmt::Debug,
{
    type Send = ServiceSend<Never, A::Res, R::Metadata, R::Send>;
    type Output = Never;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some((requests, metadata)) = &mut self.replicated {
            let Some(request) = requests.pop_front() else {
                self.replicated = None;
                return self.proceed(since_start);
            };
            if let Some(reply) = self.replies.get(&request.client_id)
                && reply.client_seq >= request.client_seq
            {
                return self.proceed(since_start);
            }
            let reply = Reply {
                client_seq: request.client_seq,
                res: self.app.execute(request.op),
                replication_metadata: metadata.clone(),
            };
            self.replies.insert(request.client_id, reply.clone());
            return Proceed::Send(ServiceSend::Reply(request.client_id, reply));
        }

        while let Some(request) = self.request_buffer.pop() {
            self.replication.submit(request)
        }
        match self.replication.proceed(since_start) {
            Proceed::Pending(tick_after) => Proceed::Pending(tick_after),
            Proceed::Send(send) => Proceed::Send(ServiceSend::Replication(send)),
            Proceed::Output(replicated) => {
                let replaced = self
                    .replicated
                    .replace((replicated.logs.into(), replicated.metadata));
                assert!(replaced.is_none());
                self.proceed(since_start)
            }
        }
    }

    type Message = ServiceMessage<Never, A::Op, R::Message>;
    fn receive(&mut self, message: Self::Message) {
        // dbg!(&message);
        match message {
            ServiceMessage::Request(request) => match self.replies.get(&request.client_id) {
                Some(reply) if reply.client_seq > request.client_seq => {}
                Some(reply) if reply.client_seq == request.client_seq => self.output_buffer.push(
                    Proceed::Send(ServiceSend::Reply(request.client_id, reply.clone())),
                ),
                _ => self.request_buffer.push(request),
            },
            ServiceMessage::Replication(message) => self.replication.receive(message),
        }
    }
}
