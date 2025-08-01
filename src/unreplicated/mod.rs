use std::{collections::BTreeMap, time::Duration};

use crate::{
    service::{ClientId, ClientSeq, ReplicationOutput, ReplicationState, Reply, Request},
    state::{AppState, Never, Proceed, State},
    workload::ClientState,
};

pub struct Client<A: AppState> {
    id: ClientId,
    config: ClientConfig,

    seq: ClientSeq,
    now: Duration,
    submits: BTreeMap<ClientSeq, ClientSubmit<A::Op>>,
    send_buffer: Vec<Request<A::Op>>,
    output_buffer: Vec<(ClientSeq, A::Res)>,
}

pub struct ClientConfig {
    timeout: Duration,
    tick_resolution: Duration,
}

struct ClientSubmit<Op> {
    #[allow(unused)]
    op: Op,
    timeout_at: Duration,
}

impl<A: AppState> Client<A> {
    pub fn new(id: ClientId, config: ClientConfig) -> Self {
        Self {
            id,
            config,
            seq: 0,
            now: Duration::ZERO,
            submits: Default::default(),
            send_buffer: Default::default(),
            output_buffer: Default::default(),
        }
    }

    fn tick_at(&self) -> Duration {
        let mut tick_at = self.config.tick_resolution;
        if let Some((_, submit)) = self.submits.first_key_value() {
            tick_at = tick_at.min(submit.timeout_at - self.now)
        }
        tick_at
    }
}

impl<A: AppState> ClientState<A::Op> for Client<A>
where
    A::Op: Clone,
{
    fn submit(&mut self, op: A::Op) -> ClientSeq {
        self.seq += 1;
        self.submits.insert(
            self.seq,
            ClientSubmit {
                op: op.clone(),
                timeout_at: self.now + self.config.timeout + self.config.tick_resolution,
            },
        );
        self.send_buffer.push(Request {
            client_id: self.id,
            seq: self.seq,
            op,
        });
        self.seq
    }
}

impl<A: AppState> State for Client<A> {
    type Send = Request<A::Op>;
    type Output = (ClientSeq, A::Res);
    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        match self.output_buffer.pop() {
            Some(output) => Proceed::Output(output),
            None => match self.send_buffer.pop() {
                Some(req) => Proceed::Send(req),
                None => Proceed::Pending(Some(self.tick_at())),
            },
        }
    }

    type Message = Reply<A::Res, ()>;
    fn receive(&mut self, message: Self::Message) {
        if self.submits.remove(&message.seq).is_none() {
            return;
        };
        self.output_buffer.push((message.seq, message.res))
    }

    fn tick(&mut self, elapsed: Duration) {
        self.now += elapsed;
        while let Some((_, submit)) = self.submits.first_key_value()
            && submit.timeout_at <= self.now
        {
            self.submits.pop_first();
        }
    }
}

pub struct Replica<A: AppState> {
    output_buffer: Vec<ReplicationOutput<A::Op, ()>>,
}

impl<A: AppState> ReplicationState<A::Op> for Replica<A> {
    type Metadata = ();

    fn submit(&mut self, request: Request<A::Op>) {
        self.output_buffer.push(ReplicationOutput {
            requests: vec![request],
            metadata: (),
        })
    }
}

impl<A: AppState> State for Replica<A> {
    type Send = Never;
    type Output = ReplicationOutput<A::Op, ()>;
    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        match self.output_buffer.pop() {
            Some(output) => Proceed::Output(output),
            None => Proceed::Pending(None),
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }

    fn tick(&mut self, _elapsed: Duration) {}
}
