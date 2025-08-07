use std::{collections::BTreeMap, time::Duration};

use crate::{
    app::AppState,
    replication::{ReplicaIndex, Replicated, ReplicationState},
    service::{ClientId, ClientSeq, Reply, Request},
    state::{Never, Proceed, State},
    workload::ClientState,
};

pub struct Client<A: AppState> {
    id: ClientId,
    config: ClientConfig,

    seq: ClientSeq,
    submits: BTreeMap<ClientSeq, ClientSubmit<A::Op>>,
    send_buffer: Vec<Request<A::Op>>,
    output_buffer: Vec<(ClientSeq, A::Res)>,
}

pub struct ClientConfig {
    timeout: Duration,
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
            submits: Default::default(),
            send_buffer: Default::default(),
            output_buffer: Default::default(),
        }
    }
}

impl<A: AppState> ClientState<A::Op> for Client<A>
where
    A::Op: Clone,
{
    fn submit(&mut self, op: A::Op, at: Duration) -> ClientSeq {
        self.seq += 1;
        self.submits.insert(
            self.seq,
            ClientSubmit {
                op: op.clone(),
                timeout_at: at + self.config.timeout,
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
    type Send = (ReplicaIndex, Request<A::Op>);
    type Output = (ClientSeq, A::Res);
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(output) = self.output_buffer.pop() {
            return Proceed::Output(output);
        }
        if let Some(req) = self.send_buffer.pop() {
            return Proceed::Send((0, req));
        }
        loop {
            let Some((&seq, submit)) = self.submits.first_key_value() else {
                break Proceed::Pending(None);
            };
            if submit.timeout_at <= since_start {
                self.submits.remove(&seq);
            } else {
                break Proceed::Pending(Some(submit.timeout_at - since_start));
            }
        }
    }

    type Message = Reply<A::Res, ()>;
    fn receive(&mut self, message: Self::Message) {
        if self.submits.remove(&message.seq).is_none() {
            return;
        };
        self.output_buffer.push((message.seq, message.res))
    }
}

pub struct Replica<A: AppState, E> {
    output_buffer: Vec<Replicated<A::Op, (), E>>,
}

impl<A: AppState, E> Default for Replica<A, E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<A: AppState, E> Replica<A, E> {
    pub fn new() -> Self {
        Self {
            output_buffer: Default::default(),
        }
    }
}

impl<A: AppState, E> ReplicationState<A::Op, E> for Replica<A, E> {
    type Metadata = ();

    fn submit(&mut self, request: Request<A::Op>) {
        self.output_buffer
            .push(Replicated::Block(vec![request], ()))
    }

    fn trigger(&mut self, event: E) {
        self.output_buffer.push(Replicated::Event(event))
    }
}

impl<A: AppState, E> State for Replica<A, E> {
    type Send = Never;
    type Output = Replicated<A::Op, (), E>;
    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.output_buffer.pop() {
            Some(output) => Proceed::Output(output),
            None => Proceed::Pending(None),
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}

mod parse {
    use std::time::Duration;

    use crate::parse::{Extract, Settings};

    impl Extract for super::ClientConfig {
        fn extract(settings: &Settings) -> anyhow::Result<Self> {
            Ok(Self {
                timeout: Duration::from_secs_f32(settings.get("client.timeout")?),
            })
        }
    }
}
