use std::{collections::BTreeMap, time::Duration};

use crate::{
    Never,
    app::AppState,
    service::{ClientId, ClientSeq, Reply, Request},
    state::{Proceed, State},
    workload::ClientState,
};

use super::{Replicated, ReplicationRecipient, ReplicationState};

pub struct Client<A: AppState> {
    id: ClientId,
    config: ClientConfig,

    seq: ClientSeq,
    submits: BTreeMap<ClientSeq, SubmitData<A>>,

    submit_buffer: Vec<(ClientSeq, A::Op)>,
    receive_buffer: Vec<Reply<A::Res, ()>>,
}

pub struct ClientConfig {
    timeout: Duration,
    // resend interval
}

struct SubmitData<A: AppState> {
    #[allow(unused)]
    op: A::Op,
    timeout_at: Duration,
}

impl<A: AppState> Client<A> {
    pub fn new(id: ClientId, config: ClientConfig) -> Self {
        Self {
            id,
            config,
            seq: 0,
            submits: Default::default(),
            submit_buffer: Default::default(),
            receive_buffer: Default::default(),
        }
    }
}

impl<A: AppState> ClientState<A> for Client<A>
where
    A::Op: Clone,
{
    fn submit(&mut self, op: A::Op) -> ClientSeq {
        self.seq += 1;
        self.submit_buffer.push((self.seq, op));
        self.seq
    }
}

impl<A: AppState> State for Client<A>
where
    A::Op: Clone,
{
    type Send = (ReplicationRecipient, Request<A::Op>);
    type Output = (ClientSeq, A::Res);
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(reply) = self.receive_buffer.pop() {
            match self.submits.remove(&reply.client_seq) {
                Some(_) => return Proceed::Output((reply.client_seq, reply.res)),
                None => return self.proceed(since_start),
            }
        }

        if let Some((seq, op)) = self.submit_buffer.pop() {
            self.submits.insert(
                seq,
                SubmitData {
                    op: op.clone(),
                    timeout_at: since_start + self.config.timeout,
                },
            );
            let request = Request {
                client_id: self.id,
                client_seq: seq,
                op,
            };
            return Proceed::Send((ReplicationRecipient::Index(0), request));
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
        self.receive_buffer.push(message)
    }
}

pub struct Replica<T> {
    output_buffer: Vec<Replicated<T, ()>>,
}

impl<T> Default for Replica<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Replica<T> {
    pub fn new() -> Self {
        Self {
            output_buffer: Default::default(),
        }
    }
}

impl<T> ReplicationState<T> for Replica<T> {
    type Metadata = ();

    fn submit(&mut self, entry: T) {
        self.output_buffer.push(Replicated {
            logs: vec![entry],
            metadata: (),
        })
    }
}

impl<T> State for Replica<T> {
    type Send = Never;
    type Output = Replicated<T, ()>;
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
