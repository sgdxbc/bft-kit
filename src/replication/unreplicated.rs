use std::{collections::BTreeMap, mem::take, time::Duration};

use crate::{
    Never,
    app::AppProtocol,
    service::{ClientId, ClientSeq, Reply, Request},
    state::{Action, State},
    workload::ClientState,
};

use super::{Dest, Replicated, ReplicationState};

pub struct UnreplicatedClient<A: AppProtocol> {
    id: ClientId,
    config: UnreplicatedClientConfig,

    seq: ClientSeq,
    submits: BTreeMap<ClientSeq, SubmitData<A>>,

    submit_buffer: Vec<(ClientSeq, A::Op)>,
    receive_buffer: Vec<Reply<A::Res, ()>>,
}

pub struct UnreplicatedClientConfig {
    timeout: Duration,
    // resend interval
}

struct SubmitData<A: AppProtocol> {
    #[allow(unused)]
    op: A::Op,
    timeout_at: Duration,
}

impl<A: AppProtocol> UnreplicatedClient<A> {
    pub fn new(id: ClientId, config: UnreplicatedClientConfig) -> Self {
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

impl<A: AppProtocol> ClientState<A> for UnreplicatedClient<A>
where
    A::Op: Clone,
{
    fn submit(&mut self, op: A::Op) -> ClientSeq {
        self.seq += 1;
        self.submit_buffer.push((self.seq, op));
        self.seq
    }
}

impl<A: AppProtocol> State for UnreplicatedClient<A>
where
    A::Op: Clone,
{
    type Effect = (Dest, Request<A::Op>);
    type Output = (ClientSeq, A::Res);
    fn proceed(&mut self, since_start: Duration) -> Action<Self::Effect, Self::Output> {
        if let Some(reply) = self.receive_buffer.pop() {
            match self.submits.remove(&reply.client_seq) {
                Some(_) => return Action::Output((reply.client_seq, reply.res)),
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
            return Action::Perform((Dest::One(0), request));
        }

        loop {
            let Some((&seq, submit)) = self.submits.first_key_value() else {
                break Action::Pending(None);
            };
            if submit.timeout_at <= since_start {
                self.submits.remove(&seq);
            } else {
                break Action::Pending(Some(submit.timeout_at - since_start));
            }
        }
    }

    type Message = Reply<A::Res, ()>;
    fn receive(&mut self, message: Self::Message) {
        self.receive_buffer.push(message)
    }
}

pub struct UnreplicatedReplica<T> {
    submit_buffer: Vec<T>,
}

impl<T> Default for UnreplicatedReplica<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> UnreplicatedReplica<T> {
    pub fn new() -> Self {
        Self {
            submit_buffer: Default::default(),
        }
    }
}

impl<T> ReplicationState<T> for UnreplicatedReplica<T> {
    type Metadata = ();

    fn submit(&mut self, entry: T) {
        self.submit_buffer.push(entry)
    }
}

impl<T> State for UnreplicatedReplica<T> {
    type Effect = Never;
    type Output = Replicated<T, ()>;
    fn proceed(&mut self, _since_start: Duration) -> Action<Self::Effect, Self::Output> {
        if self.submit_buffer.is_empty() {
            Action::Pending(None)
        } else {
            Action::Output(Replicated {
                logs: take(&mut self.submit_buffer),
                metadata: (),
            })
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}

mod parse {
    use std::time::Duration;

    use crate::parse::{Configs, Extract};

    impl Extract for super::UnreplicatedClientConfig {
        fn extract(configs: &Configs) -> anyhow::Result<Self> {
            Ok(Self {
                timeout: Duration::from_secs_f32(configs.get("client.timeout")?),
            })
        }
    }
}
