use std::{collections::BTreeMap, iter::once, mem::replace, time::Duration};

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

    now: Duration,
    seq: ClientSeq,
    submits: BTreeMap<ClientSeq, SubmitData<A>>,

    actions: Vec<Action<(Dest, Request<A::Op>), (ClientSeq, A::Res)>>,
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
            now: Duration::ZERO,
            seq: 0,
            submits: Default::default(),
            actions: Default::default(),
        }
    }
}

impl<A: AppProtocol> ClientState<A> for UnreplicatedClient<A>
where
    A::Op: Clone,
{
    fn submit(&mut self, op: A::Op) -> ClientSeq {
        tracing::trace!("submit");
        self.seq += 1;
        self.submits.insert(
            self.seq,
            SubmitData {
                op: op.clone(),
                timeout_at: self.now + self.config.timeout,
            },
        );
        let request = Request {
            client_id: self.id,
            client_seq: self.seq,
            op,
        };
        self.actions.push(Action::Send((Dest::One(0), request)));
        self.seq
    }
}

impl<A: AppProtocol> State for UnreplicatedClient<A>
where
    A::Op: Clone,
{
    type Send = (Dest, Request<A::Op>);
    type Output = (ClientSeq, A::Res);
    fn proceed(&mut self) -> Option<impl Iterator<Item = Action<Self::Send, Self::Output>>> {
        if self.actions.is_empty() {
            None
        } else {
            Some(self.actions.drain(..))
        }
    }

    type Message = Reply<A::Res, ()>;
    fn receive(&mut self, reply: Self::Message) {
        if let Some(_) = self.submits.remove(&reply.client_seq) {
            self.actions
                .push(Action::Output((reply.client_seq, reply.res)))
        }
    }

    fn tick(&mut self, since_start: Duration) {
        while let Some((&seq, submit)) = self.submits.first_key_value() {
            if submit.timeout_at <= since_start {
                self.submits.remove(&seq);
            } else {
                break;
            }
        }
        self.now = since_start
    }

    fn tick_after(&self) -> Option<Duration> {
        self.submits
            .first_key_value()
            .map(|(_, submit)| submit.timeout_at - self.now)
    }
}

pub struct UnreplicatedReplica<T> {
    output: Replicated<T, ()>,
}

impl<T> Default for UnreplicatedReplica<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> UnreplicatedReplica<T> {
    pub fn new() -> Self {
        Self {
            output: Replicated {
                logs: Default::default(),
                metadata: (),
            },
        }
    }
}

impl<T> ReplicationState<T> for UnreplicatedReplica<T> {
    type Metadata = ();

    fn submit(&mut self, entry: T) {
        self.output.logs.push(entry)
    }
}

impl<T> State for UnreplicatedReplica<T> {
    type Send = Never;
    type Output = Replicated<T, ()>;
    fn proceed(&mut self) -> Option<impl Iterator<Item = Action<Self::Send, Self::Output>>> {
        if self.output.logs.is_empty() {
            None
        } else {
            let output = replace(
                &mut self.output,
                Replicated {
                    logs: Default::default(),
                    metadata: (),
                },
            );
            Some(once(Action::Output(output)))
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }

    fn tick(&mut self, _since_start: Duration) {}
    fn tick_after(&self) -> Option<Duration> {
        None
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
