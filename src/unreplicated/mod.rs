use std::{collections::BTreeMap, time::Duration};

use crate::{
    service::{ClientId, ClientSeq, Reply, Request},
    state::{AppState, Proceed, State},
    workload::ClientState,
};

pub struct Client<A: AppState> {
    id: ClientId,
    config: ClientConfig,

    seq: ClientSeq,
    num_tick: u32,
    submits: BTreeMap<ClientSeq, ClientSubmit<A::Op>>,
    send_buffer: Vec<Request<A::Op>>,
    output_buffer: Vec<(A::Op, A::Res)>,
}

pub struct ClientConfig {
    timeout: u32, // number of full ticks
}

struct ClientSubmit<Op> {
    op: Op,
    after: u32,
}

impl<A: AppState> ClientState<A::Op> for Client<A>
where
    A::Op: Clone,
{
    fn submit(&mut self, op: A::Op) {
        self.seq += 1;
        self.submits.insert(
            self.seq,
            ClientSubmit {
                op: op.clone(),
                after: self.num_tick,
            },
        );
        self.send_buffer.push(Request {
            client_id: self.id,
            seq: self.seq,
            op,
        })
    }
}

impl<A: AppState> State for Client<A> {
    type Send = Request<A::Op>;
    type Output = (A::Op, A::Res);
    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        match self.output_buffer.pop() {
            Some(output) => Proceed::Output(output),
            None => match self.send_buffer.pop() {
                Some(req) => Proceed::Send(req),
                None => Proceed::Pending,
            },
        }
    }

    type Message = Reply<A::Res, ()>;
    fn receive(&mut self, message: Self::Message) {
        let Some(submit) = self.submits.remove(&message.seq) else {
            return;
        };
        self.output_buffer.push((submit.op, message.res))
    }

    fn tick(&mut self, _elapsed: Duration) {
        self.num_tick += 1;
        while let Some((_, submit)) = self.submits.first_key_value()
            && submit.after + self.config.timeout < self.num_tick
        {
            self.submits.pop_first();
        }
    }
}
