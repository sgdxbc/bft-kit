use std::{marker::PhantomData, time::Duration};

use crate::{
    app::AppState,
    replication::{Replicated, ReplicationState},
    service::{ClientSeq, Request},
    state::{Never, Proceed, State},
    workload::WorkloadState,
};

pub struct Replica<A, E, W> {
    event_buffer: Vec<E>,
    workload: W,
    seq: ClientSeq,
    _app: PhantomData<A>,
}

impl<A, E, W> Replica<A, E, W> {
    pub fn new(workload: W) -> Self {
        Self {
            workload,
            event_buffer: Default::default(),
            seq: 0,
            _app: PhantomData,
        }
    }
}

impl<A: AppState, E, W: WorkloadState<Op = A::Op>> ReplicationState<A::Op, E> for Replica<A, E, W> {
    type Metadata = ();

    fn submit(&mut self, _request: Request<A::Op>) {
        unimplemented!()
    }

    fn trigger(&mut self, event: E) {
        self.event_buffer.push(event)
    }
}

impl<A: AppState, E, W: WorkloadState<Op = A::Op>> State for Replica<A, E, W> {
    type Send = Never;
    type Output = Replicated<A::Op, <Self as ReplicationState<A::Op, E>>::Metadata, E>;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(event) = self.event_buffer.pop() {
            return Proceed::Output(Replicated::Event(event));
        }
        match self.workload.next_op() {
            Some(op) => {
                self.seq += 1;
                let request = Request {
                    op,
                    client_id: 0,
                    seq: self.seq,
                };
                Proceed::Output(Replicated::Block(vec![request], ()))
            }
            None => Proceed::Pending(None),
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}
