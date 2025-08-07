use std::{marker::PhantomData, time::Duration};

use crate::{
    Never,
    app::AppState,
    replication::{Replicated, ReplicationState},
    service::{ClientSeq, Request},
    state::{Proceed, State},
    workload::WorkloadState,
};

pub struct Replica<A, W> {
    workload: W,
    seq: ClientSeq,
    _app: PhantomData<A>,
}

impl<A, W> Replica<A, W> {
    pub fn new(workload: W) -> Self {
        Self {
            workload,
            seq: 0,
            _app: PhantomData,
        }
    }
}

impl<A: AppState, W: WorkloadState<Op = A::Op>> ReplicationState<Request<A::Op>> for Replica<A, W> {
    type Metadata = ();

    fn submit(&mut self, _request: Request<A::Op>) {
        unimplemented!()
    }
}

impl<A: AppState, W: WorkloadState<Op = A::Op>> State for Replica<A, W> {
    type Send = Never;
    type Output = Replicated<Request<A::Op>, <Self as ReplicationState<Request<A::Op>>>::Metadata>;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.workload.next_op() {
            Some(op) => {
                self.seq += 1;
                let request = Request {
                    op,
                    client_id: 0,
                    seq: self.seq,
                };
                Proceed::Output(Replicated {
                    block: vec![request],
                    metadata: (),
                })
            }
            None => Proceed::Pending(None),
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}
