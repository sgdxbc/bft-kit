use std::time::Duration;

use crate::{
    Never,
    app::AppState,
    service::{ClientSeq, Request},
    state::{Proceed, State},
    workload::WorkloadState,
};

use super::{Replicated, ReplicationState};

pub struct Replica<W> {
    workload: W,
    seq: ClientSeq,
}

impl<W> Replica<W> {
    pub fn new(workload: W) -> Self {
        Self { workload, seq: 0 }
    }
}

impl<W: WorkloadState> ReplicationState<Request<<W::App as AppState>::Op>> for Replica<W> {
    type Metadata = ();

    fn submit(&mut self, _request: Request<<W::App as AppState>::Op>) {
        unimplemented!()
    }
}

impl<W: WorkloadState> State for Replica<W> {
    type Send = Never;
    type Output = Replicated<
        Request<<W::App as AppState>::Op>,
        <Self as ReplicationState<Request<<W::App as AppState>::Op>>>::Metadata,
    >;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.workload.next_op() {
            Some(op) => {
                self.seq += 1;
                let request = Request {
                    op,
                    client_id: 0,
                    client_seq: self.seq,
                };
                Proceed::Output(Replicated {
                    logs: vec![request],
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
