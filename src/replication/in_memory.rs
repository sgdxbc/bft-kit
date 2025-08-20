use std::time::Duration;

use crate::{
    Never,
    service::{ClientSeq, Request, ServiceApp},
    state::{Proceed, State},
    workload::WorkloadState,
};

use super::{Replicated, ReplicationState};

pub struct Replica<W> {
    workload: W,
    batch_size: usize,
    seq: ClientSeq,
}

impl<W> Replica<W> {
    pub fn new(workload: W, batch_size: usize) -> Self {
        Self {
            workload,
            batch_size,
            seq: 0,
        }
    }
}

impl<W: WorkloadState<Metadata = ()>> ReplicationState<Request<<W::App as ServiceApp>::Op>>
    for Replica<W>
{
    type Metadata = ();

    fn submit(&mut self, _request: Request<<W::App as ServiceApp>::Op>) {
        unimplemented!()
    }
}

impl<W: WorkloadState<Metadata = ()>> State for Replica<W> {
    type Send = Never;
    type Output = Replicated<
        Request<<W::App as ServiceApp>::Op>,
        <Self as ReplicationState<Request<<W::App as ServiceApp>::Op>>>::Metadata,
    >;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        let mut logs = Vec::new();
        for _ in 0..self.batch_size {
            let Some((op, ())) = self.workload.next_op() else {
                break;
            };
            self.seq += 1;
            logs.push(Request {
                op,
                client_id: 0,
                client_seq: self.seq,
            })
        }
        if logs.is_empty() {
            Proceed::Pending(None)
        } else {
            Proceed::Output(Replicated { logs, metadata: () })
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}
