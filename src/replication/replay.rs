use std::time::Duration;

use crate::{
    Never,
    state::{Action, State},
};

use super::{Replicated, ReplicationState};

pub struct ReplayReplica<L> {
    logs: L,
    batch_size: usize,
}

impl<L> ReplayReplica<L> {
    pub fn new(logs: L, batch_size: usize) -> Self {
        Self { logs, batch_size }
    }
}

impl<L: Iterator> ReplicationState<L::Item> for ReplayReplica<L> {
    type Metadata = ();

    fn submit(&mut self, _request: L::Item) {
        unimplemented!()
    }
}

impl<L: Iterator> State for ReplayReplica<L> {
    type Effect = Never;
    type Output = Replicated<L::Item, <Self as ReplicationState<L::Item>>::Metadata>;

    fn proceed(&mut self, _since_start: Duration) -> Action<Self::Effect, Self::Output> {
        let logs = self.logs.by_ref().take(self.batch_size).collect::<Vec<_>>();
        if logs.is_empty() {
            Action::Pending(None)
        } else {
            Action::Output(Replicated { logs, metadata: () })
        }
    }

    type Message = Never;
    fn receive(&mut self, _message: Self::Message) {
        unreachable!()
    }
}
