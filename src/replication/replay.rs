use std::time::Duration;

use crate::{
    Never,
    state::{Proceed, State},
};

use super::{Replicated, ReplicationState};

pub struct Replica<L> {
    logs: L,
    batch_size: usize,
}

impl<L> Replica<L> {
    pub fn new(logs: L, batch_size: usize) -> Self {
        Self { logs, batch_size }
    }
}

impl<L: Iterator> ReplicationState<L::Item> for Replica<L> {
    type Metadata = ();

    fn submit(&mut self, _request: L::Item) {
        unimplemented!()
    }
}

impl<L: Iterator> State for Replica<L> {
    type Send = Never;
    type Output = Replicated<L::Item, <Self as ReplicationState<L::Item>>::Metadata>;

    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        let logs = self.logs.by_ref().take(self.batch_size).collect::<Vec<_>>();
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
