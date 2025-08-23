use std::{iter::once, time::Duration};

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
    type Send = Never;
    type Output = Replicated<L::Item, <Self as ReplicationState<L::Item>>::Metadata>;

    fn proceed(&mut self) -> Option<impl Iterator<Item = Action<Self::Send, Self::Output>>> {
        let logs = self.logs.by_ref().take(self.batch_size).collect::<Vec<_>>();
        if logs.is_empty() {
            None
        } else {
            Some(once(Action::Output(Replicated { logs, metadata: () })))
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
