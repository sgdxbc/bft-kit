use std::{collections::VecDeque, time::Duration};

use crate::{
    Never,
    state::{Proceed, State},
};

pub mod kv;
pub mod null;

pub trait AppState {
    type Op;
    type Res;
    fn execute(&mut self, op: &Self::Op) -> Self::Res;
}

pub struct Buffered<A: AppState> {
    app: A,
    ops: VecDeque<A::Op>,
}

impl<A: AppState> From<A> for Buffered<A> {
    fn from(app: A) -> Self {
        Self {
            app,
            ops: Default::default(),
        }
    }
}

impl<A: AppState> State for Buffered<A> {
    type Send = Never;
    type Output = A::Res;
    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.ops.pop_front() {
            Some(op) => Proceed::Output(self.app.execute(&op)),
            None => Proceed::Pending(None),
        }
    }

    type Message = A::Op;
    fn receive(&mut self, op: Self::Message) {
        self.ops.push_back(op);
    }
}
