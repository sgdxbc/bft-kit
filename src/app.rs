use std::{collections::VecDeque, time::Duration};

use crate::{
    Never,
    service::ServiceApp,
    state::{Proceed, State},
};

pub mod b_tree;
pub mod null;
pub mod rocksdb;
pub mod utxo;
pub mod ycsb;

pub trait AppState {
    type App: ServiceApp;
    fn execute(&mut self, op: <Self::App as ServiceApp>::Op) -> <Self::App as ServiceApp>::Res;
}

pub struct Batched<A>(A);

impl<A: ServiceApp> ServiceApp for Batched<A> {
    type Op = Vec<A::Op>;
    type Res = Vec<A::Res>;
}

impl<A: AppState> AppState for Batched<A> {
    type App = Batched<A::App>;

    fn execute(&mut self, ops: <Self::App as ServiceApp>::Op) -> <Self::App as ServiceApp>::Res {
        ops.into_iter().map(|op| self.0.execute(op)).collect()
    }
}

pub struct Buffered<A: AppState> {
    app: A,
    ops: VecDeque<<A::App as ServiceApp>::Op>,
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
    type Output = <A::App as ServiceApp>::Res;
    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.ops.pop_front() {
            Some(op) => Proceed::Output(self.app.execute(op)),
            None => Proceed::Pending(None),
        }
    }

    type Message = <A::App as ServiceApp>::Op;
    fn receive(&mut self, op: Self::Message) {
        self.ops.push_back(op);
    }
}
