use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    time::Duration,
};

use crate::{
    Never,
    state::{Proceed, State},
};

pub mod b_tree;
pub mod kv;
pub mod null;
pub mod rocksdb;
pub mod utxo;
pub mod ycsb;

pub trait AppProtocol {
    type Op;
    type Res;
}

pub trait AppState: AppProtocol {
    fn execute(&mut self, op: Self::Op) -> Self::Res;
}

pub trait DataShardingApp: AppProtocol {
    type Key;
    type Value;
    type ExecuteState: DataShardingExecuteState<App = Self>;
    fn new_execute(&self, op: Self::Op) -> Self::ExecuteState;
}

pub trait DataShardingExecuteState {
    type App: DataShardingApp;
    fn get_result(
        &mut self,
        key: <Self::App as DataShardingApp>::Key,
        value: Option<<Self::App as DataShardingApp>::Value>,
    );
    fn proceed(&mut self) -> DataShardingExecuteOutput<Self::App>;
}

pub enum DataShardingExecuteOutput<A: DataShardingApp> {
    Get(A::Key),
    Put(A::Key, A::Value),
    Pending,
    Complete(A::Res),
}

pub struct Batched<A>(A);

impl<A: AppProtocol> AppProtocol for Batched<A> {
    type Op = Vec<A::Op>;
    type Res = Vec<A::Res>;
}

impl<A: AppState> AppState for Batched<A> {
    fn execute(&mut self, ops: Self::Op) -> Self::Res {
        ops.into_iter().map(|op| self.0.execute(op)).collect()
    }
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
            Some(op) => Proceed::Output(self.app.execute(op)),
            None => Proceed::Pending(None),
        }
    }

    type Message = A::Op;
    fn receive(&mut self, op: Self::Message) {
        self.ops.push_back(op);
    }
}

pub struct InMemory<A: DataShardingApp> {
    pub app: A,
    pub store: HashMap<A::Key, A::Value>,
}

impl<A: DataShardingApp> AppProtocol for InMemory<A> {
    type Op = A::Op;
    type Res = A::Res;
}

impl<A: DataShardingApp> AppState for InMemory<A>
where
    A::Key: Hash + Eq,
    A::Value: Clone,
{
    fn execute(&mut self, op: Self::Op) -> Self::Res {
        let mut execute = self.app.new_execute(op);
        loop {
            match execute.proceed() {
                DataShardingExecuteOutput::Get(key) => {
                    let value = self.store.get(&key).cloned();
                    execute.get_result(key, value)
                }
                DataShardingExecuteOutput::Put(key, value) => {
                    self.store.insert(key, value);
                }
                DataShardingExecuteOutput::Pending => unimplemented!(),
                DataShardingExecuteOutput::Complete(res) => break res,
            }
        }
    }
}
