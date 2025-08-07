use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use crate::state::{Never, Proceed, State};

pub mod kv;
pub mod null;

pub trait AppState {
    type Op;
    type Res;
    fn update(&mut self, op: &Self::Op) -> Self::Res;
}

pub type ShardIndex = u32;

pub trait ShardedAppState {
    type Shard;
    fn insert_shard(&mut self, index: ShardIndex, shard: Self::Shard);
    fn remove_shard(&mut self, index: ShardIndex) -> Option<Self::Shard>;

    type Op;
    type Res;
    fn update(&mut self, op: &Self::Op) -> ShardedAppUpdate<Self::Res>;
}

pub enum ShardedAppUpdate<R> {
    NeedShards(HashSet<ShardIndex>),
    Res(R),
}

// this is safe: ShardedAppState should ensure that, as long as remove_shard is
// not called, NeedShards is never returned (hence insert_shard is never
// necessary)
// (well there is a remark: App::update can be called from a ShardedAppState. a
// newtype can prevent it, but come on)
impl<A: ShardedAppState> AppState for A {
    type Op = A::Op;
    type Res = A::Res;
    fn update(&mut self, op: &Self::Op) -> Self::Res {
        match ShardedAppState::update(self, op) {
            ShardedAppUpdate::Res(res) => res,
            ShardedAppUpdate::NeedShards(_) => unimplemented!(),
        }
    }
}

pub struct AdaptedApp<A: AppState> {
    app: A,
    results: VecDeque<A::Res>,
}

impl<A: AppState> From<A> for AdaptedApp<A> {
    fn from(app: A) -> Self {
        Self {
            app,
            results: Default::default(),
        }
    }
}

impl<A: AppState> State for AdaptedApp<A> {
    type Send = Never;
    type Output = A::Res;
    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.results.pop_front() {
            Some(res) => Proceed::Output(res),
            None => Proceed::Pending(None),
        }
    }

    type Message = A::Op;
    fn receive(&mut self, message: Self::Message) {
        let res = self.app.update(&message);
        self.results.push_back(res);
    }
}
