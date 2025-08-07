use std::{
    collections::{HashSet, VecDeque},
    time::Duration,
};

use crate::{
    Never,
    state::{Proceed, State},
};

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
    fn set_shard(&mut self, index: ShardIndex, shard: Self::Shard);
    fn get_shard(&self, index: ShardIndex) -> Option<&Self::Shard>;
    fn retain_shards(&self, indices: &HashSet<ShardIndex>);

    type Op;
    type Res;
    fn update(&mut self, op: &Self::Op) -> ShardedAppUpdate<Self::Res>;
}

pub enum ShardedAppUpdate<R> {
    RequireShards(HashSet<ShardIndex>),
    Res(R),
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
