use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    marker::PhantomData,
};

use derive_where::derive_where;

use crate::app::{
    AppState, Batched,
    kv::{Kv, KvOp, KvRes},
};

use super::{PartialStateExecute, PartialStateExecuteOutput, ShardIndex, ShardedStateApp};

#[derive_where(Debug, Clone)]
pub struct App<A> {
    num_shard: ShardIndex,
    _app: PhantomData<A>,
}

impl<A> App<A> {
    pub fn new(num_shard: ShardIndex) -> Self {
        Self {
            num_shard,
            _app: PhantomData,
        }
    }

    fn shard_index_from_hash(&self, key: impl Hash) -> ShardIndex {
        (BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % self.num_shard as u64) as _
    }
}

pub struct StaticDispatch<A: AppState> {
    op: A::Op,
    app: App<A>,
}

impl ShardedStateApp for App<Batched<Kv>> {
    type Op = Vec<KvOp>;
    type Res = Vec<KvRes>;
    type Shard = BTreeMap<String, String>;
    type Execute = StaticDispatch<Batched<Kv>>;

    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }

    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        StaticDispatch {
            op,
            app: self.clone(),
        }
    }
}

impl App<Batched<Kv>> {
    fn shard_of(&self, op: &KvOp) -> ShardIndex {
        match op {
            KvOp::Insert(key, _) | KvOp::Update(key, _) | KvOp::Get(key) => {
                self.shard_index_from_hash(key)
            }
        }
    }

    fn execute(
        &self,
        op: &KvOp,
        shards: &mut HashMap<ShardIndex, BTreeMap<String, String>>,
    ) -> KvRes {
        Kv::execute_with_store(op, shards.get_mut(&self.shard_of(op)).unwrap())
    }
}

impl PartialStateExecute<BTreeMap<String, String>, Vec<KvRes>> for StaticDispatch<Batched<Kv>> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, BTreeMap<String, String>>,
    ) -> PartialStateExecuteOutput<Vec<KvRes>> {
        let required_indices = &self
            .op
            .iter()
            .map(|op| self.app.shard_of(op))
            .collect::<HashSet<_>>()
            - &shards.keys().cloned().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            PartialStateExecuteOutput::RequireAccess(required_indices)
        } else {
            let res = self
                .op
                .iter()
                .map(|op| self.app.execute(op, shards))
                .collect();
            PartialStateExecuteOutput::Complete(res)
        }
    }
}
