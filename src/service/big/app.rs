use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    marker::PhantomData,
};

use derive_where::derive_where;

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

pub struct StaticDispatch<A: ShardedStateApp> {
    op: A::Op,
    app: A,
}

pub struct Kv;

pub enum KvOp {
    Put(String, String),
    Get(String),
}

pub enum KvRes {
    Put,
    Get(Option<String>),
}

impl ShardedStateApp for App<Kv> {
    type Op = Vec<KvOp>;
    type Res = Vec<KvRes>;
    type Shard = HashMap<String, String>;
    type Execute = StaticDispatch<Self>;

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

impl App<Kv> {
    fn shard_of(&self, op: &KvOp) -> ShardIndex {
        match op {
            KvOp::Put(key, _) | KvOp::Get(key) => self.shard_index_from_hash(key),
        }
    }

    fn execute(
        &self,
        op: &KvOp,
        shards: &mut HashMap<ShardIndex, HashMap<String, String>>,
    ) -> KvRes {
        match op {
            KvOp::Put(key, value) => {
                shards
                    .get_mut(&self.shard_of(op))
                    .unwrap()
                    .insert(key.clone(), value.clone());
                KvRes::Put
            }
            KvOp::Get(key) => KvRes::Get(shards.get(&self.shard_of(op)).unwrap().get(key).cloned()),
        }
    }
}

impl PartialStateExecute<App<Kv>> for StaticDispatch<App<Kv>> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, <App<Kv> as ShardedStateApp>::Shard>,
    ) -> PartialStateExecuteOutput<<App<Kv> as ShardedStateApp>::Res> {
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
