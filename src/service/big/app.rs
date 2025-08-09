use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    marker::PhantomData,
};

use derive_where::derive_where;

use crate::app::{
    AppState,
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

impl ShardedStateApp for App<Kv> {
    type Op = KvOp;
    type Res = KvRes;
    type Shard = HashMap<String, String>;
    type Execute = StaticDispatch<Kv>;

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
    fn shard_of(&self, op: &KvOp) -> HashSet<ShardIndex> {
        match op {
            KvOp::Insert(key, _) | KvOp::Update(key, _) | KvOp::Get(key) => {
                [self.shard_index_from_hash(key)].into()
            }
        }
    }

    fn execute(
        &self,
        op: &KvOp,
        shards: &mut HashMap<ShardIndex, HashMap<String, String>>,
    ) -> KvRes {
        match op {
            KvOp::Insert(key, value) => {
                let shard = shards.get_mut(&self.shard_index_from_hash(key)).unwrap();
                shard.insert(key.clone(), value.clone());
                KvRes::InsertOk
            }
            KvOp::Update(key, value) => match shards
                .get_mut(&self.shard_index_from_hash(key))
                .unwrap()
                .get_mut(key)
            {
                Some(existing_value) => {
                    *existing_value = value.clone();
                    KvRes::UpdateOk
                }
                None => KvRes::NotFound,
            },
            KvOp::Get(key) => match shards
                .get_mut(&self.shard_index_from_hash(key))
                .unwrap()
                .get(key)
            {
                Some(value) => KvRes::GetOk(value.clone()),
                None => KvRes::NotFound,
            },
        }
    }
}

impl PartialStateExecute<HashMap<String, String>, KvRes> for StaticDispatch<Kv> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, HashMap<String, String>>,
    ) -> PartialStateExecuteOutput<KvRes> {
        let required_indices =
            &self.app.shard_of(&self.op) - &shards.keys().cloned().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            PartialStateExecuteOutput::RequireAccess(required_indices)
        } else {
            let res = self.app.execute(&self.op, shards);
            PartialStateExecuteOutput::Complete(res)
        }
    }
}
