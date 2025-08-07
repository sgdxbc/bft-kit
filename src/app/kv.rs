use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
};

use crate::app::{ShardIndex, ShardedAppState, ShardedAppUpdate};

pub struct Kv {
    shards: HashMap<ShardIndex, HashMap<String, String>>,
    num_shard: ShardIndex,
}

pub struct KvConfig {}

impl Kv {
    pub fn new(num_shard: ShardIndex) -> Self {
        Self {
            num_shard,
            shards: Default::default(),
        }
    }

    fn shard_of_key(&self, key: &str) -> ShardIndex {
        (BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % self.num_shard as u64) as _
    }
}

pub enum KvOp {
    Insert(String, String),
    Update(String, String),
    Get(String),
    Compound(Vec<KvOp>),
}

pub enum KvRes {
    InsertOk,
    UpdateOk,
    Get(String),
    NotFound,
    Compound(Vec<KvRes>),
}

impl Kv {
    fn shard_of(&self, op: &KvOp) -> HashSet<ShardIndex> {
        match op {
            KvOp::Insert(key, _) | KvOp::Update(key, _) | KvOp::Get(key) => {
                HashSet::from([self.shard_of_key(key)])
            }
            KvOp::Compound(ops) => ops.iter().flat_map(|op| self.shard_of(op)).collect(),
        }
    }

    fn execute(&mut self, op: &KvOp) -> KvRes {
        match op {
            KvOp::Insert(key, value) => {
                self.shards
                    .get_mut(&self.shard_of_key(key))
                    .unwrap()
                    .insert(key.clone(), value.clone());
                KvRes::InsertOk
            }
            KvOp::Update(key, value) => {
                let shard = self.shards.get_mut(&self.shard_of_key(key)).unwrap();
                match shard.get_mut(key) {
                    Some(existing_value) => {
                        *existing_value = value.clone();
                        KvRes::UpdateOk
                    }
                    None => KvRes::NotFound,
                }
            }
            KvOp::Get(key) => match self.shards.get(&self.shard_of_key(key)).unwrap().get(key) {
                Some(value) => KvRes::Get(value.clone()),
                None => KvRes::NotFound,
            },
            KvOp::Compound(ops) => KvRes::Compound(ops.iter().map(|op| self.execute(op)).collect()),
        }
    }
}

impl ShardedAppState for Kv {
    type Shard = HashMap<String, String>;

    fn insert_shard(&mut self, index: ShardIndex, shard: Self::Shard) {
        self.shards.insert(index, shard);
    }

    fn remove_shard(&mut self, index: ShardIndex) -> Option<Self::Shard> {
        self.shards.remove(&index)
    }

    type Op = KvOp;
    type Res = KvRes;
    fn update(&mut self, op: &Self::Op) -> ShardedAppUpdate<Self::Res> {
        let shard_indices = self.shard_of(op);
        if !shard_indices.is_empty() {
            ShardedAppUpdate::NeedShards(shard_indices)
        } else {
            ShardedAppUpdate::Res(self.execute(op))
        }
    }
}
