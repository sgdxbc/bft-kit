use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, BuildHasherDefault, DefaultHasher},
};

use crate::app::ShardIndex;

pub struct Kv {
    num_shard: ShardIndex,
}

impl Kv {
    pub fn new(num_shard: ShardIndex) -> Self {
        Self { num_shard }
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
    fn shards_of(&self, op: &KvOp) -> HashSet<ShardIndex> {
        match op {
            KvOp::Insert(key, _) | KvOp::Update(key, _) | KvOp::Get(key) => {
                HashSet::from([self.shard_of_key(key)])
            }
            KvOp::Compound(ops) => ops.iter().flat_map(|op| self.shards_of(op)).collect(),
        }
    }
}

// impl ShardedAppState for Kv {
//     type Shard = HashMap<String, String>;
//     type Op = KvOp;
//     type Res = KvRes;

//     fn update(
//         &mut self,
//         op: &Self::Op,
//         mut shards: HashMap<ShardIndex, &mut Self::Shard>,
//     ) -> ShardedAppUpdate<Self::Res> {
//         let missing_indices = &self.shards_of(op) - &shards.keys().cloned().collect::<HashSet<_>>();
//         if !missing_indices.is_empty() {
//             ShardedAppUpdate::RequireShards(missing_indices)
//         } else {
//             ShardedAppUpdate::Res(self.execute(op, &mut shards))
//         }
//     }
// }

impl Kv {
    fn execute(
        &mut self,
        op: &KvOp,
        shards: &mut HashMap<ShardIndex, &mut HashMap<String, String>>,
    ) -> KvRes {
        match op {
            KvOp::Insert(key, value) => {
                shards
                    .get_mut(&self.shard_of_key(key))
                    .unwrap()
                    .insert(key.clone(), value.clone());
                KvRes::InsertOk
            }
            KvOp::Update(key, value) => {
                let shard = shards.get_mut(&self.shard_of_key(key)).unwrap();
                match shard.get_mut(key) {
                    Some(existing_value) => {
                        *existing_value = value.clone();
                        KvRes::UpdateOk
                    }
                    None => KvRes::NotFound,
                }
            }
            KvOp::Get(key) => match shards.get(&self.shard_of_key(key)).unwrap().get(key) {
                Some(value) => KvRes::Get(value.clone()),
                None => KvRes::NotFound,
            },
            KvOp::Compound(ops) => {
                KvRes::Compound(ops.iter().map(|op| self.execute(op, shards)).collect())
            }
        }
    }
}
