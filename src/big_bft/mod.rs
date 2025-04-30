use std::ops::Deref;

pub mod replica;
pub mod state_shard;
pub mod transport;
pub mod workload;

pub type DigestHash = [u8; 32];

#[derive(Debug, Clone)]
pub enum Op {
    Insert(DigestHash, String),
    Read(DigestHash),
    Update(DigestHash, String),
    // TODO Scan, ReadModifyWrite
}

impl Op {
    pub fn key(&self) -> &DigestHash {
        match self {
            Self::Insert(key, _) | Self::Read(key) | Self::Update(key, _) => key,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Txn(pub Vec<Op>);

impl Deref for Txn {
    type Target = [Op];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Txn {
    pub fn keys(&self) -> impl Iterator<Item = &DigestHash> {
        self.0.iter().map(Op::key)
    }
}

#[derive(Debug, Clone)]
pub struct Spec {
    num_shard: usize,
    num_replica: usize,
    num_fault: usize,
    num_stripe_shard: usize,
    num_fast_replica: usize,
}

impl Spec {
    pub fn shard_of(&self, key: &DigestHash) -> usize {
        use std::hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher};
        BuildHasherDefault::<DefaultHasher>::new().hash_one(key) as usize % self.num_shard
    }

    pub fn fast_replicas(&self, shard_index: usize) -> impl Iterator<Item = usize> {
        use std::{
            hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher},
            iter::repeat_with,
        };
        let build_hasher = BuildHasherDefault::<DefaultHasher>::new();
        let mut n = shard_index as u64;
        repeat_with(move || {
            n = build_hasher.hash_one(n);
            n as usize % self.num_replica
        })
        .take(self.num_fast_replica)
    }

    pub fn stripe_of(&self, shard_index: usize) -> usize {
        shard_index % self.num_stripe_shard
    }

    pub fn stripe_at(&self, stripe: usize) -> impl Iterator<Item = usize> {
        // should be fine for stripe placements to not including all possible
        // combinations (like fast replicas do)
        // round robin is the most clear pattern i guess
        (stripe..stripe + self.num_stripe_shard + self.num_fault * 2)
            .map(|index| index % self.num_replica)
    }
}
