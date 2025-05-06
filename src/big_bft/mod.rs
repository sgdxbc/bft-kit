use std::ops::Deref;

use bincode::{Decode, Encode};

pub mod replica;
pub mod state_shard;
pub mod transport;
pub mod workload;

pub type DigestHash = [u8; 32];

#[derive(Debug, Clone, Encode, Decode)]
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

#[derive(Debug, Clone, Encode, Decode)]
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
    pub num_shard: usize,
    pub num_replica: usize,
    pub num_fault: usize,
    pub num_stripe_shard: usize,
    pub num_fast_replica: usize,
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

pub type Version = u32;

pub mod message {
    use bincode::{Decode, Encode};

    use crate::big_bft::state_shard::StateShard;

    use super::{DigestHash, Version};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct SyncShard {
        pub version: Version,
        pub index: usize,
        pub data: StateShard,
    }

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Reply {
        pub version: Version,
        pub hash: DigestHash,
        pub replica_index: usize,
    }
}

pub mod parse {
    use std::time::Duration;

    use crate::parse::Options;

    use super::{Spec, replica::ReplicaCoreConfig, transport::TaskConfig};

    impl TryFrom<Options> for ReplicaCoreConfig {
        type Error = anyhow::Error;

        fn try_from(value: Options) -> Result<Self, Self::Error> {
            Ok(Self {
                spec: value.clone().try_into()?,
                index: value.get("replica_id")?,
            })
        }
    }

    impl TryFrom<Options> for Spec {
        type Error = anyhow::Error;

        fn try_from(options: Options) -> Result<Self, Self::Error> {
            Ok(Self {
                num_shard: options.get("num_shard")?,
                num_replica: options.get("num_replica")?,
                num_fault: options.get("num_fault")?,
                num_stripe_shard: options.get("num_stripe_shard")?,
                num_fast_replica: options.get("num_fast_replica")?,
            })
        }
    }

    impl TryFrom<Options> for TaskConfig {
        type Error = anyhow::Error;

        fn try_from(options: Options) -> Result<Self, Self::Error> {
            Ok(Self {
                service: options.clone().try_into()?,
                replica: options.clone().try_into()?,
                num_concurrent: options.get("num_concurrent")?,
                client_duration: Duration::from_secs_f32(options.get("client_duration")?),
            })
        }
    }
}
