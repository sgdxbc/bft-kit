use std::fmt::{self, Formatter};

use crate::ReplicaId;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bincode::Encode, bincode::Decode)]
pub struct Id(pub u32);

#[derive(Debug)]
pub enum Action<M> {
    Nop,
    Return(Vec<u8>),
    SendToReplica(ReplicaId, M),
    SendToAllReplicas(M),
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ClientId({:08x})", self.0)
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl Id {
    pub fn from_le_bytes(bytes: [u8; size_of::<u32>()]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    pub fn to_le_bytes(self) -> [u8; size_of::<u32>()] {
        self.0.to_le_bytes()
    }
}

impl rand::distr::Distribution<Id> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Id {
        Id(rand::Rng::random(rng))
    }
}

pub type Seq = u32;
