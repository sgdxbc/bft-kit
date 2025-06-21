//! https://github.com/sgdxbc/bft-kit/discussions/3
use std::fmt::{Debug, Display};

use bincode::{Decode, Encode};

use crate::crypto::UpdateHash;

pub mod pool;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct ClientId(pub u32);

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client#{:08x}", self.0)
    }
}

impl Debug for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

type ClientSeq = u64;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Command {
    pub client_id: ClientId,
    pub seq: ClientSeq,
    pub op: Vec<u8>,
}

// produce a more informative compile error hopefully
#[cfg(test)]
const _: () = assert!(size_of::<u32>() == size_of::<ClientId>());

#[cfg(test)]
impl Command {
    pub fn new(client_id: u32, seq: ClientSeq) -> Self {
        Self {
            client_id: ClientId(client_id),
            seq,
            op: format!("command@{client_id:x}#{seq}").into(),
        }
    }
}

impl UpdateHash for ClientId {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.0.to_le_bytes());
    }
}

impl UpdateHash for Command {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        self.client_id.update(state);
        state.update(self.seq.to_le_bytes());
        state.update(&self.op)
    }
}
