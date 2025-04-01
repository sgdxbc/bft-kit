use bincode::{Decode, Encode};

use crate::{
    common::{ClientId, ReplicaId},
    crypto::{Digest, Sig, UpdateHash},
};

use super::Block;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Request {
    pub client_id: ClientId,
    pub seq: u32,
    pub op: Vec<u8>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Reply {
    pub seq: u32,
    pub result: Vec<u8>,
    pub replica_id: ReplicaId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Generic {
    pub(super) node: Block,
    pub replica_id: ReplicaId,
    pub sig: Sig,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct VoteGeneric {
    // echo back only the hash(generic.node) instead of the full Generic
    pub node: Digest,
    pub replica_id: ReplicaId,
    pub sig: Sig,
}

impl<S: sha2::Digest> UpdateHash<S> for Block {
    fn update(&self, state: &mut S) {
        state.update(&self.parent);
        for request in &self.requests {
            state.update(request.client_id.to_le_bytes());
            state.update(request.seq.to_le_bytes());
            state.update(&request.op)
        }
        self.justify.update(state);
        state.update(self.height.to_le_bytes());
    }
}

impl<S: sha2::Digest> UpdateHash<S> for super::QuorumCert {
    fn update(&self, state: &mut S) {
        state.update(&self.node);
        for (replica_id, partial_sig) in &self.sig {
            state.update(replica_id.to_le_bytes());
            state.update(partial_sig)
        }
    }
}

impl<S: sha2::Digest> UpdateHash<S> for Generic {
    fn update(&self, state: &mut S) {
        self.node.update(state);
        state.update(self.replica_id.to_le_bytes());
        state.update(&self.sig)
    }
}
