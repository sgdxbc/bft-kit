use bincode::{Decode, Encode};

use crate::{
    common::ReplicaId,
    crypto::{Digest, Sig, UpdateHash},
};

use super::{BlockNum, ViewNum};

pub use crate::common::client::Request;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Reply {
    pub seq: u32,
    pub view_num: ViewNum,
    pub result: Vec<u8>,
    pub replica_id: ReplicaId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PrePrepare {
    pub view_num: ViewNum,
    pub block_num: BlockNum,
    pub digest: Digest,
    pub sig: Sig,
    pub requests: Vec<Request>,
}

// shared definition for Prepare and Commit
#[derive(Debug, Clone, Encode, Decode)]
pub struct Vote {
    pub view_num: ViewNum,
    pub block_num: BlockNum,
    pub digest: Digest,
    pub replica_id: ReplicaId,
    pub sig: Sig,
}

impl<S: sha2::Digest> UpdateHash<S> for PrePrepare {
    fn update(&self, state: &mut S) {
        state.update(self.view_num.to_le_bytes());
        state.update(self.block_num.to_le_bytes());
        state.update(&self.digest)
    }
}

impl<S: sha2::Digest> UpdateHash<S> for Vote {
    fn update(&self, state: &mut S) {
        state.update(self.view_num.to_le_bytes());
        state.update(self.block_num.to_le_bytes());
        state.update(&self.digest);
        state.update(self.replica_id.to_le_bytes())
    }
}
