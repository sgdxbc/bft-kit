use bincode::{Decode, Encode};

use crate::{
    ReplicaId,
    crypto::{Digest, Sig, UpdateHash},
};

use super::{BlockNum, ViewNum};

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

impl UpdateHash for PrePrepare {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.view_num.to_le_bytes());
        state.update(self.block_num.to_le_bytes());
        state.update(&self.digest)
    }
}

impl UpdateHash for Vote {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.view_num.to_le_bytes());
        state.update(self.block_num.to_le_bytes());
        state.update(&self.digest);
        state.update(self.replica_id.to_le_bytes())
    }
}
