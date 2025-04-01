use bincode::{Decode, Encode};
use sha2::{Digest as _, Sha256};

use crate::{
    common::{ClientId, ReplicaId},
    crypto::{Digest, Sha256Hash, Sig},
};

use super::{BlockNum, ViewNum};

#[derive(Debug, Clone, Encode, Decode)]
pub struct Request {
    pub client_id: ClientId,
    pub seq: u32,
    pub op: Vec<u8>,
}

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

impl PrePrepare {
    pub fn sha256(&self) -> Sha256Hash {
        let mut state = Sha256::new();
        state.update(self.view_num.to_le_bytes());
        state.update(self.block_num.to_le_bytes());
        state.update(&self.digest);
        state.finalize()
    }
}

impl Vote {
    pub fn sha256(&self) -> Sha256Hash {
        let mut state = Sha256::new();
        state.update(self.view_num.to_le_bytes());
        state.update(self.block_num.to_le_bytes());
        state.update(&self.digest);
        state.update(self.replica_id.to_le_bytes());
        state.finalize()
    }
}
