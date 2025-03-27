use bincode::{Decode, Encode};

use crate::{
    ClientId, ReplicaId,
    crypto::{Digest, Sig},
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
    pub sig: Sig,
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
