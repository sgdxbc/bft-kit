use bincode::{Decode, Encode};

use crate::{
    common::ReplicaId,
    crypto::{Digest, UpdateHash, threshold::PartialSig},
};

use super::{Block, QuorumCert};

pub use crate::common::client::Request;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Reply {
    pub seq: u32,
    pub result: Vec<u8>,
    pub replica_id: ReplicaId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Generic {
    // pub view_num: ViewNum,
    pub(super) node: Digest,
    pub(super) justify: QuorumCert,
    pub replica_id: ReplicaId,
    // the paper probably implies no signature is required for the messages
    // themselves. although the event drive algorithm uses notation MSG_u(..) it
    // seems not to be the conventional "signing" notation but simply
    // differentiate messages produced by replica u itself and others
    // in practice it is probably fine to send messages without signatures if the
    // underlying transport is point to point authenticated. (even if using
    // untrusted channels it seems ok for correctness, but that i'm not sure and
    // also there will probably be problems with liveness)
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct VoteGeneric {
    // pub view_num: ViewNum,
    pub node: Digest,
    pub partial_sig: PartialSig,
    // our transport interface does not provide sender id by default, so bring it by
    // ourselves
    pub replica_id: ReplicaId,
}

impl<S: sha2::Digest> UpdateHash<S> for Block {
    fn update(&self, state: &mut S) {
        state.update(&self.parent);
        for request in &self.requests {
            state.update(request.client_id.to_le_bytes());
            state.update(request.seq.to_le_bytes());
            state.update(&request.op)
        }
        state.update(&self.justify.node);
        // state.update(self.justify.view_num.to_le_bytes());
        state.update(self.height.to_le_bytes());
    }
}
