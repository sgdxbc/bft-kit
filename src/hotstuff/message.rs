use bincode::{Decode, Encode};

use crate::{
    common::ReplicaId,
    crypto::{
        Digest, UpdateHash,
        threshold::{GivrePublicCommitments, PartialSig, Sig},
    },
};

use super::BlockHeight;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Reply {
    pub seq: u32,
    pub result: Vec<u8>,
    pub replica_id: ReplicaId,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Generic {
    // pub view_num: ViewNum,
    pub block: Digest,
    pub public_commitments_vec: Vec<(givre::SignerIndex, GivrePublicCommitments)>,
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
pub struct Block {
    pub parent: Digest,
    pub commands: Vec<super::Command>, // cmd
    pub justify: QuorumCert,
    pub height: BlockHeight,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct QuorumCert {
    pub node: Digest,
    pub sig: Sig,
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
        (&*self.commands).update(state);
        state.update(&self.justify.node);
        state.update(self.height.to_le_bytes())
    }
}
