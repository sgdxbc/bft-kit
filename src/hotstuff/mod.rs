use bincode::{Decode, Encode};

use crate::crypto::{Digest, threshold::Sig};

pub mod message;

#[derive(Debug, Clone, Encode, Decode)]
struct Block {
    parent: Digest,
    requests: Vec<message::Request>, // `cmd` in paper
    justify: QuorumCert,
    height: u32,
}

#[derive(Debug, Clone, Encode, Decode)]
struct QuorumCert {
    node: Digest,
    sig: Sig,
}
