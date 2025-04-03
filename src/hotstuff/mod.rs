use bincode::{Decode, Encode};

use crate::crypto::{Digest, threshold::Sig};

pub mod message;

// the event driven algorithm does not maintain view number anywhere (explicitly
// stated in the "Data structures" paragraph), although it does not provide a
// new version of MSG and QC to remove the access within them
// anyway, i will first go without view number and see how it goes
// type ViewNum = u32;
type BlockHeight = u32;

#[derive(Debug, Clone, Encode, Decode)]
struct Block {
    parent: Digest,
    requests: Vec<message::Request>, // `cmd` in paper
    justify: QuorumCert,
    height: BlockHeight,
}

#[derive(Debug, Clone, Encode, Decode)]
struct QuorumCert {
    // view_num: ViewNum,
    node: Digest,
    sig: Sig,
}
