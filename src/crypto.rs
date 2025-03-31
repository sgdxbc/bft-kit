use std::fmt::Debug;

use bincode::{Decode, Encode};

use crate::common::fmt_bytes;

pub mod cert;

// wire type for signature
// erasing type for simple (de)serialization
pub type Sig = Vec<u8>;

#[derive(Clone, PartialEq, Eq, Encode, Decode)]
pub struct Digest(pub Vec<u8>);

impl Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Digest")?;
        fmt_bytes(&self.0, f)
    }
}
