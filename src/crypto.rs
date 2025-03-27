use std::fmt::Debug;

use bincode::{Decode, Encode};

// wire type for signature
// erasing type for simple (de)serialization
pub type Sig = Vec<u8>;

#[derive(Clone, PartialEq, Eq, Encode, Decode)]
pub struct Digest(pub Vec<u8>);

impl Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Digest")
            .field(
                &self
                    .0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>(),
            )
            .finish()
    }
}
