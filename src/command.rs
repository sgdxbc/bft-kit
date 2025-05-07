use bincode::{Decode, Encode};

use crate::{ClientId, ClientSeq, crypto::UpdateHash};

pub mod pool;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Command {
    pub client_id: ClientId,
    pub seq: ClientSeq,
    pub op: Vec<u8>,
}

// produce a more informative compile error hopefully
#[cfg(test)]
const _: () = assert!(size_of::<u32>() == size_of::<ClientId>());

#[cfg(test)]
impl Command {
    pub fn new(client_id: u32, seq: ClientSeq) -> Self {
        Self {
            client_id: ClientId(client_id),
            seq,
            op: format!("command@{client_id:x}#{seq}").into(),
        }
    }
}

impl UpdateHash for Command {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.client_id.to_le_bytes());
        state.update(self.seq.to_le_bytes());
        state.update(&self.op)
    }
}
