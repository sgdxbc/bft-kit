use std::fmt::{self, Formatter, Write as _};

use bincode::{Decode, Encode};

use crate::crypto::UpdateHash;

pub mod command_pool;
pub use command_pool::CommandPool;

// pub type ClientId = u32;
pub use client::Id as ClientId;
pub use client::Seq as ClientSeq;

pub mod client {
    use std::fmt::{self, Formatter};

    #[derive(
        Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bincode::Encode, bincode::Decode,
    )]
    pub struct Id(pub u32);

    #[derive(Debug)]
    pub enum Action<M> {
        Nop,
        Return(Vec<u8>),
        SendToReplica(super::ReplicaId, M),
        SendToAllReplicas(M),
    }

    impl fmt::Display for Id {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "ClientId({:08x})", self.0)
        }
    }

    impl fmt::Debug for Id {
        fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
            write!(f, "{self}")
        }
    }

    impl Id {
        pub fn from_le_bytes(bytes: [u8; size_of::<u32>()]) -> Self {
            Self(u32::from_le_bytes(bytes))
        }

        pub fn to_le_bytes(self) -> [u8; size_of::<u32>()] {
            self.0.to_le_bytes()
        }
    }

    impl rand::distr::Distribution<Id> for rand::distr::StandardUniform {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Id {
            Id(rand::Rng::random(rng))
        }
    }

    pub type Seq = u32;
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Command {
    pub client_id: ClientId,
    pub seq: ClientSeq,
    pub op: Vec<u8>,
}

impl<S: sha2::Digest> UpdateHash<S> for Command {
    fn update(&self, state: &mut S) {
        state.update(self.client_id.to_le_bytes());
        state.update(self.seq.to_le_bytes());
        state.update(&self.op)
    }
}

pub type ReplicaId = u8;

#[derive(Debug)]
pub enum ReplicaAction<M> {
    SendToReplica(ReplicaId, M),
    SendToAllReplicas(M), // except loopback
    // the commands are ready to be executed by the replicated service
    // commonly referred as "commit", but the term commit has been overloaded by
    // protocols such as PBFT as "start the commit phase", so following the
    // permissionless convention and saying the commands "reach the finality"
    Finalize(Vec<Command>),
}

pub fn fmt_bytes(bytes: &[u8], f: &mut Formatter<'_>) -> fmt::Result {
    let prefix_hex = bytes.iter().take(4).fold(String::new(), |mut s, b| {
        write!(&mut s, "{b:02x}").unwrap();
        s
    });
    write!(
        f,
        "[{}]({prefix_hex}{})",
        bytes.len(),
        if bytes.len() > 4 { "..." } else { "" }
    )
}
