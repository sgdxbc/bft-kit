use std::fmt::{self, Formatter, Write as _};

pub mod request_pool;
pub use request_pool::RequestPool;

pub mod client {
    use std::fmt::{self, Formatter};

    use bincode::{Decode, Encode};

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

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Request {
        pub client_id: Id,
        pub seq: Seq,
        pub op: Vec<u8>,
    }
}

// pub type ClientId = u32;
pub use client::Id as ClientId;
pub use client::Seq as ClientSeq;

pub type ReplicaId = u8;

#[derive(Debug)]
pub enum ReplicaAction<M> {
    SendToReplica(ReplicaId, M),
    SendToAllReplicas(M), // except loopback
    Finalize(Vec<client::Request>),
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
