use std::fmt::{self, Formatter, Write as _};

// pub type ClientId = u32;
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, bincode::Encode, bincode::Decode,
)]
pub struct ClientId(pub u32);

impl fmt::Display for ClientId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "ClientId({:08x})", self.0)
    }
}

impl ClientId {
    pub fn from_le_bytes(bytes: [u8; size_of::<u32>()]) -> Self {
        Self(u32::from_le_bytes(bytes))
    }

    pub fn to_le_bytes(self) -> [u8; size_of::<u32>()] {
        self.0.to_le_bytes()
    }
}

impl rand::distr::Distribution<ClientId> for rand::distr::StandardUniform {
    fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> ClientId {
        ClientId(rand::Rng::random(rng))
    }
}

pub type ReplicaId = u8;

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
