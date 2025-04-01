use std::fmt::{Debug, Display};

use bincode::{Decode, Encode};

use crate::common::fmt_bytes;

pub mod cert;

#[derive(Clone, PartialEq, Eq, Encode, Decode)]
pub struct Digest(pub Vec<u8>);

impl Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Digest")?;
        fmt_bytes(&self.0, f)
    }
}

impl Debug for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub type Sha256Hash = sha2::digest::Output<sha2::Sha256>;

impl From<Sha256Hash> for Digest {
    fn from(value: Sha256Hash) -> Self {
        Digest(value.to_vec())
    }
}

// wire type for signature
// erasing type for simple (de)serialization
#[derive(Clone, Default, Encode, Decode)]
pub struct Sig(pub Vec<u8>);

impl Display for Sig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Sig")?;
        fmt_bytes(&self.0, f)
    }
}

impl Debug for Sig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

pub type SecretKey = secp256k1::SecretKey;
pub type PublicKey = secp256k1::PublicKey;

thread_local!(static SECP: secp256k1::Secp256k1<secp256k1::All> = secp256k1::Secp256k1::new());

pub fn sign(message: Sha256Hash, secret_key: &SecretKey) -> Sig {
    let message = secp256k1::Message::from_digest(message.into());
    Sig(SECP
        .with(|secp| secp.sign_ecdsa(&message, secret_key))
        .serialize_compact()
        .to_vec())
}

pub fn verify(message: Sha256Hash, public_key: &PublicKey, Sig(sig): &Sig) -> anyhow::Result<()> {
    let message = secp256k1::Message::from_digest(message.into());
    SECP.with(|secp| {
        secp.verify_ecdsa(
            &message,
            &secp256k1::ecdsa::Signature::from_compact(&sig)?,
            public_key,
        )
    })?;
    Ok(())
}
