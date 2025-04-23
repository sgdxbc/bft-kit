use std::fmt::{Debug, Display};

use bincode::{BorrowDecode, Decode, Encode, error::DecodeError};
use sha2::Digest as _;

use crate::common::{ReplicaId, fmt_bytes};

pub mod cert;
pub mod threshold;

#[derive(Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct Digest(pub [u8; 32]);

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

pub type Sha256Output = sha2::digest::Output<sha2::Sha256>;

impl From<Sha256Output> for Digest {
    fn from(value: Sha256Output) -> Self {
        Digest(value.into())
    }
}

impl From<Digest> for [u8; 32] {
    fn from(Digest(bytes): Digest) -> Self {
        bytes
    }
}

pub trait UpdateHash {
    fn update<D: sha2::Digest>(&self, state: &mut D);
}

impl<T: UpdateHash> UpdateHash for &[T] {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        for item in *self {
            item.update(state)
        }
    }
}

pub type Sha512Output = sha2::digest::Output<sha2::Sha512>;

pub trait DigestHash {
    fn sha256(&self) -> Sha256Output;
    fn sha512(&self) -> Sha512Output;
}

impl<T: UpdateHash> DigestHash for T {
    fn sha256(&self) -> Sha256Output {
        let mut state = sha2::Sha256::new();
        self.update(&mut state);
        state.finalize()
    }
    fn sha512(&self) -> Sha512Output {
        let mut state = sha2::Sha512::new();
        self.update(&mut state);
        state.finalize()
    }
}

// wire type for signature
#[derive(Clone, Default)]
pub enum Sig {
    #[default]
    Uninitialized,
    Secp256k1(secp256k1::ecdsa::Signature),
}

impl Display for Sig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "Uninitialized"),
            Self::Secp256k1(sig) => {
                write!(f, "Secp256k1")?;
                fmt_bytes(&sig.serialize_compact(), f)
            }
        }
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

pub fn sign(message: &impl DigestHash, secret_key: &SecretKey) -> Sig {
    if cfg!(test) {
        Default::default()
    } else {
        let message = secp256k1::Message::from_digest(message.sha256().into());
        Sig::Secp256k1(SECP.with(|secp| secp.sign_ecdsa(&message, secret_key)))
    }
}

pub fn verify(message: &impl DigestHash, public_key: &PublicKey, sig: &Sig) -> anyhow::Result<()> {
    verify_digest(message.sha256().into(), public_key, sig)
}

fn verify_digest(Digest(digest): Digest, public_key: &PublicKey, sig: &Sig) -> anyhow::Result<()> {
    let Sig::Secp256k1(sig) = sig else {
        anyhow::bail!("mismatched signature type")
    };
    if !cfg!(test) {
        let message = secp256k1::Message::from_digest(digest);
        SECP.with(|secp| secp.verify_ecdsa(&message, sig, public_key))?;
    }
    Ok(())
}

pub fn replica_secret_key(replica_id: ReplicaId) -> SecretKey {
    let mut bytes = [0; 32];
    let tag = format!("replica#{replica_id}");
    let tag = tag.as_bytes();
    bytes[..tag.len()].copy_from_slice(tag);
    SecretKey::from_byte_array(&bytes).unwrap()
}

pub fn public_key(secret_key: &SecretKey) -> PublicKey {
    SECP.with(|secp| secret_key.public_key(secp))
}

pub struct ReplicaConfig {
    pub secret_key: SecretKey,
    pub public_keys: Vec<PublicKey>,
}

impl ReplicaConfig {
    pub fn new(replica_id: ReplicaId, num_replica: ReplicaId) -> Self {
        let secret_keys = (0..num_replica).map(replica_secret_key).collect::<Vec<_>>();
        Self {
            public_keys: secret_keys.iter().map(public_key).collect(),
            secret_key: secret_keys[replica_id as usize],
        }
    }
}

impl Encode for Sig {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        match self {
            Self::Uninitialized => Err(bincode::error::EncodeError::Other(
                "cannot encode uninitialized signature",
            )),
            // if this long tag affects performance, consider shorter ones
            Self::Secp256k1(sig) => {
                Encode::encode("secp256k1", encoder)?;
                Encode::encode(&sig.serialize_compact(), encoder)
            }
        }
    }
}

impl<C> Decode<C> for Sig {
    fn decode<D: bincode::de::Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        match &*<String>::decode(decoder)? {
            "secp256k1" => {
                let bytes = <[u8; 64]>::decode(decoder)?;
                match secp256k1::ecdsa::Signature::from_compact(&bytes) {
                    Ok(sig) => Ok(Self::Secp256k1(sig)),
                    Err(err) => Err(DecodeError::OtherString(err.to_string())),
                }
            }
            _ => Err(DecodeError::Other("invalid signature type")),
        }
    }
}

impl<'de, C> BorrowDecode<'de, C> for Sig {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}
