use std::fmt::{Debug, Display};

use bincode::{BorrowDecode, Decode, Encode, error::DecodeError};
use sha2::Digest as _;

use crate::fmt_bytes;

pub mod cert;
pub mod threshold;

#[derive(Clone, PartialEq, Eq, Hash, Default, Encode, Decode)]
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

impl From<sha2::Sha256> for Digest {
    fn from(value: sha2::Sha256) -> Self {
        Digest(value.finalize().into())
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

impl UpdateHash for Digest {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.0)
    }
}

// the canonical digest in this codebase is 32 byte SHA256
// can swap to keccak256 in the future if have a (very) good reason
pub trait DigestHash {
    fn digest(&self) -> Digest;
}

impl<T: UpdateHash> DigestHash for T {
    fn digest(&self) -> Digest {
        let mut state = sha2::Sha256::new();
        self.update(&mut state);
        state.into()
    }
}

// wire type for signature
#[derive(Clone, Default)]
pub enum Sig {
    #[default]
    Uninitialized,
    Secp256k1(secp256k1::ecdsa::Signature),
    Ed25519(ed25519_dalek::Signature),
}

impl Display for Sig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Uninitialized => write!(f, "Uninitialized"),
            Self::Secp256k1(sig) => {
                write!(f, "Secp256k1")?;
                fmt_bytes(&sig.serialize_compact(), f)
            }
            Self::Ed25519(sig) => {
                write!(f, "Ed25519")?;
                fmt_bytes(&sig.to_bytes(), f)
            }
        }
    }
}

impl Debug for Sig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

#[derive(Debug, Clone)]
pub enum SecretKey {
    Secp256k1(secp256k1::SecretKey),
    Ed25519(ed25519_dalek::SigningKey),
}

#[derive(Debug, Clone)]
pub enum PublicKey {
    Secp256k1(secp256k1::PublicKey),
    Ed25519(ed25519_dalek::VerifyingKey),
}

thread_local!(static SECP: secp256k1::Secp256k1<secp256k1::All> = secp256k1::Secp256k1::new());

pub fn sign(message: &impl UpdateHash, secret_key: &SecretKey) -> Sig {
    if cfg!(test) {
        return Default::default();
    }
    match secret_key {
        SecretKey::Secp256k1(secret_key) => {
            let message = secp256k1::Message::from_digest(message.digest().0);
            Sig::Secp256k1(SECP.with(|secp| secp.sign_ecdsa(&message, secret_key)))
        }
        SecretKey::Ed25519(signing_key) => {
            let mut state = sha2::Sha512::new();
            message.update(&mut state);
            let sig = signing_key
                .sign_prehashed(state, None)
                // TODO
                .unwrap();
            Sig::Ed25519(sig)
        }
    }
}

pub fn verify(message: &impl UpdateHash, public_key: &PublicKey, sig: &Sig) -> anyhow::Result<()> {
    if cfg!(test) {
        return Ok(());
    }
    match (public_key, sig) {
        (PublicKey::Secp256k1(public_key), Sig::Secp256k1(sig)) => {
            let message = secp256k1::Message::from_digest(message.digest().0);
            SECP.with(|secp| secp.verify_ecdsa(&message, sig, public_key))?
        }
        (PublicKey::Ed25519(verifying_key), Sig::Ed25519(sig)) => {
            let mut state = sha2::Sha512::new();
            message.update(&mut state);
            verifying_key.verify_prehashed(state, None, sig)?;
        }
        _ => anyhow::bail!("mismatched signature type"),
    }
    Ok(())
}

pub fn verify_digest(
    Digest(digest): Digest,
    public_key: &PublicKey,
    sig: &Sig,
) -> anyhow::Result<()> {
    let (PublicKey::Secp256k1(public_key), Sig::Secp256k1(sig)) = (public_key, sig) else {
        anyhow::bail!("unsupported public key and/or signature types")
    };
    let message = secp256k1::Message::from_digest(digest);
    SECP.with(|secp| secp.verify_ecdsa(&message, sig, public_key))?;
    Ok(())
}

pub fn peer_secret_key(index: usize) -> SecretKey {
    let mut bytes = [0; 32];
    let tag = format!("peer#{index}");
    let tag = tag.as_bytes();
    bytes[..tag.len()].copy_from_slice(tag);
    SecretKey::Secp256k1(secp256k1::SecretKey::from_byte_array(&bytes).unwrap())
}

impl SecretKey {
    pub fn public_key(&self) -> PublicKey {
        match self {
            Self::Secp256k1(secret_key) => {
                PublicKey::Secp256k1(SECP.with(|secp| secret_key.public_key(secp)))
            }
            Self::Ed25519(signing_key) => PublicKey::Ed25519(signing_key.verifying_key()),
        }
    }
}

pub struct PeerConfig {
    pub secret_key: SecretKey,
    pub public_keys: Vec<PublicKey>,
}

impl PeerConfig {
    pub fn new(index: usize, num_peer: usize) -> Self {
        let secret_keys = (0..num_peer).map(peer_secret_key).collect::<Vec<_>>();
        Self {
            public_keys: secret_keys.iter().map(SecretKey::public_key).collect(),
            secret_key: secret_keys[index].clone(),
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
            // if these long tags affects performance, switch to shorter ones
            Self::Secp256k1(sig) => {
                Encode::encode("secp256k1", encoder)?;
                Encode::encode(&sig.serialize_compact(), encoder)
            }
            Self::Ed25519(sig) => {
                Encode::encode("ed25519", encoder)?;
                Encode::encode(&sig.to_bytes(), encoder)
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
            "ed25519" => {
                let bytes = <[u8; 64]>::decode(decoder)?;
                Ok(Self::Ed25519(ed25519_dalek::Signature::from_bytes(&bytes)))
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
