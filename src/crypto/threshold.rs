use std::collections::HashMap;

use bincode::{BorrowDecode, Decode, Encode, error::DecodeError};

pub type Index = usize;

#[derive(Debug, Clone)]
// box to prevent imbalance enum size below
// box here instead of in enum for better pattern matching ergonomics
pub struct ThresholdCryptoSig(pub Box<threshold_crypto::Signature>);

#[derive(Debug, Clone)]
pub struct ThresholdCryptoSigShare(pub Box<threshold_crypto::SignatureShare>);

#[derive(Debug, Clone, Encode, Decode)]
pub enum Sig {
    Vec(Vec<(Index, super::Sig)>),
    ThresholdCrypto(ThresholdCryptoSig),
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum PartialSig {
    Vec(super::Sig),
    ThresholdCrypto(ThresholdCryptoSigShare),
}

#[derive(Debug)]
pub enum PartialSigs {
    Vec(HashMap<Index, super::Sig>),
    ThresholdCrypto(HashMap<Index, threshold_crypto::SignatureShare>),
}

#[derive(Debug)]
pub enum PartialSecretKey {
    Vec(super::SecretKey),
    ThresholdCrypto(threshold_crypto::SecretKeyShare),
}

// conventionally a (Partial)PublicKey type is provided to verify a partial
// signature
// however, since we always need to verify partial signatures from every
// participants, what's the difference between a public master key and a vector
// of (partial) public keys?

#[derive(Debug)]
pub enum PublicMasterKey {
    Vec(Vec<super::PublicKey>, Index), // (keys, threshold)
    ThresholdCrypto(threshold_crypto::PublicKeySet),
}

pub fn sign(message: impl Into<[u8; 32]>, secret_key: &PartialSecretKey) -> PartialSig {
    match secret_key {
        PartialSecretKey::Vec(secret_key) => PartialSig::Vec(super::sign(message, secret_key)),
        PartialSecretKey::ThresholdCrypto(secret_key_share) => PartialSig::ThresholdCrypto(
            ThresholdCryptoSigShare(secret_key_share.sign(message.into()).into()),
        ),
    }
}

pub fn verify_partial(
    message: impl Into<[u8; 32]>,
    master_key: &PublicMasterKey,
    index: usize,
    partial_sig: &PartialSig,
) -> anyhow::Result<()> {
    match (master_key, partial_sig) {
        (PublicMasterKey::Vec(public_keys, _), PartialSig::Vec(sig)) => {
            super::verify(message, &public_keys[index], sig)?
        }
        (
            PublicMasterKey::ThresholdCrypto(public_key_set),
            PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(sig_share)),
        ) => {
            let valid = public_key_set
                .public_key_share(index)
                .verify(sig_share, message.into());
            anyhow::ensure!(valid)
        }
        // TODO make exclusive error type
        _ => anyhow::bail!("unmatched public key and signature types"),
    }
    Ok(())
}

impl PartialSigs {
    pub fn add_partial(
        &mut self,
        index: Index,
        partial_sig: PartialSig,
        master_key: &PublicMasterKey,
    ) -> anyhow::Result<Option<Sig>> {
        Ok(match (self, partial_sig, master_key) {
            (
                Self::Vec(partial_sigs),
                PartialSig::Vec(partial_sig),
                PublicMasterKey::Vec(_, threshold),
            ) => {
                partial_sigs.insert(index, partial_sig);
                if partial_sigs.len() <= *threshold {
                    None
                } else {
                    Some(Sig::Vec(
                        partial_sigs
                            .iter()
                            .map(|(&index, partial_sig)| (index, partial_sig.clone()))
                            .collect(),
                    ))
                }
            }
            (
                Self::ThresholdCrypto(sig_shares),
                PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(sig_share)),
                PublicMasterKey::ThresholdCrypto(public_key_set),
            ) => {
                sig_shares.insert(index, *sig_share);
                if sig_shares.len() <= public_key_set.threshold() {
                    None
                } else {
                    let sig = public_key_set
                        .combine_signatures(sig_shares.iter().map(|(&index, sig)| (index, sig)))
                        .map_err(|err| anyhow::format_err!(err))?;
                    Some(Sig::ThresholdCrypto(ThresholdCryptoSig(sig.into())))
                }
            }
            _ => anyhow::bail!("unmatched public key and signature types"),
        })
    }
}

pub fn verify(
    message: impl Into<[u8; 32]>,
    sig: &Sig,
    master_key: &PublicMasterKey,
) -> anyhow::Result<()> {
    let message = message.into();
    match (sig, master_key) {
        (Sig::Vec(sigs), PublicMasterKey::Vec(public_keys, threshold)) => {
            // deduplicate partial signatures by index
            let sigs = sigs
                .iter()
                .map(|(index, sig)| (*index, sig))
                .collect::<HashMap<_, _>>();
            let count = sigs
                .into_iter()
                .filter(|&(index, sig)| super::verify(message, &public_keys[index], sig).is_ok())
                .take(*threshold + 1)
                .count();
            anyhow::ensure!(count == *threshold + 1)
        }
        (
            Sig::ThresholdCrypto(ThresholdCryptoSig(sig)),
            PublicMasterKey::ThresholdCrypto(public_key_set),
        ) => {
            anyhow::ensure!(public_key_set.public_key().verify(sig, message))
        }
        _ => anyhow::bail!("unmatched public key and signature types"),
    }
    Ok(())
}

impl Encode for ThresholdCryptoSig {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        Encode::encode(&self.0.to_bytes(), encoder)
    }
}

impl<C> Decode<C> for ThresholdCryptoSig {
    fn decode<D: bincode::de::Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        // otherwise compile error
        // TODO report issue to clippy
        #[allow(clippy::needless_borrows_for_generic_args)]
        match threshold_crypto::Signature::from_bytes(&Decode::decode(decoder)?) {
            Ok(sig) => Ok(Self(sig.into())),
            Err(err) => Err(DecodeError::OtherString(err.to_string())),
        }
    }
}

impl<'de, C> BorrowDecode<'de, C> for ThresholdCryptoSig {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}

impl Encode for ThresholdCryptoSigShare {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        Encode::encode(&self.0.to_bytes(), encoder)
    }
}

impl<C> Decode<C> for ThresholdCryptoSigShare {
    fn decode<D: bincode::de::Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        #[allow(clippy::needless_borrows_for_generic_args)]
        match threshold_crypto::SignatureShare::from_bytes(&Decode::decode(decoder)?) {
            Ok(sig) => Ok(Self(sig.into())),
            Err(err) => Err(DecodeError::OtherString(err.to_string())),
        }
    }
}

impl<'de, C> BorrowDecode<'de, C> for ThresholdCryptoSigShare {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}
