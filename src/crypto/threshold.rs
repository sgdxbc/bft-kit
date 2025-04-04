use std::collections::HashMap;

use bincode::{
    BorrowDecode, Decode, Encode,
    de::BorrowDecoder,
    enc::Encoder,
    error::{DecodeError, EncodeError},
};

// note on threshold definition
// threshold_crypto defines threshold as the maximum number of faulty
// participants, and combine signature with threshold + 1 partial signatures
// givre defines threshold as the minimum number of signing participants
// (actually the actual number, as the signing participants are selected before
// signing), and combine signature with threshold partial signatures
// we follow the givre convention here because it's a bit consistent with the
// targeted use case i.e. permissioned blockchain, and the threshold is set to
// n - f as the same to a regular majority quorum size

pub type Index = usize;

#[derive(Debug, Clone)]
// box to prevent imbalance enum size below
// box here instead of in enum for better pattern matching ergonomics
pub struct ThresholdCryptoSig(pub Box<threshold_crypto::Signature>);

#[derive(Debug, Clone)]
pub struct ThresholdCryptoSigShare(pub Box<threshold_crypto::SignatureShare>);

pub type GivreCiphersuite = givre::ciphersuite::Secp256k1;
pub type GivreCurve = <GivreCiphersuite as givre::Ciphersuite>::Curve;

#[derive(Debug, Clone)]
pub struct GivreSig(pub Box<givre::signing::aggregate::Signature<GivreCiphersuite>>);

#[derive(Debug, Clone)]
pub struct GivreSigShare(pub givre::signing::round2::SigShare<GivreCurve>);

pub type GivreKeyShare = givre::KeyShare<GivreCurve>;

type GivrePublicKey = givre::ciphersuite::NormalizedPoint<
    GivreCiphersuite,
    givre::generic_ec::NonZero<givre::generic_ec::Point<GivreCurve>>,
>;

#[derive(Debug, Clone, Encode, Decode)]
pub enum Sig {
    Vec(Vec<(Index, super::Sig)>),
    ThresholdCrypto(ThresholdCryptoSig),
    Givre(GivreSig),
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum PartialSig {
    Vec(super::Sig),
    ThresholdCrypto(ThresholdCryptoSigShare),
    Givre(GivreSigShare),
}

#[derive(Debug)]
pub enum PartialSigs {
    Vec(HashMap<Index, super::Sig>),
    ThresholdCrypto(HashMap<Index, threshold_crypto::SignatureShare>),
    Givre(HashMap<Index, givre::signing::round2::SigShare<GivreCurve>>),
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
    Givre(GivrePublicKey),
}

impl PublicMasterKey {
    pub fn givre(key_share: &GivreKeyShare) -> PublicMasterKey {
        Self::Givre(<GivreCiphersuite as givre::Ciphersuite>::normalize_point(
            key_share.shared_public_key(),
        ))
    }
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
        // we don't implement for givre variant here as it does not support verification
        // of signature shares (yet, as it claims)

        // TODO make exclusive error type
        _ => anyhow::bail!("unimplemented"),
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub enum AggregateContext<'a> {
    Vec(Index), // threshold
    ThresholdCrypto(&'a threshold_crypto::PublicKeySet),
    Givre(GivreAggregateContext<'a>),
}

#[derive(Clone, Copy)]
pub struct GivreAggregateContext<'a> {
    pub key_share: &'a GivreKeyShare,
    pub signers: &'a [(
        givre::SignerIndex,
        givre::signing::round1::PublicCommitments<GivreCurve>,
    )],
    pub message: &'a [u8],
}

impl PartialSigs {
    pub fn add_partial(
        &mut self,
        index: Index,
        partial_sig: PartialSig,
        context: AggregateContext<'_>,
    ) -> anyhow::Result<Option<Sig>> {
        Ok(match (self, partial_sig, context) {
            (
                Self::Vec(partial_sigs),
                PartialSig::Vec(partial_sig),
                AggregateContext::Vec(threshold),
            ) => {
                partial_sigs.insert(index, partial_sig);
                if partial_sigs.len() < threshold {
                    None
                } else {
                    Some(Sig::Vec(partial_sigs.drain().collect()))
                }
            }
            (
                Self::ThresholdCrypto(sig_shares),
                PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(sig_share)),
                AggregateContext::ThresholdCrypto(public_key_set),
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
            (
                Self::Givre(sig_shares),
                PartialSig::Givre(GivreSigShare(sig_share)),
                AggregateContext::Givre(context),
            ) => {
                sig_shares.insert(index, sig_share);
                if sig_shares.len() < context.key_share.min_signers() as usize {
                    None
                } else {
                    let mut signers = Vec::new();
                    for &(index, public_commitments) in context.signers {
                        let Some(sig_share) = sig_shares.remove(&(index as usize)) else {
                            anyhow::bail!("missing signature share for index {index}")
                        };
                        signers.push((index, public_commitments, sig_share))
                    }
                    Some(Sig::Givre(GivreSig(
                        givre::signing::aggregate::aggregate(
                            context.key_share.as_ref(),
                            &signers,
                            context.message,
                        )?
                        .into(),
                    )))
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
            anyhow::ensure!(count == *threshold)
        }
        (
            Sig::ThresholdCrypto(ThresholdCryptoSig(sig)),
            PublicMasterKey::ThresholdCrypto(public_key_set),
        ) => {
            anyhow::ensure!(public_key_set.public_key().verify(sig, message))
        }
        (Sig::Givre(GivreSig(sig)), PublicMasterKey::Givre(public_key)) => {
            sig.verify(public_key, &message)?
        }
        _ => anyhow::bail!("unmatched public key and signature types"),
    }
    Ok(())
}

impl Encode for ThresholdCryptoSig {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
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
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}

impl Encode for ThresholdCryptoSigShare {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
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
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}

impl Encode for GivreSig {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let mut bytes =
            vec![0; givre::signing::aggregate::Signature::<GivreCiphersuite>::serialized_len()];
        self.0.write_to_slice(&mut bytes);
        Encode::encode(&bytes, encoder)
    }
}

impl<C> Decode<C> for GivreSig {
    fn decode<D: bincode::de::Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes = <Vec<_>>::decode(decoder)?;
        match givre::signing::aggregate::Signature::<GivreCiphersuite>::read_from_slice(&bytes) {
            Some(sig) => Ok(Self(sig.into())),
            None => Err(DecodeError::Other("invalid signature")),
        }
    }
}

impl<'de, C> BorrowDecode<'de, C> for GivreSig {
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}

impl Encode for GivreSigShare {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        Encode::encode(
            &<GivreCiphersuite as givre::Ciphersuite>::serialize_scalar(&self.0.0).to_vec(),
            encoder,
        )
    }
}

impl<C> Decode<C> for GivreSigShare {
    fn decode<D: bincode::de::Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes = <Vec<_>>::decode(decoder)?;
        match <GivreCiphersuite as givre::Ciphersuite>::deserialize_scalar(&bytes) {
            Ok(scalar) => Ok(Self(givre::signing::round2::SigShare(scalar))),
            Err(err) => Err(DecodeError::OtherString(err.to_string())),
        }
    }
}

impl<'de, C> BorrowDecode<'de, C> for GivreSigShare {
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}
