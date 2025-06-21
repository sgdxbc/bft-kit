//! https://github.com/sgdxbc/bft-kit/discussions/2
use std::{collections::HashMap, hash::Hash};

use bincode::{
    BorrowDecode, Decode, Encode,
    de::{BorrowDecoder, Decoder},
    enc::Encoder,
    error::{DecodeError, EncodeError},
};

use super::{Digest, DigestHash, UpdateHash};

// note on threshold definition
// threshold_crypto defines threshold t as the maximum number of faulty
// participants, and combine signature with t + 1 partial signatures
// givre defines threshold t as the minimum number of signing participants
// (actually the exact number, as the signing participants are selected before
// signing), and combine signature with t partial signatures
// we follow the givre convention here because it's a bit consistent with the
// targeted use case i.e. permissioned blockchain, so that the threshold is set
// to n - f, the same as a regular (super)majority quorum size

pub type Index = givre::SignerIndex;

// newtypes for (partial) signatures serialization
#[derive(Debug, Clone)]
// box to prevent imbalance enum size below
// box here instead of in enum for better pattern matching ergonomics
pub struct ThresholdCryptoSig(pub Box<threshold_crypto::Signature>);
#[derive(Debug, Clone)]
pub struct ThresholdCryptoSigShare(pub Box<threshold_crypto::SignatureShare>);
#[derive(Debug, Clone)]
pub struct GivreSig(pub Box<givre::signing::aggregate::Signature<GivreCiphersuite>>);
#[derive(Debug, Clone)]
pub struct GivreSigShare(pub givre::signing::round2::SigShare<GivreCurve>);

#[derive(Debug, Clone, Encode, Decode)]
pub enum PartialSig {
    Vec(super::Sig),
    ThresholdCrypto(ThresholdCryptoSigShare),
}

#[derive(Debug)]
pub enum PartialSecretKey {
    Vec(super::SecretKey),
    ThresholdCrypto(threshold_crypto::SecretKeyShare),
}

// conventionally a (Partial)PublicKey type is provided to verify a partial
// signature
// however, since we always need to (be prepared for) verify partial signatures
// from every participants, what's the difference between a public master key
// and a vector of (partial) public keys?

#[derive(Debug)]
pub enum PublicMasterKey {
    Vec(Vec<super::PublicKey>, Index), // (keys, threshold)
    ThresholdCrypto(threshold_crypto::PublicKeySet),
    Givre(GivrePublicKey),
}

pub fn partial_sign(message: &impl UpdateHash, secret_key: &PartialSecretKey) -> PartialSig {
    match secret_key {
        PartialSecretKey::Vec(secret_key) => PartialSig::Vec(super::sign(message, secret_key)),
        // TODO avoid double hash (the other one is inside threshold_crypto)
        PartialSecretKey::ThresholdCrypto(secret_key_share) => PartialSig::ThresholdCrypto(
            ThresholdCryptoSigShare(secret_key_share.sign(message.digest().0).into()),
        ),
    }
}

pub fn partial_verify(
    message: &impl UpdateHash,
    master_key: &PublicMasterKey,
    index: Index,
    partial_sig: &PartialSig,
) -> anyhow::Result<()> {
    match (master_key, partial_sig) {
        (PublicMasterKey::Vec(public_keys, _), PartialSig::Vec(sig)) => {
            super::verify(message, &public_keys[index as usize], sig)?
        }
        (
            PublicMasterKey::ThresholdCrypto(public_key_set),
            PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(sig_share)),
        ) => {
            let valid = public_key_set
                .public_key_share(index as usize)
                .verify(sig_share, message.digest().0);
            anyhow::ensure!(valid)
        }
        // TODO make exclusive error type
        _ => anyhow::bail!("unimplemented"),
    }
    Ok(())
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum Sig {
    Vec(Vec<(Index, super::Sig)>),
    ThresholdCrypto(ThresholdCryptoSig),
    Givre(GivreSig),
}

pub fn aggregate(
    partial_sigs: impl Iterator<Item = (Index, PartialSig)>,
    public_master_key: &PublicMasterKey,
) -> anyhow::Result<Sig> {
    let sig = match public_master_key {
        PublicMasterKey::Givre(_) => anyhow::bail!("unimplemented"),
        &PublicMasterKey::Vec(_, threshold) => {
            let mut index_sigs = Vec::new();
            for (index, partial_sig) in partial_sigs {
                let PartialSig::Vec(sig) = partial_sig else {
                    anyhow::bail!("unexpected partial signature type")
                };
                index_sigs.push((index, sig))
            }
            anyhow::ensure!(index_sigs.len() >= threshold as usize);
            Sig::Vec(index_sigs)
        }
        PublicMasterKey::ThresholdCrypto(public_key_set) => {
            let mut indexes = Vec::new();
            let mut sig_shares = Vec::new();
            for (index, partial_sig) in partial_sigs {
                let PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(sig_share)) = partial_sig
                else {
                    anyhow::bail!("unexpected partial signature type")
                };
                indexes.push(index as usize);
                sig_shares.push(*sig_share)
            }
            Sig::ThresholdCrypto(ThresholdCryptoSig(
                public_key_set
                    .combine_signatures(indexes.into_iter().zip(&sig_shares))
                    .map_err(|err| anyhow::format_err!(err))?
                    .into(),
            ))
        }
    };
    Ok(sig)
}

// givre types/type aliases
// type aliases are mostly for convenient and self-contained `use` i.e. only
// need to `use` from this crate instead of directly from givre
// types also for implementing serialization
type GivreCiphersuite = givre::ciphersuite::Secp256k1;
type GivreCurve = <GivreCiphersuite as givre::Ciphersuite>::Curve;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GivrePublicCommitments(pub givre::signing::round1::PublicCommitments<GivreCurve>);
type GivreSecretNonces = givre::signing::round1::SecretNonces<GivreCurve>;
type GivreKeyShare = givre::KeyShare<GivreCurve>;
type GivrePublicKey = givre::ciphersuite::NormalizedPoint<
    GivreCiphersuite,
    givre::generic_ec::NonZero<givre::generic_ec::Point<GivreCurve>>,
>;

pub type KeyShare = GivreKeyShare;

pub type Commitments = GivrePublicCommitments;

#[derive(Default)]
pub struct CommitStore {
    pairs: HashMap<GivrePublicCommitments, GivreSecretNonces>,
}

impl CommitStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn commit(&mut self, key_share: &KeyShare) -> GivrePublicCommitments {
        let (secret_nonces, public_commitments) = givre::signing::round1::commit::<GivreCiphersuite>(
            &mut rand08::thread_rng(),
            key_share,
        );
        let commitments = GivrePublicCommitments(public_commitments);
        let replaced = self.pairs.insert(commitments, secret_nonces);
        assert!(replaced.is_none());
        commitments
    }
}

pub mod commit {
    use crate::crypto::DigestHash as _;

    use super::{
        CommitStore, Commitments, GivreCiphersuite, GivrePublicCommitments, GivreSig,
        GivreSigShare, Index, KeyShare, Sig, UpdateHash,
    };

    pub type PartialSig = GivreSigShare;

    pub fn partial_sign(
        message: &impl UpdateHash,
        key_share: &KeyShare,
        index: Index,
        commit_store: &mut CommitStore,
        signer_commitments: &[(Index, Commitments)],
    ) -> anyhow::Result<PartialSig> {
        let signers = signer_commitments
            .iter()
            .map(|&(index, GivrePublicCommitments(public_commitments))| (index, public_commitments))
            .collect::<Vec<_>>();
        let secret_nonces = commit_store
            .pairs
            .remove(
                &signer_commitments
                    .iter()
                    .find(|&&(other_index, _)| other_index == index)
                    .ok_or_else(|| anyhow::anyhow!("commitments not found"))?
                    .1,
            )
            .ok_or_else(|| anyhow::anyhow!("commitments not found"))?;
        let sig_share = givre::signing::round2::sign::<GivreCiphersuite>(
            key_share,
            secret_nonces,
            &message.digest().0,
            &signers,
        )?;
        Ok(GivreSigShare(sig_share))
    }

    pub fn aggregate(
        partial_sig_commitments: impl Iterator<Item = (Index, PartialSig, Commitments)>,
        key_share: &KeyShare,
        message: &impl UpdateHash,
    ) -> anyhow::Result<Sig> {
        let sig = givre::signing::aggregate::aggregate::<GivreCiphersuite>(
            key_share.as_ref(),
            &partial_sig_commitments
                .map(
                    |(
                        index,
                        GivreSigShare(sig_share),
                        GivrePublicCommitments(public_commitments),
                    )| (index, public_commitments, sig_share),
                )
                .collect::<Vec<_>>(),
            &message.digest().0,
        )?;
        Ok(Sig::Givre(GivreSig(sig.into())))
    }
}

impl PublicMasterKey {
    pub fn givre(key_share: &GivreKeyShare) -> PublicMasterKey {
        Self::Givre(<GivreCiphersuite as givre::Ciphersuite>::normalize_point(
            key_share.shared_public_key(),
        ))
    }
}

pub fn verify(
    message: &impl DigestHash,
    sig: &Sig,
    master_key: &PublicMasterKey,
) -> anyhow::Result<()> {
    verify_digest(message.digest(), sig, master_key)
}

pub fn verify_digest(
    Digest(digest): Digest,
    sig: &Sig,
    master_key: &PublicMasterKey,
) -> anyhow::Result<()> {
    match (sig, master_key) {
        (Sig::Vec(sigs), PublicMasterKey::Vec(public_keys, threshold)) => {
            let threshold = *threshold as usize;
            // deduplicate partial signatures by index
            let sigs = sigs
                .iter()
                .map(|(index, sig)| (*index, sig))
                .collect::<HashMap<_, _>>();
            let count = sigs
                .into_iter()
                .filter(|&(index, sig)| {
                    super::verify_digest(Digest(digest), &public_keys[index as usize], sig).is_ok()
                })
                .take(threshold + 1)
                .count();
            anyhow::ensure!(count == threshold)
        }
        (
            Sig::ThresholdCrypto(ThresholdCryptoSig(sig)),
            PublicMasterKey::ThresholdCrypto(public_key_set),
        ) => {
            anyhow::ensure!(public_key_set.public_key().verify(sig, digest))
        }
        (Sig::Givre(GivreSig(sig)), PublicMasterKey::Givre(public_key)) => {
            sig.verify(public_key, &digest)?
        }
        _ => anyhow::bail!("unmatched public key and signature types"),
    }
    Ok(())
}

pub fn givre_peer_key_shares(num_peer: usize, num_faulty: usize) -> Vec<KeyShare> {
    givre::trusted_dealer::builder(num_peer as _)
        .set_threshold(Some((num_peer - num_faulty) as _))
        .generate_shares(
            &mut <rand08::rngs::StdRng as rand08::SeedableRng>::seed_from_u64(0x117418),
        )
        .unwrap()
}

fn into_decode(err: impl ToString) -> DecodeError {
    DecodeError::OtherString(err.to_string())
}

impl Encode for ThresholdCryptoSig {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        Encode::encode(&self.0.to_bytes(), encoder)
    }
}

impl<C> Decode<C> for ThresholdCryptoSig {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        // otherwise compile error
        // TODO report issue to clippy
        let bytes = Decode::decode(decoder)?;
        #[allow(clippy::needless_borrows_for_generic_args)]
        threshold_crypto::Signature::from_bytes(&bytes)
            .map(Into::into)
            .map(Self)
            .map_err(into_decode)
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
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        #[allow(clippy::needless_borrows_for_generic_args)]
        threshold_crypto::SignatureShare::from_bytes(&Decode::decode(decoder)?)
            .map(Into::into)
            .map(Self)
            .map_err(into_decode)
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
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes = <Vec<_>>::decode(decoder)?;
        givre::signing::aggregate::Signature::<GivreCiphersuite>::read_from_slice(&bytes)
            .map(Into::into)
            .map(Self)
            .ok_or("invalid signature")
            .map_err(into_decode)
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
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let bytes = <Vec<_>>::decode(decoder)?;
        <GivreCiphersuite as givre::Ciphersuite>::deserialize_scalar(&bytes)
            .map(givre::signing::round2::SigShare)
            .map(Self)
            .map_err(into_decode)
    }
}

impl<'de, C> BorrowDecode<'de, C> for GivreSigShare {
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}

impl Encode for GivrePublicCommitments {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let serialize = <GivreCiphersuite as givre::Ciphersuite>::serialize_point;
        Encode::encode(&serialize(&self.0.hiding_comm).to_vec(), encoder)?;
        Encode::encode(&serialize(&self.0.binding_comm).to_vec(), encoder)?;
        Ok(())
    }
}

impl<C> Decode<C> for GivrePublicCommitments {
    fn decode<D: Decoder<Context = C>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let deserialize = <GivreCiphersuite as givre::Ciphersuite>::deserialize_point;
        let hiding_comm = deserialize(&<Vec<_>>::decode(decoder)?).map_err(into_decode)?;
        let binding_comm = deserialize(&<Vec<_>>::decode(decoder)?).map_err(into_decode)?;
        Ok(Self(givre::signing::round1::PublicCommitments {
            hiding_comm,
            binding_comm,
        }))
    }
}

impl<'de, C> BorrowDecode<'de, C> for GivrePublicCommitments {
    fn borrow_decode<D: BorrowDecoder<'de, Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Decode::decode(decoder)
    }
}

impl Hash for GivrePublicCommitments {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hiding_comm.hash(state);
        self.0.binding_comm.hash(state)
    }
}
