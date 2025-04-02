use bincode::{Decode, Encode};

pub type Index = usize;

#[derive(Debug, Clone, Encode, Decode)]
pub enum Sig {
    Vec(Vec<(Index, super::Sig)>),
    Combined(super::Sig),
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum PartialSig {
    Vec(super::Sig),
    Combined(super::Sig),
}

pub enum PartialSigs {
    Vec(Vec<(Index, super::Sig)>),
    Combined(Vec<(Index, threshold_crypto::SignatureShare)>),
}

pub enum SecretKey {
    Vec(super::SecretKey),
    Combined(threshold_crypto::SecretKeyShare),
}

pub enum PublicMasterKey {
    Vec(Vec<super::PublicKey>, Index), // (keys, threshold)
    Combined(threshold_crypto::PublicKeySet),
}

pub fn sign(message: impl Into<[u8; 32]>, secret_key: &SecretKey) -> PartialSig {
    match secret_key {
        SecretKey::Vec(secret_key) => PartialSig::Vec(super::sign(message, secret_key)),
        SecretKey::Combined(secret_key) => PartialSig::Combined(super::Sig(
            secret_key.sign(message.into()).to_bytes().to_vec(),
        )),
    }
}

pub fn verify_partial(
    message: impl Into<[u8; 32]>,
    public_key_set: &PublicMasterKey,
    index: usize,
    partial_sig: &PartialSig,
) -> anyhow::Result<()> {
    match (public_key_set, partial_sig) {
        (PublicMasterKey::Vec(public_keys, _), PartialSig::Vec(sig)) => {
            super::verify(message, &public_keys[index], sig)?
        }
        (PublicMasterKey::Combined(public_key_set), PartialSig::Combined(sig)) => {
            let valid = public_key_set
                .public_key_share(index)
                .verify(&decode_signature_share(sig)?, message.into());
            anyhow::ensure!(valid)
        }
        // TODO make exclusive error type
        _ => anyhow::bail!("unmatched public key and signature types"),
    }
    Ok(())
}

fn decode_signature_share(
    super::Sig(sig): &super::Sig,
) -> anyhow::Result<threshold_crypto::SignatureShare> {
    let Ok(sig) = <[u8; threshold_crypto::SIG_SIZE]>::try_from(sig.clone()) else {
        anyhow::bail!("incorrect signature size")
    };
    let sig = threshold_crypto::SignatureShare::from_bytes(sig)
        .map_err(|err| anyhow::format_err!(err))?;
    Ok(sig)
}

impl PartialSigs {
    pub fn push(&mut self, index: Index, partial_sig: PartialSig) -> anyhow::Result<()> {
        match (self, partial_sig) {
            (Self::Vec(partial_sigs), PartialSig::Vec(partial_sig)) => {
                partial_sigs.push((index, partial_sig))
            }
            (Self::Combined(partial_sigs), PartialSig::Combined(partial_sig)) => {
                // a bit inefficient: any partial signature that get `push`ed is probably
                // verified first, where it has always been decoded
                partial_sigs.push((index, decode_signature_share(&partial_sig)?))
            }
            _ => anyhow::bail!("unmatched public key and signature types"),
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Vec(partial_sigs) => partial_sigs.len(),
            Self::Combined(partial_sigs) => partial_sigs.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn combine(partial_sigs: &PartialSigs, master_key: &PublicMasterKey) -> anyhow::Result<Sig> {
    Ok(match (partial_sigs, master_key) {
        (PartialSigs::Vec(partial_sigs), PublicMasterKey::Vec(_, threshold)) => {
            anyhow::ensure!(partial_sigs.len() >= threshold);
            Sig::Vec(partial_sigs.clone())
        }
        (PartialSigs::Combined(partial_sigs), PublicMasterKey::Combined(master_key)) => {
            let sig = master_key
                .combine_signatures(
                    partial_sigs
                        .iter()
                        .map(|(index, partial_sig)| (index, partial_sig)),
                )
                .map_err(|err| anyhow::format_err!(err))?;
            Sig::Combined(super::Sig(sig.to_bytes().to_vec()))
        }
        _ => anyhow::bail!("unmatched public key and signature types"),
    })
}

pub fn verify(
    message: impl Into<[u8; 32]>,
    sig: &Sig,
    master_key: &PublicMasterKey,
) -> anyhow::Result<()> {
    let message = message.into();
    match (sig, master_key) {
        (Sig::Vec(sigs), PublicMasterKey::Vec(public_keys, threshold)) => {
            let count = sigs
                .iter()
                .filter(|(index, sig)| super::verify(message, &public_keys[*index], sig).is_ok())
                .take(*threshold)
                .count();
            anyhow::ensure!(count == *threshold)
        }
        (Sig::Combined(super::Sig(sig)), PublicMasterKey::Combined(public_key_set)) => {
            let Ok(sig) = <[u8; threshold_crypto::SIG_SIZE]>::try_from(sig.clone()) else {
                anyhow::bail!("incorrect signature size")
            };
            let sig = threshold_crypto::Signature::from_bytes(sig)
                .map_err(|err| anyhow::format_err!(err))?;
            anyhow::ensure!(public_key_set.public_key().verify(&sig, message))
        }
        _ => anyhow::bail!("unmatched public key and signature types"),
    }
    Ok(())
}
