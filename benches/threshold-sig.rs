#![allow(clippy::unit_arg)]
use std::iter::repeat_with;

use bft_kit::crypto::{
    Digest, DigestHash, SecretKey, UpdateHash, sign,
    threshold::{self, ThresholdCryptoSigShare},
    verify,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::random;

struct Message([u8; 64]);

impl UpdateHash for Message {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.0)
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sign");
    group.bench_function("Secp256k1", |b| {
        let secret_key = secp256k1_secret_key();
        let message = Message(random());
        b.iter(|| black_box(sign(&message, &secret_key)))
    });
    group.bench_function("Ed25519", |b| {
        let secret_key = SecretKey::Ed25519(ed25519_dalek::SigningKey::from_bytes(&random()));
        let message = Message(random());
        b.iter(|| black_box(sign(&message, &secret_key)))
    });
    group.bench_function("ThresholdCrypto", |b| {
        let secret_key = rand07::random::<threshold_crypto::SecretKey>();
        let message = random::<[u8; 32]>();
        b.iter(|| black_box(secret_key.sign(message)))
    });
    group.finish();

    let mut group = c.benchmark_group("PartialVerify");
    group.bench_function("Secp256k1", |b| {
        let secret_key = secp256k1_secret_key();
        let message = Message(random());
        let sig = sign(&message, &secret_key);
        let public_key = secret_key.public_key();
        b.iter(|| black_box(verify(&message, &public_key, &sig).unwrap()))
    });
    group.bench_function("Ed25519", |b| {
        let secret_key = SecretKey::Ed25519(ed25519_dalek::SigningKey::from_bytes(&random()));
        let message = Message(random());
        let sig = sign(&message, &secret_key);
        let public_key = secret_key.public_key();
        b.iter(|| black_box(verify(&message, &public_key, &sig).unwrap()))
    });
    group.bench_function("ThresholdCrypto", |b| {
        let secret_key_set = threshold_crypto::SecretKeySet::random(2, &mut rand07::thread_rng());
        let secret_key_share = secret_key_set.secret_key_share(0);
        let message = random::<[u8; 32]>();
        let sig_share = secret_key_share.sign(message);
        let public_key_set = secret_key_set.public_keys();
        b.iter(|| {
            black_box(assert!(
                public_key_set
                    .public_key_share(0)
                    .verify(&sig_share, message)
            ))
        })
    });
    group.finish();

    let mut group = c.benchmark_group("Aggregate");
    for f in [1, 10, 33] {
        let threshold = 2 * f + 1;
        let message = Message(random());

        let (partial_sigs, public_master_key) = prepare_aggregate_vec(threshold, &message);
        group.bench_function(BenchmarkId::new("Vec", threshold), |b| {
            b.iter(|| black_box(aggregate(&partial_sigs, &public_master_key)))
        });

        let (partial_sigs, public_master_key) =
            prepare_aggregate_threshold_crypto(threshold, message.digest());
        group.bench_function(BenchmarkId::new("ThresholdCrypto", threshold), |b| {
            b.iter(|| black_box(aggregate(&partial_sigs, &public_master_key)))
        });

        let (partial_sigs, signers, key_share) =
            prepare_aggregate_givre((3 * f + 1) as _, threshold as _, &message);
        group.bench_function(BenchmarkId::new("Givre", threshold), |b| {
            b.iter(|| {
                black_box(aggregate_givre(
                    &partial_sigs,
                    &signers,
                    &key_share,
                    &message,
                ))
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("Verify");
    for f in [1, 10, 33] {
        let threshold = 2 * f + 1;
        let message = Message(random());

        let (partial_sigs, public_master_key) = prepare_aggregate_vec(threshold, &message);
        let sig = aggregate(&partial_sigs, &public_master_key);
        group.bench_function(BenchmarkId::new("Vec", threshold), |b| {
            b.iter(|| black_box(threshold::verify(&message, &sig, &public_master_key).unwrap()))
        });

        let (partial_sigs, public_master_key) =
            prepare_aggregate_threshold_crypto(threshold, message.digest());
        let sig = aggregate(&partial_sigs, &public_master_key);
        group.bench_function(BenchmarkId::new("ThresholdCrypto", threshold), |b| {
            b.iter(|| black_box(threshold::verify(&message, &sig, &public_master_key).unwrap()))
        });

        let (partial_sigs, signers, key_share) =
            prepare_aggregate_givre((3 * f + 1) as _, threshold as _, &message);
        let sig = aggregate_givre(&partial_sigs, &signers, &key_share, &message);
        group.bench_function(BenchmarkId::new("Givre", threshold), |b| {
            b.iter(|| black_box(threshold::verify(&message, &sig, &public_master_key).unwrap()))
        });
    }
}

fn aggregate(
    sigs: &[threshold::PartialSig],
    public_master_key: &threshold::PublicMasterKey,
) -> threshold::Sig {
    threshold::aggregate(
        sigs.iter()
            .enumerate()
            .map(|(i, partial_sig)| (i as _, partial_sig.clone())),
        public_master_key,
    )
    .unwrap()
}

fn aggregate_givre(
    partial_sigs: &[threshold::commit::PartialSig],
    signers: &[(threshold::Index, threshold::GivrePublicCommitments)],
    key_share: &threshold::GivreKeyShare,
    message: &Message,
) -> threshold::Sig {
    threshold::commit::aggregate(
        partial_sigs
            .iter()
            .enumerate()
            .map(|(i, sig)| (i as _, sig.clone(), signers[i].1)),
        key_share,
        message,
    )
    .unwrap()
}

fn prepare_aggregate_vec(
    threshold: threshold::Index,
    message: &Message,
) -> (Vec<threshold::PartialSig>, threshold::PublicMasterKey) {
    let (sigs, public_keys) = repeat_with(secp256k1_secret_key)
        .take(threshold as _)
        .map(|secret_key| {
            let sig = sign(message, &secret_key);
            (threshold::PartialSig::Vec(sig), secret_key.public_key())
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let master_key = threshold::PublicMasterKey::Vec(public_keys, threshold);
    (sigs, master_key)
}

fn prepare_aggregate_threshold_crypto(
    threshold: threshold::Index,
    digest: Digest,
) -> (Vec<threshold::PartialSig>, threshold::PublicMasterKey) {
    let secret_key_set =
        threshold_crypto::SecretKeySet::random((threshold - 1) as _, &mut rand07::thread_rng());
    let sigs = (0..threshold)
        .map(|i| {
            threshold::PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(
                secret_key_set
                    .secret_key_share(i as usize)
                    .sign(digest.0)
                    .into(),
            ))
        })
        .collect::<Vec<_>>();
    (
        sigs,
        threshold::PublicMasterKey::ThresholdCrypto(secret_key_set.public_keys()),
    )
}

fn prepare_aggregate_givre(
    n: threshold::Index,
    threshold: threshold::Index,
    message: &Message,
) -> (
    Vec<threshold::commit::PartialSig>,
    Vec<(threshold::Index, threshold::GivrePublicCommitments)>,
    threshold::GivreKeyShare,
) {
    let key_shares = givre::trusted_dealer::builder(n)
        .set_threshold(Some(threshold))
        .generate_shares(&mut rand08::thread_rng())
        .unwrap();
    let (secret_nonces, public_commitments) = key_shares
        .iter()
        .take(threshold as _) // first `threshold` participants are joining
        .map(|key_share| {
            givre::signing::round1::commit::<threshold::GivreCiphersuite>(
                &mut rand08::thread_rng(),
                key_share,
            )
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let signers = public_commitments
        .iter()
        .copied()
        .enumerate()
        .map(|(i, public_commitments)| {
            (
                i as _,
                threshold::GivrePublicCommitments(public_commitments),
            )
        })
        .collect::<Vec<_>>();
    let sig_shares = secret_nonces
        .into_iter()
        .zip(&key_shares)
        .map(|(nonce, key_share)| {
            threshold::commit::sign(message, key_share, nonce, &signers).unwrap()
        })
        .collect();
    (sig_shares, signers, key_shares.into_iter().next().unwrap())
}

fn secp256k1_secret_key() -> SecretKey {
    loop {
        if let Ok(secret_key) = secp256k1::SecretKey::from_byte_array(&random()) {
            break SecretKey::Secp256k1(secret_key);
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
