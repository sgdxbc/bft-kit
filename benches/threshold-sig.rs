#![allow(clippy::unit_arg)]
use std::iter::repeat_with;

use bft_testbed::crypto::{
    SecretKey, sign,
    threshold::{self, ThresholdCryptoSigShare},
    verify,
};
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use rand::random;

pub fn criterion_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sign");
    group.bench_function("Secp256k1", |b| {
        let secret_key = secp256k1_secret_key();
        let message = random::<[u8; 32]>();
        b.iter(|| black_box(sign(message, &secret_key)))
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
        let message = random::<[u8; 32]>();
        let sig = sign(message, &secret_key);
        let public_key = secret_key.public_key(&secp256k1::Secp256k1::new());
        b.iter(|| black_box(verify(message, &public_key, &sig).unwrap()))
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

    let mut group = c.benchmark_group("Combine");
    for f in [1, 10, 33] {
        let threshold = 2 * f;
        let message = random::<[u8; 32]>();

        let (sigs, _) = prepare_combine_vec(threshold, message);
        group.bench_function(BenchmarkId::new("Vec", threshold), |b| {
            b.iter(|| {
                black_box(combine(
                    threshold::PartialSigs::Vec(Default::default()),
                    &sigs,
                    threshold::AggregateContext::Vec(threshold),
                ))
            })
        });

        let (sigs, public_key_set) = prepare_combine_threshold_crypto(threshold, message);
        group.bench_function(BenchmarkId::new("ThresholdCrypto", threshold), |b| {
            b.iter(|| {
                black_box(combine(
                    threshold::PartialSigs::ThresholdCrypto(Default::default()),
                    &sigs,
                    threshold::AggregateContext::ThresholdCrypto(&public_key_set),
                ))
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("Verify");
    for f in [1, 10, 33] {
        let threshold = 2 * f;
        let message = random::<[u8; 32]>();

        let (sigs, master_key) = prepare_combine_vec(threshold, message);
        let sig = combine(
            threshold::PartialSigs::Vec(Default::default()),
            &sigs,
            threshold::AggregateContext::Vec(threshold),
        );
        group.bench_function(BenchmarkId::new("Vec", threshold), |b| {
            b.iter(|| black_box(threshold::verify(message, &sig, &master_key).unwrap()))
        });

        let (sigs, public_key_set) = prepare_combine_threshold_crypto(threshold, message);
        let sig = combine(
            threshold::PartialSigs::ThresholdCrypto(Default::default()),
            &sigs,
            threshold::AggregateContext::ThresholdCrypto(&public_key_set),
        );
        let master_key = threshold::PublicMasterKey::ThresholdCrypto(public_key_set);
        group.bench_function(BenchmarkId::new("ThresholdCrypto", threshold), |b| {
            b.iter(|| black_box(threshold::verify(message, &sig, &master_key).unwrap()))
        });
    }
}

fn combine(
    mut partial_sigs: threshold::PartialSigs,
    sigs: &[threshold::PartialSig],
    context: threshold::AggregateContext<'_>,
) -> threshold::Sig {
    let mut sig = None;
    for (i, partial_sig) in sigs.iter().enumerate() {
        assert!(sig.is_none());
        sig = partial_sigs
            .add_partial(i, partial_sig.clone(), context)
            .unwrap()
    }
    sig.unwrap()
}

fn prepare_combine_threshold_crypto(
    threshold: usize,
    message: [u8; 32],
) -> (Vec<threshold::PartialSig>, threshold_crypto::PublicKeySet) {
    let secret_key_set =
        threshold_crypto::SecretKeySet::random(threshold, &mut rand07::thread_rng());
    let sigs = (0..=threshold)
        .map(|i| {
            threshold::PartialSig::ThresholdCrypto(ThresholdCryptoSigShare(
                secret_key_set.secret_key_share(i).sign(message).into(),
            ))
        })
        .collect::<Vec<_>>();
    (sigs, secret_key_set.public_keys())
}

fn prepare_combine_vec(
    threshold: usize,
    message: [u8; 32],
) -> (Vec<threshold::PartialSig>, threshold::PublicMasterKey) {
    let secp = secp256k1::Secp256k1::new();
    let (sigs, public_keys) = repeat_with(secp256k1_secret_key)
        .take(threshold + 1)
        .map(|secret_key| {
            let sig = sign(message, &secret_key);
            (
                threshold::PartialSig::Vec(sig),
                secret_key.public_key(&secp),
            )
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();
    let master_key = threshold::PublicMasterKey::Vec(public_keys, threshold);
    (sigs, master_key)
}

fn secp256k1_secret_key() -> secp256k1::SecretKey {
    loop {
        if let Ok(secret_key) = SecretKey::from_byte_array(&random()) {
            break secret_key;
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
