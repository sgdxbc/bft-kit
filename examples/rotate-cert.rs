use std::fs::write;

fn main() -> anyhow::Result<()> {
    // https://docs.rs/rcgen/latest/rcgen/#example
    use rcgen::{CertifiedKey, generate_simple_self_signed};
    let subject_alt_names = vec!["server.example".to_string(), "localhost".to_string()];
    let CertifiedKey { cert, key_pair } = generate_simple_self_signed(subject_alt_names)?;
    write("src/crypto/cert/cert.pem", cert.pem())?;
    write("src/crypto/cert/key_pair.pem", key_pair.serialize_pem())?;
    Ok(())
}
