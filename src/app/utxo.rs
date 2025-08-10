use std::collections::HashMap;

pub type TxId = u64;
pub type PublicKey = crate::crypto::PublicKey;

pub struct Utxo {
    outputs: HashMap<UtxoId, UtxoData>,
}

pub struct UtxoId(pub TxId, pub u8);

pub struct UtxoData {
    pub owner: PublicKey,
    pub amount: u64,
}

impl Utxo {
    pub fn new() -> Self {
        Self {
            outputs: Default::default(),
        }
    }
}

pub struct UtxoOp {
    pub input: UtxoOpInput,
    pub outputs: Vec<UtxoData>,
}

pub enum UtxoOpInput {
    Spend(Vec<UtxoId>),
    Mint,
}
