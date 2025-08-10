use std::collections::HashMap;

use thiserror::Error;

use super::AppState;

pub type TxId = u64;
pub type PublicKey = crate::crypto::PublicKey;
pub type Sig = crate::crypto::Sig;

pub struct Utxo {
    outputs: HashMap<UtxoId, UtxoData>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
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
    pub id: TxId,
    pub input: UtxoOpInput,
    pub outputs: Vec<UtxoData>,
    pub sigs: Vec<Sig>,
}

pub enum UtxoOpInput {
    Spend(Vec<UtxoId>),
    Mint,
}

#[derive(Debug, Error)]
pub enum UtxoError {
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("invalid signature")]
    InvalidSignature,
}

impl AppState for Utxo {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;

    fn execute(&mut self, op: Self::Op) -> Self::Res {
        if let UtxoOpInput::Spend(inputs) = op.input {
            // TODO also check owner signature
            if inputs
                .iter()
                // should we just report invalid fund if this `get` misses?
                .filter_map(|id| self.outputs.get(id))
                .map(|output| output.amount)
                .sum::<u64>()
                < op.outputs.iter().map(|o| o.amount).sum::<u64>()
            {
                return Err(UtxoError::InsufficientFunds);
            }

            for input in inputs {
                self.outputs.remove(&input);
            }
        }

        for (index, output) in op.outputs.into_iter().enumerate() {
            self.outputs.insert(UtxoId(op.id, index as _), output);
        }
        Ok(())
    }
}
