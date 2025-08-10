use std::collections::HashMap;

use thiserror::Error;

use crate::crypto::{Digest, DigestHash as _, UpdateHash, verify};

use super::AppState;

pub type TxId = Digest;
pub type PublicKey = crate::crypto::PublicKey;
pub type Sig = crate::crypto::Sig;

pub struct Utxo {
    outputs: HashMap<UtxoId, UtxoData>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct UtxoId(pub TxId, pub u8);

#[derive(Debug, Clone)]
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

impl Default for Utxo {
    fn default() -> Self {
        Self::new()
    }
}

pub struct UtxoOp {
    pub input: UtxoOpInput,
    pub outputs: Vec<UtxoData>,
    pub nonce: u64,
    pub sigs: Vec<Sig>,
}

pub enum UtxoOpInput {
    Spend(Vec<UtxoId>),
    Mint,
}

impl UtxoOp {
    pub fn tx_id(&self) -> TxId {
        self.digest()
    }

    pub fn total_output(&self) -> u64 {
        self.outputs.iter().map(|o| o.amount).sum()
    }
}

#[derive(Debug, Error)]
pub enum UtxoError {
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("invalid signature")]
    InvalidSignature,
}

impl Utxo {
    pub fn total_input(&self, op: &UtxoOp) -> Result<u64, UtxoError> {
        let UtxoOpInput::Spend(spend) = &op.input else {
            return Ok(0);
        };
        let mut total = 0;
        for (id, sig) in spend.iter().zip(&op.sigs) {
            let Some(output) = self.outputs.get(id) else {
                continue;
            };
            verify(op, &output.owner, sig).map_err(|_| UtxoError::InvalidSignature)?;
            total += output.amount
        }
        Ok(total)
    }

    pub fn remove_input(&mut self, op: &UtxoOp) {
        let UtxoOpInput::Spend(spend) = &op.input else {
            return;
        };
        for input in spend {
            self.outputs.remove(input);
        }
    }

    pub fn insert_outputs(&mut self, outputs: impl Iterator<Item = (UtxoId, UtxoData)>) {
        for (id, output) in outputs {
            self.outputs.insert(id, output);
        }
    }
}

impl AppState for Utxo {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;

    fn execute(&mut self, op: Self::Op) -> Self::Res {
        if matches!(op.input, UtxoOpInput::Spend(_)) && self.total_input(&op)? < op.total_output() {
            return Err(UtxoError::InsufficientFunds);
        }
        self.remove_input(&op);

        let tx_id = op.tx_id();
        self.insert_outputs(
            op.outputs
                .into_iter()
                .enumerate()
                .map(|(index, output)| (UtxoId(tx_id.clone(), index as _), output)),
        );
        Ok(())
    }
}

impl UpdateHash for UtxoOp {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        match &self.input {
            UtxoOpInput::Spend(inputs) => {
                state.update(b"spend");
                for UtxoId(tx_id, index) in inputs {
                    state.update(tx_id);
                    state.update(index.to_le_bytes())
                }
            }
            UtxoOpInput::Mint => state.update(b"mint"),
        }
        for output in &self.outputs {
            output.owner.update(state);
            state.update(output.amount.to_le_bytes())
        }
        state.update(self.nonce.to_le_bytes())
    }
}
