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
    pub nonce: u64,
    pub sigs: Vec<Sig>,
}

pub enum UtxoOpInput {
    Spend(Vec<UtxoId>),
    Mint,
}

impl UtxoOp {
    fn tx_id(&self) -> TxId {
        self.digest()
    }
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
        if let UtxoOpInput::Spend(inputs) = &op.input {
            if inputs
                .iter()
                .filter_map(|id| self.outputs.get(id))
                .map(|output| output.amount)
                .sum::<u64>()
                < op.outputs.iter().map(|o| o.amount).sum::<u64>()
            {
                return Err(UtxoError::InsufficientFunds);
            }

            for (input, sig) in inputs.iter().zip(&op.sigs) {
                let Some(output) = self.outputs.get(input) else {
                    return Err(UtxoError::InvalidSignature);
                };
                verify(&op, &output.owner, sig).map_err(|_| UtxoError::InvalidSignature)?
            }

            for input in inputs {
                self.outputs.remove(input);
            }
        }

        let tx_id = op.tx_id();
        for (index, output) in op.outputs.into_iter().enumerate() {
            self.outputs
                .insert(UtxoId(tx_id.clone(), index as _), output);
        }
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
