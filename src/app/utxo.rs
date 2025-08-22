use std::collections::{HashMap, VecDeque};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::crypto::{Digest, DigestHash as _, UpdateHash, verify};

use super::{
    AppProtocol, AppState, DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState,
};

pub type TxId = Digest;
pub type PublicKey = crate::crypto::PublicKey;
pub type Sig = crate::crypto::Sig;

#[derive(Debug, Encode, Decode)]
pub struct Utxo {
    outputs: HashMap<UtxoId, UtxoData>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq, Encode, Decode)]
pub struct UtxoId(pub TxId, pub u8);

#[derive(Debug, Clone, Encode, Decode)]
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
}

pub enum UtxoOpInput {
    Spend(HashMap<UtxoId, Sig>),
    Mint,
}

#[derive(Debug, Error)]
pub enum UtxoError {
    #[error("insufficient funds")]
    InsufficientFunds,
    #[error("invalid signature")]
    InvalidSignature,
}

impl AppProtocol for Utxo {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;
}

impl UtxoOp {
    pub fn tx_id(&self) -> TxId {
        self.digest()
    }

    pub fn total_output(&self) -> u64 {
        self.outputs.iter().map(|o| o.amount).sum()
    }
}

impl Utxo {
    pub fn total_input(&self, op: &UtxoOp) -> Result<u64, UtxoError> {
        let UtxoOpInput::Spend(spend) = &op.input else {
            return Ok(0);
        };
        let mut total = 0;
        for (id, sig) in spend {
            let Some(output) = self.outputs.get(id) else {
                continue;
            };
            verify(op, &output.owner, sig).map_err(|_| UtxoError::InvalidSignature)?;
            total += output.amount
        }
        Ok(total)
    }

    pub fn remove_input(&mut self, input: &UtxoOpInput) {
        let UtxoOpInput::Spend(spend) = input else {
            return;
        };
        for (input, _) in spend {
            self.outputs.remove(input);
        }
    }

    pub fn insert_output(&mut self, id: UtxoId, output: UtxoData) {
        self.outputs.insert(id, output);
    }
}

impl AppState for Utxo {
    fn execute(&mut self, op: Self::Op) -> Self::Res {
        if matches!(op.input, UtxoOpInput::Spend(_)) && self.total_input(&op)? < op.total_output() {
            return Err(UtxoError::InsufficientFunds);
        }
        self.remove_input(&op.input);

        let tx_id = op.tx_id();
        for (index, output) in op.outputs.into_iter().enumerate() {
            self.insert_output(UtxoId(tx_id.clone(), index as _), output)
        }
        Ok(())
    }
}

impl UpdateHash for UtxoOp {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        match &self.input {
            UtxoOpInput::Spend(inputs) => {
                state.update(b"spend");
                for (UtxoId(tx_id, index), _) in inputs {
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

pub struct DataShardingUtxo;
pub struct DataShardingUtxoExecute {
    op: UtxoOp,
    spend_amount: u64,
    num_remaining_input: usize,
    input_buffer: Vec<(UtxoId, UtxoData)>,
    proceed_buffer: VecDeque<DataShardingExecuteOutput<DataShardingUtxo>>,
}

impl AppProtocol for DataShardingUtxo {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;
}

impl DataShardingApp for DataShardingUtxo {
    type Key = UtxoId;
    type Value = UtxoData;
    type ExecuteState = DataShardingUtxoExecute;
    fn new_execute(&self, op: Self::Op) -> Self::ExecuteState {
        let proceed_buffer;
        let num_remaining_input;
        match &op.input {
            UtxoOpInput::Spend(sigs) => {
                proceed_buffer = sigs
                    .keys()
                    .map(|id| DataShardingExecuteOutput::Get(id.clone()))
                    .collect();
                num_remaining_input = sigs.len()
            }
            UtxoOpInput::Mint => {
                proceed_buffer = Default::default();
                num_remaining_input = 0
            }
        }
        Self::ExecuteState {
            op,
            spend_amount: 0,
            num_remaining_input,
            input_buffer: Default::default(),
            proceed_buffer,
        }
    }
}

impl DataShardingExecuteState for DataShardingUtxoExecute {
    type App = DataShardingUtxo;
    fn get_ok(
        &mut self,
        key: <Self::App as DataShardingApp>::Key,
        value: <Self::App as DataShardingApp>::Value,
    ) {
        self.input_buffer.push((key, value))
    }
    fn proceed(&mut self) -> DataShardingExecuteOutput<Self::App> {
        if let Some(output) = self.proceed_buffer.pop_front() {
            return output;
        }

        while let Some((id, data)) = self.input_buffer.pop() {
            let UtxoOpInput::Spend(sigs) = &self.op.input else {
                unimplemented!()
            };
            match verify(&self.op, &data.owner, &sigs[&id]) {
                Ok(()) => self.spend_amount += data.amount,
                Err(_) => {
                    return DataShardingExecuteOutput::Complete(Err(UtxoError::InvalidSignature));
                }
            }
            self.num_remaining_input -= 1
        }

        if match &self.op.input {
            UtxoOpInput::Mint => true,
            UtxoOpInput::Spend(_) => self.spend_amount >= self.op.total_output(),
        } {
            let tx_id = self.op.tx_id();
            for (index, data) in self.op.outputs.drain(..).enumerate() {
                self.proceed_buffer
                    .push_back(DataShardingExecuteOutput::Put(
                        UtxoId(tx_id.clone(), index as _),
                        data,
                    ))
            }
            self.proceed_buffer
                .push_back(DataShardingExecuteOutput::Complete(Ok(())));
            self.proceed()
        } else if self.num_remaining_input == 0 {
            DataShardingExecuteOutput::Complete(Err(UtxoError::InsufficientFunds))
        } else {
            DataShardingExecuteOutput::Pending
        }
    }
}
