use std::collections::{HashMap, VecDeque};

use bincode::{Decode, Encode};
use thiserror::Error;

use crate::crypto::{Digest, DigestHash as _, UpdateHash, verify};

use super::{
    AppProtocol, DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState, InMemory,
};

pub type TxId = Digest;
pub type PublicKey = crate::crypto::PublicKey;
pub type Sig = crate::crypto::Sig;

pub type Utxo = InMemory<DataShardingUtxo>;

impl Utxo {
    pub fn new() -> Self {
        Self {
            app: DataShardingUtxo,
            store: Default::default(),
        }
    }
}

impl Default for Utxo {
    fn default() -> Self {
        Self::new()
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

#[derive(Debug, Clone, Hash, Eq, PartialEq, Encode, Decode)]
pub struct UtxoId(pub TxId, pub u8);

#[derive(Debug, Clone, Encode, Decode)]
pub struct UtxoData {
    pub owner: PublicKey,
    pub amount: u64,
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

impl UtxoOp {
    pub fn tx_id(&self) -> TxId {
        self.digest()
    }

    pub fn total_output(&self) -> u64 {
        self.outputs.iter().map(|o| o.amount).sum()
    }
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
        let get_proceeds = match &op.input {
            UtxoOpInput::Spend(sigs) => sigs
                .keys()
                .map(|id| DataShardingExecuteOutput::Get(id.clone()))
                .collect::<Vec<_>>(),
            UtxoOpInput::Mint => Default::default(),
        };
        Self::ExecuteState {
            op,
            spend_amount: 0,
            num_remaining_input: get_proceeds.len(),
            input_buffer: Default::default(),
            proceed_buffer: get_proceeds.into(),
        }
    }
}

impl DataShardingExecuteState for DataShardingUtxoExecute {
    type App = DataShardingUtxo;
    fn get_result(
        &mut self,
        key: <Self::App as DataShardingApp>::Key,
        value: Option<<Self::App as DataShardingApp>::Value>,
    ) {
        if let Some(value) = value {
            self.input_buffer.push((key, value))
        } else {
            // or directly error?
            self.num_remaining_input -= 1
        }
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

impl UpdateHash for UtxoOp {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        match &self.input {
            UtxoOpInput::Spend(inputs) => {
                state.update(b"spend");
                for UtxoId(tx_id, index) in inputs.keys() {
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
