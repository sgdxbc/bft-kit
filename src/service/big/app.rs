use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    mem::take,
};

use bincode::{Decode, Encode};

use crate::{
    app::utxo::{UtxoData, UtxoError, UtxoId, UtxoOp, UtxoOpInput},
    service::AppProtocol,
};

// if this is a sufficiently universal abstraction, can promote it to crate::app

pub type ShardIndex = u32;

pub trait DataShardingApp: AppProtocol {
    type Shard;
    type Execute: DataShardingExecuteState<App = Self>;
    fn new_execute(&self, op: Self::Op) -> Self::Execute;
}

pub trait InitDataShard<S> {
    fn num_shard(&self) -> ShardIndex;
    fn init(&self, index: ShardIndex) -> S;
}

pub struct DefaultShard(pub ShardIndex);

impl<S: Default> InitDataShard<S> for DefaultShard {
    fn num_shard(&self) -> ShardIndex {
        self.0
    }

    fn init(&self, _index: ShardIndex) -> S {
        Default::default()
    }
}

pub trait DataShardingExecuteState {
    type App: DataShardingApp;

    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, <Self::App as DataShardingApp>::Shard>,
    ) -> DataShardingExecuteOutput<<Self::App as AppProtocol>::Res>;
}

#[derive(Debug)]
pub enum DataShardingExecuteOutput<R> {
    RequireAccess(HashSet<ShardIndex>),
    Complete(R),
}

#[derive(Debug, Clone)]
pub struct DataShardingSchema {
    num_shard: ShardIndex,
}

impl DataShardingSchema {
    pub fn new(num_shard: ShardIndex) -> Self {
        Self { num_shard }
    }

    fn shard_index_from_hash(&self, key: impl Hash) -> ShardIndex {
        (BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % self.num_shard as u64) as _
    }
}

pub use crate::app::null::Null;

impl DataShardingApp for Null {
    // more appropriately this would be Never, but that requires storage to be able
    // to _not_ store shards it ought to store
    type Shard = ();
    type Execute = Self;
    fn new_execute(&self, (): Self::Op) -> Self::Execute {
        Null
    }
}

impl DataShardingExecuteState for Null {
    type App = Self;

    fn proceed(
        &mut self,
        _shards: &mut HashMap<ShardIndex, <Self::App as DataShardingApp>::Shard>,
    ) -> DataShardingExecuteOutput<<Self::App as AppProtocol>::Res> {
        DataShardingExecuteOutput::Complete(())
    }
}

pub struct Kv(pub DataShardingSchema);

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum KvOp {
    Put(String, String),
    Get(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum KvRes {
    Put,
    Get(Option<String>),
}

impl AppProtocol for Kv {
    type Op = Vec<KvOp>;
    type Res = Vec<KvRes>;
}

pub struct KvExecute(Vec<(ShardIndex, KvOp)>);

impl Kv {
    fn shard_of(&self, op: &KvOp) -> ShardIndex {
        match op {
            KvOp::Put(key, _) | KvOp::Get(key) => self.0.shard_index_from_hash(key),
        }
    }
}

impl DataShardingApp for Kv {
    type Shard = HashMap<String, String>;
    type Execute = KvExecute;
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        KvExecute(op.into_iter().map(|op| (self.shard_of(&op), op)).collect())
    }
}

impl DataShardingExecuteState for KvExecute {
    type App = Kv;

    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, <Self::App as DataShardingApp>::Shard>,
    ) -> DataShardingExecuteOutput<<Self::App as AppProtocol>::Res> {
        let required_indices = self
            .0
            .iter()
            .map(|&(index, _)| index)
            .filter(|index| !shards.contains_key(index))
            .collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            return DataShardingExecuteOutput::RequireAccess(required_indices);
        }
        let res = take(&mut self.0)
            .into_iter()
            .map(|(index, op)| match op {
                KvOp::Put(key, value) => {
                    shards.get_mut(&index).unwrap().insert(key, value);
                    KvRes::Put
                }
                KvOp::Get(key) => KvRes::Get(shards[&index].get(&key).cloned()),
            })
            .collect();
        DataShardingExecuteOutput::Complete(res)
    }
}

pub mod ycsb {
    use crate::{
        app::{
            AppProtocol,
            ycsb::{YcsbOp, YcsbRes},
        },
        workload::WorkloadState,
    };

    use super::{KvOp, KvRes};

    pub struct AdaptKv<W>(pub W);

    impl<W> AppProtocol for AdaptKv<W> {
        type Op = Vec<KvOp>;
        type Res = Vec<KvRes>;
    }

    impl<W: WorkloadState<Op = YcsbOp, Res = YcsbRes>> WorkloadState for AdaptKv<W> {
        type Metadata = W::Metadata;

        fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)> {
            let (op, metadata) = self.0.next_op()?;
            let op = match op {
                YcsbOp::Insert(key, value) | YcsbOp::Update(key, value) => KvOp::Put(key, value),
                YcsbOp::Get(key) => KvOp::Get(key),
                _ => unimplemented!(),
            };
            Some((vec![op], metadata))
        }

        fn complete(&mut self, metadata: Self::Metadata, mut res: Self::Res) -> anyhow::Result<()> {
            anyhow::ensure!(res.len() == 1);
            let res = match res.remove(0) {
                KvRes::Put => YcsbRes::Ok,
                KvRes::Get(Some(value)) => YcsbRes::Get(value),
                KvRes::Get(None) => YcsbRes::NotFound,
            };
            self.0.complete(metadata, res)
        }
    }
}

pub struct Utxo(pub DataShardingSchema);

impl AppProtocol for Utxo {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;
}

pub struct UtxoExecute {
    op: UtxoOp,
    input_shards: HashSet<ShardIndex>,
    outputs: HashMap<ShardIndex, Vec<(UtxoId, UtxoData)>>,
}

impl DataShardingApp for Utxo {
    type Shard = crate::app::utxo::Utxo;
    type Execute = UtxoExecute;
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        let input_shards = match &op.input {
            UtxoOpInput::Mint => Default::default(),
            UtxoOpInput::Spend(spend) => spend
                .iter()
                .map(|(id, _)| self.0.shard_index_from_hash(id))
                .collect(),
        };
        let mut outputs = HashMap::<_, Vec<_>>::new();
        for (index, output) in op.outputs.iter().enumerate() {
            let id = UtxoId(op.tx_id(), index as _);
            let shard_index = self.0.shard_index_from_hash(&id);
            outputs
                .entry(shard_index)
                .or_default()
                .push((id, output.clone()))
        }
        UtxoExecute {
            input_shards,
            outputs,
            op,
        }
    }
}

impl DataShardingExecuteState for UtxoExecute {
    type App = Utxo;

    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, <Self::App as DataShardingApp>::Shard>,
    ) -> DataShardingExecuteOutput<<Self::App as AppProtocol>::Res> {
        let required_indices = &(&self.input_shards
            | &self.outputs.keys().copied().collect::<HashSet<_>>())
            - &shards.keys().copied().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            DataShardingExecuteOutput::RequireAccess(required_indices)
        } else {
            DataShardingExecuteOutput::Complete(self.execute(shards))
        }
    }
}

impl UtxoExecute {
    fn execute(
        &mut self,
        shards: &mut HashMap<ShardIndex, crate::app::utxo::Utxo>,
    ) -> Result<(), UtxoError> {
        if matches!(self.op.input, UtxoOpInput::Spend(_))
            && self
                .input_shards
                .iter()
                .map(|index| shards[index].total_input(&self.op))
                .sum::<Result<u64, _>>()?
                < self.op.total_output()
        {
            return Err(UtxoError::InsufficientFunds);
        }

        for index in &self.input_shards {
            shards.get_mut(index).unwrap().remove_input(&self.op.input)
        }

        for (shard_index, outputs) in &self.outputs {
            let shard = shards.get_mut(shard_index).unwrap();
            for (id, output) in outputs {
                shard.insert_output(id.clone(), output.clone())
            }
        }
        Ok(())
    }
}
