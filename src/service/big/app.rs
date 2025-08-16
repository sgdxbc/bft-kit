use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    marker::PhantomData,
};

use bincode::{Decode, Encode};
use derive_where::derive_where;

use crate::{
    app::utxo::{UtxoError, UtxoId, UtxoOp, UtxoOpInput},
    service::ServiceApp,
};

use super::{DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState, ShardIndex};

#[derive_where(Debug, Clone)]
pub struct DataShardingSchema<A> {
    num_shard: ShardIndex,
    _app: PhantomData<A>,
}

impl<A> DataShardingSchema<A> {
    pub fn new(num_shard: ShardIndex) -> Self {
        Self {
            num_shard,
            _app: PhantomData,
        }
    }

    fn shard_index_from_hash(&self, key: impl Hash) -> ShardIndex {
        (BuildHasherDefault::<DefaultHasher>::default().hash_one(key) % self.num_shard as u64) as _
    }
}

pub struct StaticDispatchExecute<A: DataShardingApp> {
    op: Option<A::Op>,
    schema: A,
}

impl<A> DataShardingSchema<A> {
    fn static_dispatch(&self, op: <Self as ServiceApp>::Op) -> StaticDispatchExecute<Self>
    where
        Self: DataShardingApp,
    {
        StaticDispatchExecute {
            op: Some(op),
            schema: self.clone(),
        }
    }
}

trait StaticDispatch: DataShardingApp {
    fn shards_of(&self, op: &Self::Op) -> HashSet<ShardIndex>;
    fn execute(&self, op: Self::Op, shards: &mut HashMap<ShardIndex, Self::Shard>) -> Self::Res;
}

impl<A: StaticDispatch> DataShardingExecuteState<A> for StaticDispatchExecute<A> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, A::Shard>,
    ) -> DataShardingExecuteOutput<A::Res> {
        let required_indices = &self.schema.shards_of(self.op.as_ref().unwrap())
            - &shards.keys().cloned().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            DataShardingExecuteOutput::RequireAccess(required_indices)
        } else {
            DataShardingExecuteOutput::Complete(
                self.schema.execute(self.op.take().unwrap(), shards),
            )
        }
    }
}

pub struct Null;

impl ServiceApp for DataShardingSchema<Null> {
    type Op = ();
    type Res = ();
}

impl DataShardingApp for DataShardingSchema<Null> {
    type Shard = ();
    type Execute = StaticDispatchExecute<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {}
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        self.static_dispatch(op)
    }
}

impl DataShardingExecuteState<DataShardingSchema<Null>>
    for StaticDispatchExecute<DataShardingSchema<Null>>
{
    fn proceed(
        &mut self,
        _shards: &mut HashMap<ShardIndex, <DataShardingSchema<Null> as DataShardingApp>::Shard>,
    ) -> DataShardingExecuteOutput<<DataShardingSchema<Null> as ServiceApp>::Res> {
        DataShardingExecuteOutput::Complete(())
    }
}

pub struct Kv;

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

impl ServiceApp for DataShardingSchema<Kv> {
    type Op = Vec<KvOp>;
    type Res = Vec<KvRes>;
}

impl DataShardingApp for DataShardingSchema<Kv> {
    type Shard = HashMap<String, String>;
    type Execute = StaticDispatchExecute<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        self.static_dispatch(op)
    }
}

impl DataShardingSchema<Kv> {
    fn shard_of(&self, op: &KvOp) -> ShardIndex {
        match op {
            KvOp::Put(key, _) | KvOp::Get(key) => self.shard_index_from_hash(key),
        }
    }

    fn execute(
        &self,
        op: &KvOp,
        shards: &mut HashMap<ShardIndex, HashMap<String, String>>,
    ) -> KvRes {
        match op {
            KvOp::Put(key, value) => {
                shards
                    .get_mut(&self.shard_of(op))
                    .unwrap()
                    .insert(key.clone(), value.clone());
                KvRes::Put
            }
            KvOp::Get(key) => KvRes::Get(shards.get(&self.shard_of(op)).unwrap().get(key).cloned()),
        }
    }
}

impl StaticDispatch for DataShardingSchema<Kv> {
    fn shards_of(&self, op: &Self::Op) -> HashSet<ShardIndex> {
        op.iter().map(|op| self.shard_of(op)).collect()
    }
    fn execute(&self, op: Self::Op, shards: &mut HashMap<ShardIndex, Self::Shard>) -> Self::Res {
        op.into_iter().map(|op| self.execute(&op, shards)).collect()
    }
}

pub struct Utxo;

impl ServiceApp for DataShardingSchema<Utxo> {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;
}

impl DataShardingApp for DataShardingSchema<Utxo> {
    type Shard = crate::app::utxo::Utxo;
    type Execute = StaticDispatchExecute<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        self.static_dispatch(op)
    }
}

impl StaticDispatch for DataShardingSchema<Utxo> {
    // consider shard by owner?
    fn shards_of(&self, op: &UtxoOp) -> HashSet<ShardIndex> {
        let mut shards = HashSet::new();
        if let UtxoOpInput::Spend(spend) = &op.input {
            for id in spend {
                shards.insert(self.shard_index_from_hash(id));
            }
        }
        let tx_id = op.tx_id();
        for i in 0..op.outputs.len() {
            shards.insert(self.shard_index_from_hash(UtxoId(tx_id.clone(), i as _)));
        }
        shards
    }

    fn execute(
        &self,
        op: UtxoOp,
        shards: &mut HashMap<ShardIndex, crate::app::utxo::Utxo>,
    ) -> Result<(), UtxoError> {
        if matches!(op.input, UtxoOpInput::Spend(_))
            && shards
                .values()
                .map(|shard| shard.total_input(&op))
                .sum::<Result<u64, _>>()?
                < op.total_output()
        {
            return Err(UtxoError::InsufficientFunds);
        }
        let tx_id = op.tx_id();
        let output_iter = op
            .outputs
            .into_iter()
            .enumerate()
            .map(|(index, output)| (UtxoId(tx_id.clone(), index as _), output));
        for (&index, shard) in shards {
            shard.remove_input(&op.input);
            shard.insert_outputs(
                output_iter
                    .clone()
                    .filter(|(id, _)| self.shard_index_from_hash(id) == index),
            );
        }
        Ok(())
    }
}
