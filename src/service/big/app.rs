use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    marker::PhantomData,
};

use derive_where::derive_where;

use crate::app::utxo::{UtxoError, UtxoId, UtxoOp, UtxoOpInput};

use super::{PartialStateExecute, PartialStateExecuteOutput, ShardIndex, ShardedStateApp};

#[derive_where(Debug, Clone)]
pub struct ShardSchema<A> {
    num_shard: ShardIndex,
    _app: PhantomData<A>,
}

impl<A> ShardSchema<A> {
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

pub struct StaticDispatchExecuteState<A: ShardedStateApp> {
    op: Option<A::Op>,
    schema: A,
}

impl<A> ShardSchema<A> {
    fn static_dispatch(&self, op: <Self as ShardedStateApp>::Op) -> StaticDispatchExecuteState<Self>
    where
        Self: ShardedStateApp,
    {
        StaticDispatchExecuteState {
            op: Some(op),
            schema: self.clone(),
        }
    }
}

trait StaticDispatch: ShardedStateApp {
    fn shards_of(&self, op: &Self::Op) -> HashSet<ShardIndex>;
    fn execute(&self, op: Self::Op, shards: &mut HashMap<ShardIndex, Self::Shard>) -> Self::Res;
}

impl<A: ShardedStateApp> PartialStateExecute<A> for StaticDispatchExecuteState<A>
where
    A: StaticDispatch,
{
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, A::Shard>,
    ) -> PartialStateExecuteOutput<A::Res> {
        let required_indices = &self.schema.shards_of(self.op.as_ref().unwrap())
            - &shards.keys().cloned().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            PartialStateExecuteOutput::RequireAccess(required_indices)
        } else {
            PartialStateExecuteOutput::Complete(
                self.schema.execute(self.op.take().unwrap(), shards),
            )
        }
    }
}

pub use crate::app::null::Null;

impl ShardedStateApp for ShardSchema<Null> {
    type Op = ();
    type Res = ();
    type Shard = ();
    type Execute = StaticDispatchExecuteState<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {}
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        self.static_dispatch(op)
    }
}

impl PartialStateExecute<ShardSchema<Null>> for StaticDispatchExecuteState<ShardSchema<Null>> {
    fn proceed(
        &mut self,
        _shards: &mut HashMap<ShardIndex, <ShardSchema<Null> as ShardedStateApp>::Shard>,
    ) -> PartialStateExecuteOutput<<ShardSchema<Null> as ShardedStateApp>::Res> {
        PartialStateExecuteOutput::Complete(())
    }
}

pub struct Kv;

pub enum KvOp {
    Put(String, String),
    Get(String),
}

pub enum KvRes {
    Put,
    Get(Option<String>),
}

impl ShardedStateApp for ShardSchema<Kv> {
    type Op = Vec<KvOp>;
    type Res = Vec<KvRes>;
    type Shard = HashMap<String, String>;
    type Execute = StaticDispatchExecuteState<Self>;

    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }

    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        self.static_dispatch(op)
    }
}

impl ShardSchema<Kv> {
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

impl StaticDispatch for ShardSchema<Kv> {
    fn shards_of(&self, op: &Self::Op) -> HashSet<ShardIndex> {
        op.iter().map(|op| self.shard_of(op)).collect()
    }

    fn execute(&self, op: Self::Op, shards: &mut HashMap<ShardIndex, Self::Shard>) -> Self::Res {
        op.into_iter().map(|op| self.execute(&op, shards)).collect()
    }
}

pub use crate::app::utxo::Utxo;

impl ShardedStateApp for ShardSchema<Utxo> {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;
    type Shard = Utxo;
    type Execute = StaticDispatchExecuteState<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        self.static_dispatch(op)
    }
}

impl StaticDispatch for ShardSchema<Utxo> {
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

    fn execute(&self, op: UtxoOp, shards: &mut HashMap<ShardIndex, Utxo>) -> Result<(), UtxoError> {
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
