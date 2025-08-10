use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher as _, BuildHasherDefault, DefaultHasher, Hash},
    marker::PhantomData,
};

use derive_where::derive_where;

use crate::app::utxo::{UtxoError, UtxoId, UtxoOp, UtxoOpInput};

use super::{PartialStateExecute, PartialStateExecuteOutput, ShardIndex, ShardedStateApp};

#[derive_where(Debug, Clone)]
pub struct App<A> {
    num_shard: ShardIndex,
    _app: PhantomData<A>,
}

impl<A> App<A> {
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

pub struct StaticDispatch<A: ShardedStateApp> {
    op: A::Op,
    app: A,
}

pub use crate::app::null::Null;

impl ShardedStateApp for App<Null> {
    type Op = ();
    type Res = ();
    type Shard = ();
    type Execute = StaticDispatch<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        ()
    }
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        StaticDispatch {
            op,
            app: self.clone(),
        }
    }
}

impl PartialStateExecute<App<Null>> for StaticDispatch<App<Null>> {
    fn proceed(
        &mut self,
        _shards: &mut HashMap<ShardIndex, <App<Null> as ShardedStateApp>::Shard>,
    ) -> PartialStateExecuteOutput<<App<Null> as ShardedStateApp>::Res> {
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

impl ShardedStateApp for App<Kv> {
    type Op = Vec<KvOp>;
    type Res = Vec<KvRes>;
    type Shard = HashMap<String, String>;
    type Execute = StaticDispatch<Self>;

    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }

    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        StaticDispatch {
            op,
            app: self.clone(),
        }
    }
}

impl App<Kv> {
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

impl PartialStateExecute<App<Kv>> for StaticDispatch<App<Kv>> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, <App<Kv> as ShardedStateApp>::Shard>,
    ) -> PartialStateExecuteOutput<<App<Kv> as ShardedStateApp>::Res> {
        let required_indices = &self
            .op
            .iter()
            .map(|op| self.app.shard_of(op))
            .collect::<HashSet<_>>()
            - &shards.keys().cloned().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            PartialStateExecuteOutput::RequireAccess(required_indices)
        } else {
            let res = self
                .op
                .iter()
                .map(|op| self.app.execute(op, shards))
                .collect();
            PartialStateExecuteOutput::Complete(res)
        }
    }
}

pub use crate::app::utxo::Utxo;

impl ShardedStateApp for App<Utxo> {
    type Op = UtxoOp;
    type Res = Result<(), UtxoError>;
    type Shard = Utxo;
    type Execute = StaticDispatch<Self>;
    fn new_shard(&self, _index: ShardIndex) -> Self::Shard {
        Default::default()
    }
    fn new_execute(&self, op: Self::Op) -> Self::Execute {
        StaticDispatch {
            op,
            app: self.clone(),
        }
    }
}

impl App<Utxo> {
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
        op: &UtxoOp,
        shards: &mut HashMap<ShardIndex, Utxo>,
    ) -> Result<(), UtxoError> {
        if matches!(op.input, UtxoOpInput::Spend(_))
            && shards
                .values()
                .map(|shard| shard.total_input(op))
                .sum::<Result<u64, _>>()?
                < op.total_output()
        {
            return Err(UtxoError::InsufficientFunds);
        }
        let tx_id = op.tx_id();
        let output_iter = op
            .outputs
            .clone()
            .into_iter()
            .enumerate()
            .map(|(index, output)| (UtxoId(tx_id.clone(), index as _), output));
        for (&index, shard) in shards {
            shard.remove_input(op);
            shard.insert_outputs(
                output_iter
                    .clone()
                    .filter(|(id, _)| self.shard_index_from_hash(id) == index),
            );
        }
        Ok(())
    }
}

impl PartialStateExecute<App<Utxo>> for StaticDispatch<App<Utxo>> {
    fn proceed(
        &mut self,
        shards: &mut HashMap<ShardIndex, <App<Utxo> as ShardedStateApp>::Shard>,
    ) -> PartialStateExecuteOutput<<App<Utxo> as ShardedStateApp>::Res> {
        let required_indices =
            &self.app.shards_of(&self.op) - &shards.keys().cloned().collect::<HashSet<_>>();
        if !required_indices.is_empty() {
            PartialStateExecuteOutput::RequireAccess(required_indices)
        } else {
            PartialStateExecuteOutput::Complete(self.app.execute(&self.op, shards))
        }
    }
}
