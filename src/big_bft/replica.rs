use std::{
    collections::HashMap,
    mem::{replace, take},
};

use bincode::{Decode, Encode};

use super::{
    DigestHash, Spec, Txn, message,
    state_shard::{StateShard, StateShardDigestHashes},
};

#[cfg(test)]
mod tests;

pub type Version = u32;

pub struct ReplicaCore {
    config: ReplicaCoreConfig,

    version: Version,
    txn: Txn,
    shard_hashes: StateShardDigestHashes,
    shards: HashMap<usize, StateShard>,
}

pub struct ReplicaCoreConfig {
    spec: Spec,
    index: usize,
}

impl ReplicaCoreConfig {
    fn is_fast_replica_of(&self, shard_index: usize) -> bool {
        self.spec
            .fast_replicas(shard_index)
            .any(|index| index == self.index)
    }
}

pub enum ReplicaCoreEvent {
    // the transaction that will bring self.version to self.version + 1
    Execute(Txn),
    // remote shard of self.version
    SyncShard(usize, StateShard),
}

pub enum ReplicaCoreAction {
    // demanded remote shard of self.version
    PullShard(usize),
    // self.shard_hashes.root() is reflecting the state of self.version
    Executed,
}
type ReplicaCoreActions = Vec<ReplicaCoreAction>;

impl ReplicaCore {
    pub fn new(config: ReplicaCoreConfig) -> Self {
        Self {
            shard_hashes: StateShardDigestHashes::new(config.spec.num_shard),
            config,
            shards: Default::default(),
            version: 0,
            txn: Default::default(),
        }
    }

    pub fn init(&mut self, initial_state: impl IntoIterator<Item = (DigestHash, String)>) {
        let mut shards = (0..self.config.spec.num_shard)
            .map(|_| StateShard::new())
            .collect::<Vec<_>>();
        for (key, value) in initial_state {
            shards[self.config.spec.shard_of(&key)]
                .store
                .insert(key, value);
        }
        for (shard_index, shard) in shards.iter().enumerate() {
            self.shard_hashes.update(shard_index, shard)
        }
        self.shard_hashes.refresh();
        self.shards = shards
            .into_iter()
            .enumerate()
            .filter_map(|(index, shard)| {
                if self.config.is_fast_replica_of(index) {
                    Some((index, shard))
                } else {
                    None
                }
            })
            .collect();
    }

    fn to_sync(&self) -> impl Iterator<Item = usize> {
        self.txn.keys().filter_map(|key| {
            Some(self.config.spec.shard_of(key))
                .filter(|shard_index| !self.shards.contains_key(shard_index))
        })
    }

    pub fn handle(&mut self, event: ReplicaCoreEvent, actions: &mut ReplicaCoreActions) {
        match event {
            ReplicaCoreEvent::Execute(txn) => {
                self.txn = txn;
                let mut can_execute = true;
                for shard_index in self.to_sync() {
                    can_execute = false;
                    actions.push(ReplicaCoreAction::PullShard(shard_index))
                }
                if can_execute {
                    self.execute(actions)
                }
            }
            ReplicaCoreEvent::SyncShard(shard_index, shard) => {
                self.shards.insert(shard_index, shard);
                if self.to_sync().count() == 0 {
                    self.execute(actions)
                }
            }
        }
    }

    fn execute(&mut self, actions: &mut ReplicaCoreActions) {
        for op in &*self.txn {
            match op {
                super::Op::Insert(key, value) => {
                    let shard = self
                        .shards
                        .get_mut(&self.config.spec.shard_of(key))
                        .unwrap();
                    if shard.store.contains_key(key) {
                        unimplemented!() // abort?
                    }
                    shard.store.insert(*key, value.clone());
                }
                super::Op::Read(key) => {
                    let _value = self.shards[&self.config.spec.shard_of(key)]
                        .store
                        .get(key)
                        .cloned();
                }
                super::Op::Update(key, new_value) => {
                    let shard = self
                        .shards
                        .get_mut(&self.config.spec.shard_of(key))
                        .unwrap();
                    let Some(value) = shard.store.get_mut(key) else {
                        unimplemented!() // abort?
                    };
                    *value = new_value.clone()
                }
            }
        }
        for (&shard_index, shard) in &self.shards {
            self.shard_hashes.update(shard_index, shard)
        }
        self.shard_hashes.refresh();
        self.version += 1;
        actions.push(ReplicaCoreAction::Executed);
        self.shards
            .retain(|&index, _| self.config.is_fast_replica_of(index))
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum Message {
    PushShard(message::SyncShard),
}

pub struct Replica {
    pub core: ReplicaCore,
    core_actions: ReplicaCoreActions,
    // None means not executing, Some([]) means executing but no pending
    pending_txns: Option<Vec<Txn>>,
    reordering_sync_shards: HashMap<Version, Vec<(usize, StateShard)>>,
    ticked_version: Version,
}

pub enum ReplicaAction {
    SendToAll(Message),
    Executed(Version, DigestHash),
}
pub type ReplicaActions = Vec<ReplicaAction>;

impl Replica {
    pub fn new(core: ReplicaCore) -> Self {
        Self {
            core,
            core_actions: Default::default(),
            pending_txns: None,
            reordering_sync_shards: Default::default(),
            ticked_version: 0,
        }
    }

    pub fn id(&self) -> crate::common::ReplicaId {
        self.core.config.index as _
    }

    pub fn execute(&mut self, txn: Txn, actions: &mut ReplicaActions) {
        if let Some(pending_txns) = self.pending_txns.as_mut() {
            pending_txns.push(txn);
            return;
        }
        for key in txn.keys() {
            // the first fast replica speculative (pre)push to all the others
            // as long as fast path hits, no more message should be required
            let index = self.core.config.spec.shard_of(key);
            if self.core.config.spec.fast_replicas(index).next() == Some(self.core.config.index) {
                let data = self.core.shards[&index].clone();
                let push_shard = message::SyncShard {
                    version: self.core.version,
                    index,
                    data,
                };
                actions.push(ReplicaAction::SendToAll(Message::PushShard(push_shard)))
            }
        }
        self.core
            .handle(ReplicaCoreEvent::Execute(txn), &mut self.core_actions);
        self.pending_txns = Some(Default::default());
        self.effect_core_actions(actions)
    }

    pub fn receive(&mut self, message: Message, actions: &mut ReplicaActions) {
        match message {
            Message::PushShard(sync_shard) => {
                if sync_shard.version < self.core.version {
                    return;
                }
                if sync_shard.version > self.core.version {
                    self.reordering_sync_shards
                        .entry(sync_shard.version)
                        .or_default()
                        .push((sync_shard.index, sync_shard.data));
                    return;
                }
                if self
                    .core
                    .shard_hashes
                    .verify(sync_shard.index, &sync_shard.data)
                {
                    self.core.handle(
                        ReplicaCoreEvent::SyncShard(sync_shard.index, sync_shard.data),
                        &mut self.core_actions,
                    )
                } else {
                    tracing::warn!(%sync_shard.version, %sync_shard.index, "malformed SyncShard")
                }
            }
        }
        self.effect_core_actions(actions)
    }

    fn effect_core_actions(&mut self, actions: &mut ReplicaActions) {
        while !self.core_actions.is_empty() {
            for core_action in take(&mut self.core_actions) {
                match core_action {
                    ReplicaCoreAction::PullShard(_) => {
                        // should save the shard index to pull, and pull after a while if it is
                        // (still) not speculative pushed
                    }
                    ReplicaCoreAction::Executed => {
                        actions.push(ReplicaAction::Executed(
                            self.core.version,
                            self.core.shard_hashes.root(),
                        ));
                        if let Some(sync_shards) =
                            self.reordering_sync_shards.remove(&self.core.version)
                        {
                            for (shard_index, shard) in sync_shards {
                                if self.core.shard_hashes.verify(shard_index, &shard) {
                                    self.core.handle(
                                        ReplicaCoreEvent::SyncShard(shard_index, shard),
                                        &mut self.core_actions,
                                    )
                                } else {
                                    tracing::warn!(%self.core.version, %shard_index, "malformed SyncShard (reordered)")
                                }
                            }
                        }
                        if let Some(txn) = self.pending_txns.as_mut().unwrap().pop() {
                            self.core
                                .handle(ReplicaCoreEvent::Execute(txn), &mut self.core_actions)
                        } else {
                            self.pending_txns = None
                        }
                    }
                }
            }
        }
    }

    pub fn tick(&mut self, _actions: &mut ReplicaActions) {
        let ticked_version = replace(&mut self.ticked_version, self.core.version);
        if ticked_version == self.core.version && self.pending_txns.is_some() {
            unimplemented!()
        }
    }
}
