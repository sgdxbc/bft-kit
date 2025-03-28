use std::{
    collections::{BTreeMap, HashMap},
    mem::replace,
};

use sha2::Digest as _;

use crate::{ClientId, ReplicaId, crypto::Digest};

pub mod message;
pub mod tcp;

pub type ViewNum = u32;
pub type BlockNum = u32;

#[derive(Debug, Clone)]
pub struct Spec {
    pub num_faulty: ReplicaId,
    pub num_replica: ReplicaId,
}

impl Spec {
    fn primary(&self, view_num: ViewNum) -> ReplicaId {
        (view_num % self.num_replica as ViewNum) as _
    }
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub enum ToReplica {
    Request(message::Request),
    PrePrepare(message::PrePrepare),
    Prepare(message::Vote),
    Commit(message::Vote),
}

#[derive(Debug)]
pub struct ClientConfig {
    pub spec: Spec,
    pub id: ClientId,
}

pub struct Client {
    config: ClientConfig,
    view_num: ViewNum,
    seq: u32,
    seq_ticked: u32,
    op: Option<Vec<u8>>,
    results: HashMap<ReplicaId, Vec<u8>>,
}

pub enum ClientAction {
    Nop,
    Return(Vec<u8>),
    SendToReplica(ReplicaId, ToReplica),
    SendToAllReplicas(ToReplica),
}

impl Client {
    pub fn new(config: ClientConfig) -> Self {
        Self {
            config,
            view_num: 0,
            seq: 0,
            seq_ticked: 0,
            op: None,
            results: Default::default(),
        }
    }

    pub fn invoke(&mut self, op: Vec<u8>) -> ClientAction {
        assert!(self.op.is_none());
        self.op = Some(op.clone());
        self.seq += 1;
        let request = message::Request {
            client_id: self.config.id,
            seq: self.seq,
            op,
        };
        ClientAction::SendToReplica(
            self.config.spec.primary(self.view_num),
            ToReplica::Request(request),
        )
    }

    pub fn tick(&mut self) -> ClientAction {
        if replace(&mut self.seq_ticked, self.seq) != self.seq {
            return ClientAction::Nop;
        }
        let Some(op) = self.op.clone() else {
            return ClientAction::Nop;
        };
        let request = message::Request {
            client_id: self.config.id,
            seq: self.seq,
            op,
        };
        ClientAction::SendToAllReplicas(ToReplica::Request(request))
    }

    pub fn receive(&mut self, reply: message::Reply) -> ClientAction {
        if reply.seq != self.seq || self.op.is_none() {
            return ClientAction::Nop;
        }
        // TODO verify signature
        self.results.insert(reply.replica_id, reply.result.clone());
        if self
            .results
            .values()
            .filter(|&result| result == &reply.result)
            .count() as ReplicaId
            == self.config.spec.num_faulty + 1
        {
            // paper does not specify how to keep track of current view, just arbitrarily
            // implement
            self.view_num = reply.view_num;
            self.op = None;
            self.results.clear();
            ClientAction::Return(reply.result)
        } else {
            ClientAction::Nop
        }
    }
}

#[derive(Debug)]
pub struct ReplicaConfig {
    pub spec: Spec,
    pub id: ReplicaId,

    pub max_num_inflight: BlockNum,
    pub max_batch_size: usize,
}

impl ReplicaConfig {
    pub fn new_base(spec: Spec, id: ReplicaId) -> Self {
        Self {
            spec,
            id,
            max_num_inflight: 1,
            max_batch_size: 1,
        }
    }
}

pub struct Replica {
    config: ReplicaConfig,
    view_num: ViewNum,
    // use BTreeMap to efficiently (and simply) garbage collection with split_off
    blocks: BTreeMap<BlockNum, message::PrePrepare>,
    prepare_votes: BTreeMap<BlockNum, Quorum>,
    commit_votes: BTreeMap<BlockNum, Quorum>,
    requests: Vec<message::Request>,
    // only primary maintains. the last proposed block number
    propose_num: BlockNum,
    ticked_propose_num: BlockNum,
    // every block up to commit_num is committed. blocks above may also commit-able
    // but not checked yet
    commit_num: BlockNum,
}

type Quorum = HashMap<ReplicaId, message::Vote>;

impl Replica {
    pub fn new(config: ReplicaConfig) -> Self {
        Self {
            config,
            view_num: 0,
            blocks: Default::default(),
            prepare_votes: Default::default(),
            commit_votes: Default::default(),
            requests: Default::default(),
            propose_num: 0,
            ticked_propose_num: 0,
            commit_num: 0,
        }
    }

    fn is_primary(&self) -> bool {
        self.config.spec.primary(self.view_num) == self.config.id
    }

    fn is_prepared(&self, block_num: BlockNum) -> bool {
        self.commit_num >= block_num
            || self.blocks.contains_key(&block_num)
                && self.prepare_votes.get(&block_num).is_some_and(|votes| {
                    votes.len() as ReplicaId + 1
                        >= self.config.spec.num_replica - self.config.spec.num_faulty
                })
    }

    // `can_commit`?
    fn is_committed(&self, block_num: BlockNum) -> bool {
        let votes_len = self.commit_votes.get(&block_num).map(|votes| votes.len());
        tracing::trace!(
            self.config.id,
            block_num,
            is_prepared = self.is_prepared(block_num),
            ?votes_len,
            "is_committed?"
        );
        self.commit_num >= block_num
            || self.is_prepared(block_num)
                && self.commit_votes.get(&block_num).is_some_and(|votes| {
                    votes.len() as ReplicaId
                        >= self.config.spec.num_replica - self.config.spec.num_faulty
                })
    }

    fn can_propose(&mut self) -> bool {
        !self.requests.is_empty()
            && self.propose_num - self.commit_num < self.config.max_num_inflight
    }
}

fn block_digest(requests: &[message::Request]) -> Digest {
    let mut state = sha2::Sha256::new();
    for request in requests {
        state.update(request.client_id.to_le_bytes());
        state.update(request.seq.to_le_bytes());
        state.update(&request.op)
    }
    Digest(state.finalize().to_vec())
}

#[derive(Debug)]
pub enum ReplicaAction {
    Nop,
    SendToReplica(ReplicaId, ToReplica),
    SendToAllReplicas(ToReplica), // except loopback

    // same as SendToAllReplicas(ToReplica::PrePrepare(block)) for each block
    Propose(Vec<message::PrePrepare>),
    // same as SendToAllReplicas(ToReplica::Prepare(vote)) + runtime calls
    // insert_prepare(vote) afterward
    Prepare(message::Vote),
    // similar to above but with insert_commit call
    Commit(message::Vote),
    Finalize(Vec<message::Request>),
}

impl Replica {
    pub fn receive(&mut self, message: ToReplica) -> ReplicaAction {
        match message {
            ToReplica::Request(request) => self.receive_request(request),
            ToReplica::PrePrepare(pre_prepare) => self.receive_pre_prepare(pre_prepare),
            ToReplica::Prepare(vote) => self.receive_prepare(vote),
            ToReplica::Commit(vote) => self.receive_commit(vote),
        }
    }

    fn receive_request(&mut self, request: message::Request) -> ReplicaAction {
        if !self.is_primary() {
            // TODO bookkeeping forwarded
            return ReplicaAction::SendToReplica(
                self.config.spec.primary(self.view_num),
                ToReplica::Request(request),
            );
        }
        self.requests.push(request);
        self.propose_blocks()
    }

    fn propose_blocks(&mut self) -> ReplicaAction {
        assert!(self.is_primary());
        if !self.can_propose() {
            ReplicaAction::Nop
        } else {
            let mut pre_prepares = Vec::new();
            while {
                self.propose_num += 1;
                let requests = self
                    .requests
                    .drain(..self.config.max_batch_size.min(self.requests.len()))
                    .collect::<Vec<_>>(); // TODO
                let pre_prepare = message::PrePrepare {
                    view_num: self.view_num,
                    block_num: self.propose_num,
                    digest: block_digest(&requests),
                    sig: Default::default(), // TODO
                    requests,
                };
                let replaced = self
                    .blocks
                    .insert(pre_prepare.block_num, pre_prepare.clone());
                assert!(replaced.is_none());
                pre_prepares.push(pre_prepare);
                self.can_propose()
            } {}
            ReplicaAction::Propose(pre_prepares)
        }
    }

    fn receive_pre_prepare(&mut self, pre_prepare: message::PrePrepare) -> ReplicaAction {
        if pre_prepare.view_num < self.view_num {
            return ReplicaAction::Nop;
        }
        // TODO verify PrePrepare
        // TODO enter view
        assert!(!self.is_primary());
        let block_num = pre_prepare.block_num;
        let digest = pre_prepare.digest.clone();
        if let Some(block) = self.blocks.get(&block_num) {
            if digest == block.digest {
                // primary is retrying on the block/PrePrepare, so try to progress the primary
                // in a minimalism way. note that this only ensures liveness on the primary,
                // other replicas request state transfer when they believe someone (at least the
                // primary) has committed the block
                // by returning ReplicationAction::Prepare(vote) here, current implementation
                // _happens_ to resend Prepare, and additionally resend Commit if applicable
                // further updates may affect this coincidence and cause liveness problems
                // at the same time it would
                // * (re)insert the loopback Prepare (and Commit) which has no effect
                // * re-signing the resent Commit (if any)
                // * resend Prepare (and Commit) to all replicas, while we only need to progress
                //   the primary here
                // so it's a very inefficient fallback path
                let vote = self.prepare_votes[&block_num][&self.config.id].clone();
                return ReplicaAction::Prepare(vote);
            }
            return ReplicaAction::Nop;
        }

        let replaced = self.blocks.insert(pre_prepare.block_num, pre_prepare);
        assert!(replaced.is_none());
        // delayed Prepare vote pruning
        if let Some(votes) = self.prepare_votes.get_mut(&block_num) {
            votes.retain(|_, vote| vote.digest == digest);
        }
        if let Some(votes) = self.commit_votes.get_mut(&block_num) {
            votes.retain(|_, vote| vote.digest == digest);
        }
        let vote = message::Vote {
            view_num: self.view_num,
            block_num,
            digest,
            replica_id: self.config.id,
            sig: Default::default(), //TODO
        };
        ReplicaAction::Prepare(vote)
    }

    fn receive_prepare(&mut self, prepare: message::Vote) -> ReplicaAction {
        if prepare.view_num < self.view_num || self.is_prepared(prepare.block_num) {
            return ReplicaAction::Nop;
        }
        // TODO verify Prepare
        // TODO enter view
        if let Some(block) = self.blocks.get(&prepare.block_num) {
            if prepare.digest != block.digest {
                return ReplicaAction::Nop;
            }
        } // otherwise the vote pruning is delayed until the late PrePrepare arrives
        self.insert_prepare(prepare)
    }

    pub fn insert_prepare(&mut self, prepare: message::Vote) -> ReplicaAction {
        let block_num = prepare.block_num;
        let digest = prepare.digest.clone();
        self.prepare_votes
            .entry(prepare.block_num)
            .or_default()
            .insert(prepare.replica_id, prepare);
        if !self.is_prepared(block_num) {
            return ReplicaAction::Nop;
        }
        let vote = message::Vote {
            view_num: self.view_num,
            block_num,
            digest,
            replica_id: self.config.id,
            sig: Default::default(), // TODO
        };
        ReplicaAction::Commit(vote)
    }

    fn receive_commit(&mut self, commit: message::Vote) -> ReplicaAction {
        if commit.view_num < self.view_num || self.is_committed(commit.block_num) {
            return ReplicaAction::Nop;
        }
        // TODO verify Commit
        // TODO enter view
        if let Some(block) = self.blocks.get(&commit.block_num) {
            if commit.digest != block.digest {
                return ReplicaAction::Nop;
            }
        } // otherwise the vote pruning is delayed until the late PrePrepare arrives
        self.insert_commit(commit)
    }

    pub fn insert_commit(&mut self, commit: message::Vote) -> ReplicaAction {
        self.commit_votes
            .entry(commit.block_num)
            .or_default()
            .insert(commit.replica_id, commit);
        if !self.is_committed(self.commit_num + 1) {
            return ReplicaAction::Nop;
        }
        let mut requests = Vec::new();
        while {
            self.commit_num += 1;
            tracing::debug!(self.config.id, self.commit_num, "committing");
            requests.extend(self.blocks[&self.commit_num].requests.clone());
            self.is_committed(self.commit_num + 1)
        } {}
        ReplicaAction::Finalize(requests)
    }

    pub fn on_finalize(&mut self) -> ReplicaAction {
        if self.is_primary() {
            self.propose_blocks()
        } else {
            ReplicaAction::Nop
        }
    }

    pub fn tick(&mut self) -> ReplicaAction {
        if self.is_primary() {
            let action = if self.ticked_propose_num <= self.commit_num {
                ReplicaAction::Nop
            } else {
                let pre_prepares = (self.commit_num + 1..=self.ticked_propose_num)
                    .map(|block_num| self.blocks[&block_num].clone())
                    .collect();
                ReplicaAction::Propose(pre_prepares)
            };
            self.ticked_propose_num = self.propose_num;
            action
        } else {
            ReplicaAction::Nop
        }
    }
}

#[cfg(test)]
mod tests;
