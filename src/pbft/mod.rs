use std::{
    collections::{BTreeMap, HashMap},
    mem::replace,
};

use sha2::Digest as _;

use crate::{
    common::{ClientId, ReplicaId, RequestPool, client},
    crypto::{
        Digest, PublicKey, SecretKey, Sha256Hash as _, public_key, replica_secret_key, sign, verify,
    },
};

pub mod message;
pub mod parse;
pub mod transport;

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
    // this is kind of a secure bug: malformed replica can "repackage" a Prepare of
    // any other replica into a Commit to pretend that replica has sent Commit
    // can be easily addressed by e.g. adding a nonce in Commit messages
    // deliberately left unresolved to remind this is a prototype implementation
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

type ClientAction = client::Action<ToReplica>;

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
        tracing::warn!(%self.config.id, self.seq, "resend request");
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

    pub secret_key: SecretKey,
    pub public_keys: Vec<PublicKey>,

    pub max_num_inflight: BlockNum,
    pub max_batch_size: usize,
}

impl ReplicaConfig {
    pub fn new_basic(spec: Spec, id: ReplicaId) -> Self {
        let public_keys = (0..spec.num_replica)
            .map(|id| public_key(&replica_secret_key(id)))
            .collect();
        Self {
            spec,
            id,
            secret_key: replica_secret_key(id),
            public_keys,
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
    request_pool: RequestPool,
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
            request_pool: RequestPool::close_loop(), // make it configurable if useful
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
        self.commit_num >= block_num
            || self.is_prepared(block_num)
                && self.commit_votes.get(&block_num).is_some_and(|votes| {
                    votes.len() as ReplicaId
                        >= self.config.spec.num_replica - self.config.spec.num_faulty
                })
    }

    fn can_propose(&mut self) -> bool {
        self.is_primary() && self.propose_num - self.commit_num < self.config.max_num_inflight
    }
}

fn block_digest(requests: &[message::Request]) -> Digest {
    let mut state = sha2::Sha256::new();
    for request in requests {
        state.update(request.client_id.to_le_bytes());
        state.update(request.seq.to_le_bytes());
        state.update(&request.op)
    }
    state.finalize().into()
}

type ReplicaAction = crate::common::ReplicaAction<ToReplica>;
type ReplicaActions = Vec<ReplicaAction>;

impl Replica {
    pub fn receive(&mut self, message: ToReplica, actions: &mut ReplicaActions) {
        if tracing::enabled!(tracing::Level::TRACE) {
            tracing::trace!(?message)
        }
        match message {
            ToReplica::Request(request) => self.receive_request(request, actions),
            ToReplica::PrePrepare(pre_prepare) => self.receive_pre_prepare(pre_prepare, actions),
            ToReplica::Prepare(vote) => self.receive_prepare(vote, actions),
            ToReplica::Commit(vote) => self.receive_commit(vote, actions),
        }
    }

    fn receive_request(&mut self, request: message::Request, actions: &mut ReplicaActions) {
        self.request_pool.push(request.clone());
        if self.is_primary() {
            self.propose_blocks(actions)
        } else {
            tracing::warn!(self.config.id, %request.client_id, request.seq, "forward broadcast request to primary");
            // TODO bookkeeping forwarded
            actions.push(ReplicaAction::SendToReplica(
                self.config.spec.primary(self.view_num),
                ToReplica::Request(request),
            ))
        }
    }

    fn propose_blocks(&mut self, actions: &mut ReplicaActions) {
        if !self.can_propose() {
            return;
        };
        while let Some(requests) = self.request_pool.close_batch(self.config.max_batch_size) {
            self.propose_num += 1;
            let mut pre_prepare = message::PrePrepare {
                view_num: self.view_num,
                block_num: self.propose_num,
                digest: block_digest(&requests),
                sig: Default::default(),
                requests,
            };
            pre_prepare.sig = sign(pre_prepare.sha256(), &self.config.secret_key);
            let replaced = self
                .blocks
                .insert(pre_prepare.block_num, pre_prepare.clone());
            assert!(replaced.is_none());
            actions.push(ReplicaAction::SendToAllReplicas(ToReplica::PrePrepare(
                pre_prepare,
            )));
            if !self.can_propose() {
                break;
            }
        }
    }

    fn receive_pre_prepare(
        &mut self,
        pre_prepare: message::PrePrepare,
        actions: &mut ReplicaActions,
    ) {
        if pre_prepare.view_num < self.view_num {
            return;
        }
        // TODO ignore PrePrepare with anomaly block number
        let public_key =
            &self.config.public_keys[self.config.spec.primary(pre_prepare.view_num) as usize];
        if let Err(err) = verify(pre_prepare.sha256(), public_key, &pre_prepare.sig) {
            tracing::warn!(%err, "malformed PrePrepare");
            return;
        }
        // TODO enter view
        assert!(!self.is_primary());
        let block_num = pre_prepare.block_num;
        let digest = pre_prepare.digest.clone();
        if let Some(block) = self.blocks.get(&block_num) {
            if digest != block.digest {
                tracing::warn!(
                    self.config.id,
                    block_num,
                    preparing = %block.digest,
                    incoming = %digest,
                    "multiple proposals",
                );
            } else {
                tracing::warn!(
                    self.config.id,
                    block_num,
                    "resend vote for duplicated proposal"
                );
                let vote = self.prepare_votes[&block_num][&self.config.id].clone();
                actions.push(ReplicaAction::SendToReplica(
                    self.config.spec.primary(self.view_num),
                    ToReplica::Prepare(vote),
                ));
                if let Some(vote) = self
                    .commit_votes
                    .get(&block_num)
                    .and_then(|votes| votes.get(&self.config.id))
                {
                    actions.push(ReplicaAction::SendToReplica(
                        self.config.spec.primary(self.view_num),
                        ToReplica::Commit(vote.clone()),
                    ))
                }
            }
            return;
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
        let mut vote = message::Vote {
            view_num: self.view_num,
            block_num,
            digest,
            replica_id: self.config.id,
            sig: Default::default(),
        };
        vote.sig = sign(vote.sha256(), &self.config.secret_key);
        actions.push(ReplicaAction::SendToAllReplicas(ToReplica::Prepare(
            vote.clone(),
        )));
        self.insert_prepare(vote, actions)
    }

    fn receive_prepare(&mut self, prepare: message::Vote, actions: &mut ReplicaActions) {
        if prepare.view_num < self.view_num || self.is_prepared(prepare.block_num) {
            return;
        }
        if let Err(err) = verify(
            prepare.sha256(),
            &self.config.public_keys[prepare.replica_id as usize],
            &prepare.sig,
        ) {
            tracing::warn!(%err, "malformed Prepare");
            return;
        }
        // TODO enter view
        if let Some(block) = self.blocks.get(&prepare.block_num) {
            if prepare.digest != block.digest {
                tracing::warn!(
                    prepare.block_num,
                    prepare.replica_id,
                    "Prepare digest mismatch"
                );
                return;
            }
        } else {
            // frequently happens with single machine setting
            tracing::debug!(
                self.config.id,
                prepare.block_num,
                prepare.replica_id,
                "receive Prepare before PrePrepare"
            )
            // the vote pruning is delayed until the late PrePrepare arrives
        }
        self.insert_prepare(prepare, actions)
    }

    pub fn insert_prepare(&mut self, prepare: message::Vote, actions: &mut ReplicaActions) {
        let block_num = prepare.block_num;
        let digest = prepare.digest.clone();
        self.prepare_votes
            .entry(prepare.block_num)
            .or_default()
            .insert(prepare.replica_id, prepare);
        if !self.is_prepared(block_num) {
            return;
        }
        tracing::trace!(self.config.id, block_num, "prepared");
        let mut vote = message::Vote {
            view_num: self.view_num,
            block_num,
            digest,
            replica_id: self.config.id,
            sig: Default::default(),
        };
        vote.sig = sign(vote.sha256(), &self.config.secret_key);
        actions.push(ReplicaAction::SendToAllReplicas(ToReplica::Commit(
            vote.clone(),
        )));
        self.insert_commit(vote, actions)
    }

    fn receive_commit(&mut self, commit: message::Vote, actions: &mut ReplicaActions) {
        if commit.view_num < self.view_num || self.is_committed(commit.block_num) {
            return;
        }
        if let Err(err) = verify(
            commit.sha256(),
            &self.config.public_keys[commit.replica_id as usize],
            &commit.sig,
        ) {
            tracing::warn!(%err, "malformed Commit");
            return;
        }
        // TODO enter view
        if let Some(block) = self.blocks.get(&commit.block_num) {
            if commit.digest != block.digest {
                tracing::warn!(
                    commit.block_num,
                    commit.replica_id,
                    "Commit digest mismatch"
                );
                return;
            }
        } else {
            tracing::debug!(
                self.config.id,
                commit.block_num,
                commit.replica_id,
                "receive Commit before PrePrepare"
            )
            // the vote pruning is delayed until the late PrePrepare arrives
        }
        self.insert_commit(commit, actions)
    }

    pub fn insert_commit(&mut self, commit: message::Vote, actions: &mut ReplicaActions) {
        let block_num = commit.block_num;
        let replica_id = commit.replica_id;
        let replaced = self
            .commit_votes
            .entry(commit.block_num)
            .or_default()
            .insert(commit.replica_id, commit);
        if replaced.is_none() && block_num <= self.ticked_propose_num {
            tracing::warn!(
                self.config.id,
                block_num,
                replica_id,
                "receive slow Commit for ticked proposal"
            )
        }
        if !self.is_committed(self.commit_num + 1) {
            return;
        }
        while {
            self.commit_num += 1;
            tracing::trace!(self.config.id, self.commit_num, "committed");
            let requests = self.blocks[&self.commit_num].requests.clone();
            for request in &requests {
                self.request_pool.commit(request)
            }
            actions.push(ReplicaAction::Finalize(requests));
            self.is_committed(self.commit_num + 1)
        } {}
        self.on_finalize(actions)
    }

    pub fn on_finalize(&mut self, actions: &mut ReplicaActions) {
        if self.is_primary() {
            self.propose_blocks(actions)
        }
    }

    pub fn tick(&mut self, actions: &mut ReplicaActions) {
        if self.is_primary() {
            let ticked_propose_num = replace(&mut self.ticked_propose_num, self.propose_num);
            let block_range = self.commit_num + 1..=ticked_propose_num;
            if !block_range.is_empty() {
                tracing::warn!(self.config.id, block_range = ?block_range, "resend PrePrepare(s)");
                if tracing::enabled!(tracing::Level::DEBUG) {
                    for block_num in block_range.clone() {
                        let prepare_votes = self
                            .prepare_votes
                            .get(&block_num)
                            .map(|votes| votes.keys().collect::<Vec<_>>());
                        let commit_votes = self
                            .commit_votes
                            .get(&block_num)
                            .map(|votes| votes.keys().collect::<Vec<_>>());
                        tracing::debug!(block_num, ?prepare_votes, ?commit_votes);
                    }
                }
                for block_num in block_range {
                    actions.push(ReplicaAction::SendToAllReplicas(ToReplica::PrePrepare(
                        self.blocks[&block_num].clone(),
                    )))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
