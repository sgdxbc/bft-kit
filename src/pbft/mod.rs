use std::{
    collections::{BTreeMap, HashMap},
    mem::{replace, take},
};

use sha2::Digest as _;

use crate::{
    common::{ClientId, ReplicaId, RequestPool, client},
    crypto::{self, Digest, Sha256Hash, sign, verify},
};

mod message;
pub mod parse;
pub mod transport;

type ViewNum = u32;
type BlockNum = u32;

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
    PrePrepare(message::PrePrepare, Vec<message::Request>),
    // this is kind of a secure bug: malformed replica can "repackage" a Prepare of
    // any other replica into a Commit to pretend that replica has sent Commit
    // can be easily addressed by e.g. adding a nonce in Commit messages
    // deliberately left unresolved to remind this is a prototype implementation
    Prepare(message::Vote),
    Commit(message::Vote),
    // TODO recover path messages
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
pub struct ReplicaCoreConfig {
    pub spec: Spec,
    pub id: ReplicaId,
    pub max_num_inflight: BlockNum,
    pub max_batch_size: usize,
}

impl ReplicaCoreConfig {
    pub fn new_basic(spec: Spec, id: ReplicaId) -> Self {
        Self {
            spec,
            id,
            max_num_inflight: 1,
            max_batch_size: 1,
        }
    }
}

pub struct ReplicaCore {
    config: ReplicaCoreConfig,
    view_num: ViewNum,
    pool: RequestPool,
    blocks: BTreeMap<BlockNum, Block>,
    propose_num: BlockNum,
    finalize_num: BlockNum,
}

use crate::crypto::Sig;

#[derive(Debug, Clone)]
pub struct Block {
    requests: Vec<message::Request>,
    digest: Digest, // block_digest(&requests), cached
    #[allow(unused)]
    pre_prepare: (ViewNum, Sig),
    prepare_quorum: Option<Quorum<Sig>>,
    commit_quorum: Option<Quorum<Sig>>,
}

type Quorum<T> = HashMap<ReplicaId, T>;

pub enum ReplicaCoreEvent {
    // main path: Request -> Propose -> Proposal (-> Prepare) -> PrepareQuorum
    // -> Commit -> CommitQuorum -> Finalize
    Request(message::Request),
    Proposal(BlockNum, Block), // the `requests` are saved for later Finalize action
    PrepareQuorum(BlockNum, Quorum<Sig>),
    CommitQuorum(BlockNum, Quorum<Sig>),
    // recover path: (Request ->) Forward -> ViewExpired -> ViewChange
    // -> ViewChangeQuorum -> NewView -> EnterView (event) -> EnterView (action)
    ViewExpired,
    ViewChangeQuorum(ViewNum, Quorum<BTreeMap<BlockNum, Block>>),
    EnterView(
        ViewNum,
        Quorum<BTreeMap<BlockNum, Block>>,
        BTreeMap<BlockNum, Block>,
    ),
}

#[derive(Debug)]
pub enum ReplicaCoreAction {
    // main path

    // should package the requests into a Block, package block digest into a
    // PrePrepare and disseminate to all replicas including the proposer itself
    // and start to collect a Prepare quorum for the block
    Propose(BlockNum, Vec<message::Request>),
    // invariants on main path
    // * Finalize with strict increasing block numbers without gap. Prepare and
    //   Commit may be out of order regarding block numbers
    // * every `Finalize`d block is out of the concern of the protocol, so any
    //   related bookkeeping should be garbage collected
    // * Prepare and Commit may happen on the same block number for multiple times
    //   (for same or different blocks), but at most once per view (i.e. must first
    //   NewView then Prepare again), and never after the block number has been
    //   `Finalize`d. furthermore, a block is only `Commit`-ed for a block number
    //   after it is `Prepare`d for the same block number, and only `Finalize`d
    //   after it is `Commit`-ed. so Commit and Finalize are without mentioning the
    //   block (Digest): the convention is to Commit and Finalize the currently
    //   `Prepare`-ing block (Digest)
    // * (Prepare becomes Propose on primary replica)

    // should package the Digest into a Prepare, disseminate to all replicas and
    // start to collect a Prepare quorum for Digest
    Prepare(BlockNum, Digest),
    // should package the `Prepare`-ing Digest into a Commit, disseminate to all
    // replicas and start to collect a Commit quorum for the Digest
    Commit(BlockNum),
    // should produce a Finalize action to the requests of the block
    Finalize(BlockNum),

    // recover path
    Forward(ReplicaId, message::Request),
    ViewChange(ViewNum, BTreeMap<BlockNum, Block>),
    NewView(
        ViewNum,
        Quorum<BTreeMap<BlockNum, Block>>,
        BTreeMap<BlockNum, Vec<message::Request>>,
    ),
    EnterView,
}

pub type ReplicaCoreActions = Vec<ReplicaCoreAction>;

impl ReplicaCore {
    pub fn new(config: ReplicaCoreConfig) -> Self {
        Self {
            config,
            view_num: 0,
            pool: RequestPool::close_loop(), // TODO configurable?
            blocks: Default::default(),
            propose_num: 0,
            finalize_num: 0,
        }
    }

    fn is_primary_of(&self, view_num: ViewNum) -> bool {
        self.config.spec.primary(view_num) == self.config.id
    }

    fn is_primary(&self) -> bool {
        self.is_primary_of(self.view_num)
    }

    fn can_propose(&self) -> bool {
        self.is_primary() && self.propose_num - self.finalize_num < self.config.max_num_inflight
    }

    pub fn handle(&mut self, event: ReplicaCoreEvent, actions: &mut ReplicaCoreActions) {
        match event {
            ReplicaCoreEvent::Request(request) => {
                self.pool.push(request.clone());
                if !self.is_primary() {
                    actions.push(ReplicaCoreAction::Forward(
                        self.config.spec.primary(self.view_num),
                        request,
                    ));
                    return;
                }
                self.propose(actions)
            }
            ReplicaCoreEvent::Proposal(block_num, block) => {
                if self.blocks.contains_key(&block_num) {
                    return;
                }
                let digest = block.digest.clone();
                self.blocks.insert(block_num, block);
                if !self.is_primary() {
                    actions.push(ReplicaCoreAction::Prepare(block_num, digest))
                }
            }
            ReplicaCoreEvent::PrepareQuorum(block_num, prepare_quorum) => {
                let block = self.blocks.get_mut(&block_num).unwrap();
                let replaced = block.prepare_quorum.replace(prepare_quorum);
                assert!(replaced.is_none());
                actions.push(ReplicaCoreAction::Commit(block_num))
            }
            ReplicaCoreEvent::CommitQuorum(block_num, commit_quorum) => {
                let block = self.blocks.get_mut(&block_num).unwrap();
                let replaced = block.commit_quorum.replace(commit_quorum);
                assert!(replaced.is_none());
                let mut block;
                while {
                    block = self.blocks.get(&(self.finalize_num + 1));
                    block.is_some_and(|block| {
                        block.prepare_quorum.is_some() && block.commit_quorum.is_some()
                    })
                } {
                    self.finalize_num += 1;
                    actions.push(ReplicaCoreAction::Finalize(self.finalize_num))
                }
                self.propose(actions)
            }
            ReplicaCoreEvent::ViewExpired => {
                // TODO checkpoint
                actions.push(ReplicaCoreAction::ViewChange(
                    self.view_num + 1,
                    self.blocks.clone(),
                ))
            }
            ReplicaCoreEvent::ViewChangeQuorum(view_num, quorum) => {
                if !self.is_primary_of(view_num) {
                    return;
                }
                self.view_num = view_num;
                let proposals = Self::view_change_proposals(&quorum);
                actions.push(ReplicaCoreAction::NewView(view_num, quorum, proposals))
            }
            ReplicaCoreEvent::EnterView(view_num, quorum, proposals) => {
                assert!(!self.is_primary_of(view_num));
                for ((block_num, block), (other_block_num, requests)) in
                    proposals.iter().zip(&Self::view_change_proposals(&quorum))
                {
                    if block_num != other_block_num || block.digest != block_digest(requests) {
                        return;
                    }
                }
                self.view_num = view_num;
                actions.push(ReplicaCoreAction::EnterView);
                let Some((&min_num, _)) = proposals.first_key_value() else {
                    return;
                };
                self.blocks.split_off(&min_num);
                for (block_num, block) in proposals {
                    actions.push(ReplicaCoreAction::Prepare(block_num, block.digest.clone()));
                    self.blocks.insert(block_num, block);
                }
            }
        }
    }

    fn propose(&mut self, actions: &mut ReplicaCoreActions) {
        while self.can_propose() {
            let Some(requests) = self.pool.close_batch(self.config.max_batch_size) else {
                return;
            };
            self.propose_num += 1;
            actions.push(ReplicaCoreAction::Propose(self.propose_num, requests))
        }
    }

    #[allow(unused)]
    fn view_change_proposals(
        quorum: &Quorum<BTreeMap<BlockNum, Block>>,
    ) -> BTreeMap<BlockNum, Vec<message::Request>> {
        Default::default() // TODO
    }

    fn is_prepared(&self, block_num: BlockNum) -> bool {
        block_num < self.finalize_num
            || self
                .blocks
                .get(&block_num)
                .is_some_and(|block| block.prepare_quorum.is_some())
    }

    fn is_committed(&self, block_num: BlockNum) -> bool {
        block_num < self.finalize_num
            || self.is_prepared(block_num) && self.blocks[&block_num].commit_quorum.is_some()
    }
}

pub struct Replica {
    core: ReplicaCore,
    core_actions: ReplicaCoreActions,
    crypto_config: crypto::ReplicaConfig,
    block_scratches: BTreeMap<BlockNum, BlockScratch>,
    ticked_scratch_num: BlockNum,
}

#[derive(Default)]
struct BlockScratch {
    prepare_votes: Vec<message::Vote>,
    prepare_quorum: Quorum<Sig>,
    commit_votes: Vec<message::Vote>,
    commit_quorum: Quorum<Sig>,
}

impl Replica {
    pub fn new(core_config: ReplicaCoreConfig) -> Self {
        Self {
            crypto_config: crypto::ReplicaConfig::new(core_config.id, core_config.spec.num_replica),
            core: ReplicaCore::new(core_config),
            core_actions: Default::default(),
            block_scratches: Default::default(),
            ticked_scratch_num: 0,
        }
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
        tracing::trace!(?message);
        match message {
            ToReplica::Request(request) => self
                .core
                .handle(ReplicaCoreEvent::Request(request), &mut self.core_actions),
            ToReplica::PrePrepare(pre_prepare, requests) => {
                if pre_prepare.view_num != self.core.view_num {
                    return;
                }
                if let Some(block) = self.core.blocks.get(&pre_prepare.block_num) {
                    if block.digest == pre_prepare.digest {
                        // liveness measurement of primary: duplicated PrePrepare
                        // (re)send whatever we have to best effort progress the primary
                        // note that only primary is progressed by this path, so client progress is
                        // not guaranteed (solely) by this
                        let mut vote = message::Vote {
                            view_num: self.core.view_num,
                            block_num: pre_prepare.block_num,
                            digest: pre_prepare.digest,
                            replica_id: self.core.config.id,
                            sig: Default::default(),
                        };
                        vote.sig = sign(vote.sha256(), &self.crypto_config.secret_key);
                        let replica_id = self.core.config.spec.primary(self.core.view_num);
                        actions.push(ReplicaAction::SendToReplica(
                            replica_id,
                            ToReplica::Prepare(vote.clone()),
                        ));
                        if block.prepare_quorum.is_some() {
                            actions.push(ReplicaAction::SendToReplica(
                                replica_id,
                                ToReplica::Commit(vote.clone()),
                            ));
                        }
                    }
                    return;
                }
                if let Err(err) = verify(
                    pre_prepare.sha256(),
                    &self.crypto_config.public_keys
                        [self.core.config.spec.primary(pre_prepare.view_num) as usize],
                    &pre_prepare.sig,
                ) {
                    tracing::warn!(%err, ?pre_prepare, "malformed PrePrepare");
                    return;
                }
                let digest = block_digest(&requests);
                if pre_prepare.digest != digest {
                    tracing::warn!(?pre_prepare, "malformed PrePrepare (digest mismatch)");
                    return;
                }
                let block = Block {
                    requests: requests.clone(),
                    digest,
                    pre_prepare: (pre_prepare.view_num, pre_prepare.sig),
                    prepare_quorum: None,
                    commit_quorum: None,
                };
                self.core.handle(
                    ReplicaCoreEvent::Proposal(pre_prepare.block_num, block),
                    &mut self.core_actions,
                )
            }
            ToReplica::Prepare(prepare) => {
                if prepare.view_num != self.core.view_num
                    || self.core.is_prepared(prepare.block_num)
                {
                    return;
                }
                if let Err(err) = verify(
                    prepare.sha256(),
                    &self.crypto_config.public_keys[prepare.replica_id as usize],
                    &prepare.sig,
                ) {
                    tracing::warn!(%err, "malformed Prepare");
                    return;
                }
                let scratch = self.block_scratches.entry(prepare.block_num).or_default();
                if let Some(digest) = self
                    .core
                    .blocks
                    .get(&prepare.block_num)
                    .map(|block| &block.digest)
                {
                    if &prepare.digest != digest {
                        return;
                    }
                    scratch
                        .prepare_quorum
                        .insert(prepare.replica_id, prepare.sig);
                    Self::check_prepare_quorum(
                        prepare.block_num,
                        scratch,
                        &mut self.core,
                        &mut self.core_actions,
                    )
                } else {
                    scratch.prepare_votes.push(prepare)
                }
            }
            ToReplica::Commit(commit) => {
                if commit.view_num != self.core.view_num || self.core.is_committed(commit.block_num)
                {
                    return;
                }
                if let Err(err) = verify(
                    commit.sha256(),
                    &self.crypto_config.public_keys[commit.replica_id as usize],
                    &commit.sig,
                ) {
                    tracing::warn!(%err, "malformed Commit");
                    return;
                }
                let scratch = self.block_scratches.entry(commit.block_num).or_default();
                if let Some(digest) = self
                    .core
                    .blocks
                    .get(&commit.block_num)
                    .map(|block| &block.digest)
                {
                    if &commit.digest != digest {
                        return;
                    }
                    scratch.commit_quorum.insert(commit.replica_id, commit.sig);
                    Self::check_commit_quorum(
                        commit.block_num,
                        scratch,
                        &mut self.core,
                        &mut self.core_actions,
                    )
                } else {
                    scratch.commit_votes.push(commit)
                }
            }
        }
        // would like to drain but need to use self.core_actions in the loop
        for action in take(&mut self.core_actions) {
            tracing::trace!(?action);
            match action {
                ReplicaCoreAction::Propose(block_num, requests) => {
                    let digest = block_digest(&requests);
                    let mut pre_prepare = message::PrePrepare {
                        view_num: self.core.view_num,
                        block_num,
                        digest: digest.clone(),
                        sig: Default::default(),
                    };
                    pre_prepare.sig = sign(pre_prepare.sha256(), &self.crypto_config.secret_key);
                    let block = Block {
                        requests: requests.clone(),
                        digest,
                        pre_prepare: (pre_prepare.view_num, pre_prepare.sig.clone()),
                        prepare_quorum: None,
                        commit_quorum: None,
                    };
                    actions.push(ReplicaAction::SendToAllReplicas(ToReplica::PrePrepare(
                        pre_prepare,
                        requests,
                    )));
                    self.core.handle(
                        ReplicaCoreEvent::Proposal(block_num, block),
                        &mut self.core_actions,
                    )
                }
                ReplicaCoreAction::Prepare(block_num, digest) => {
                    let scratch = self.block_scratches.entry(block_num).or_default();
                    let matching_vote = |vote: message::Vote| {
                        if vote.digest == digest {
                            Some((vote.replica_id, vote.sig))
                        } else {
                            None
                        }
                    };
                    scratch.prepare_quorum.extend(
                        take(&mut scratch.prepare_votes)
                            .into_iter()
                            .filter_map(matching_vote),
                    );
                    scratch.commit_quorum.extend(
                        take(&mut scratch.commit_votes)
                            .into_iter()
                            .filter_map(matching_vote),
                    );
                    let mut prepare = message::Vote {
                        view_num: self.core.view_num,
                        block_num,
                        digest,
                        replica_id: self.core.config.id,
                        sig: Default::default(),
                    };
                    prepare.sig = sign(prepare.sha256(), &self.crypto_config.secret_key);
                    scratch
                        .prepare_quorum
                        .insert(prepare.replica_id, prepare.sig.clone());
                    actions.push(ReplicaAction::SendToAllReplicas(ToReplica::Prepare(
                        prepare,
                    )));
                    Self::check_prepare_quorum(
                        block_num,
                        scratch,
                        &mut self.core,
                        &mut self.core_actions,
                    )
                    // the PrepareQuorum event will trigger a Commit action and we will check for
                    // commit quorum when performing that Commit nevertheless. not check here to
                    // avoid duplicated CommitQuorum event
                    // this is really coupled with ReplicaCore...
                }
                ReplicaCoreAction::Commit(block_num) => {
                    let scratch = self.block_scratches.get_mut(&block_num).unwrap();
                    let mut commit = message::Vote {
                        view_num: self.core.view_num,
                        block_num,
                        digest: self.core.blocks[&block_num].digest.clone(),
                        replica_id: self.core.config.id,
                        sig: Default::default(),
                    };
                    commit.sig = sign(commit.sha256(), &self.crypto_config.secret_key);
                    scratch
                        .commit_quorum
                        .insert(commit.replica_id, commit.sig.clone());
                    actions.push(ReplicaAction::SendToAllReplicas(ToReplica::Commit(commit)));
                    Self::check_commit_quorum(
                        block_num,
                        scratch,
                        &mut self.core,
                        &mut self.core_actions,
                    )
                }
                ReplicaCoreAction::Finalize(block_num) => {
                    let removed = self.block_scratches.remove(&block_num);
                    assert!(removed.is_some()); // really?
                    actions.push(ReplicaAction::Finalize(
                        self.core.blocks[&block_num].requests.clone(),
                    ));
                }
                ReplicaCoreAction::Forward(replica_id, request) => {
                    actions.push(ReplicaAction::SendToReplica(
                        replica_id,
                        ToReplica::Request(request),
                    ));
                    // TODO view expiration timer
                }
                #[allow(unused)]
                ReplicaCoreAction::ViewChange(view_num, blocks) => todo!(),
                #[allow(unused)]
                ReplicaCoreAction::NewView(view_num, view_changes, proposals) => {
                    self.block_scratches.clear();
                    todo!()
                }
                ReplicaCoreAction::EnterView => self.block_scratches.clear(),
            }
        }
    }

    fn check_prepare_quorum(
        block_num: u32,
        scratch: &mut BlockScratch,
        core: &mut ReplicaCore,
        core_actions: &mut ReplicaCoreActions,
    ) {
        // if prepare quorum is not empty, PrePrepare must present
        if scratch.prepare_quorum.len() as ReplicaId + 1
            >= core.config.spec.num_replica - core.config.spec.num_faulty
        {
            core.handle(
                ReplicaCoreEvent::PrepareQuorum(block_num, take(&mut scratch.prepare_quorum)),
                core_actions,
            );
        }
    }

    fn check_commit_quorum(
        block_num: u32,
        scratch: &mut BlockScratch,
        core: &mut ReplicaCore,
        core_actions: &mut ReplicaCoreActions,
    ) {
        if core.is_prepared(block_num)
            && scratch.commit_quorum.len() as ReplicaId
                >= core.config.spec.num_replica - core.config.spec.num_faulty
        {
            core.handle(
                ReplicaCoreEvent::CommitQuorum(block_num, take(&mut scratch.commit_quorum)),
                core_actions,
            );
        }
    }

    pub fn tick(&mut self, actions: &mut ReplicaActions) {
        let ticked2_scratch_num = replace(&mut self.ticked_scratch_num, self.core.propose_num);
        if !self.core.is_primary() {
            // TODO tick view expiration
            return;
        }
        for block_num in self.core.finalize_num + 1..=ticked2_scratch_num {
            if self.core.is_committed(block_num) {
                continue;
            }
            let block = &self.core.blocks[&block_num];
            let mut pre_prepare = message::PrePrepare {
                view_num: self.core.view_num,
                block_num,
                digest: block.digest.clone(),
                sig: Default::default(),
            };
            pre_prepare.sig = sign(pre_prepare.sha256(), &self.crypto_config.secret_key);
            actions.push(ReplicaAction::SendToAllReplicas(ToReplica::PrePrepare(
                pre_prepare,
                block.requests.clone(),
            )))
        }
    }
}

#[cfg(test)]
mod tests;
