use std::{collections::HashMap, mem::take};

use bincode::{Decode, Encode};

use crate::{
    CommandPool,
    crypto::{Digest, DigestHash as _, PeerConfig, Sig, UpdateHash, sign, verify},
};

#[cfg(test)]
mod tests;

pub type Command = crate::Command;
type Commands = Vec<Command>;

// internal aliases for readability
type OpNum = u64;
type ViewNum = u64;
type ReplicaId = u16;

#[derive(Debug, Clone, Encode, Decode)]
pub enum Message {
    PrePrepare(PrePrepare, Commands),
    Prepare(Vote),
    Commit(Vote),
}

pub trait Context {
    fn send_message(&mut self, message: Message);
    fn finalize(&mut self, commands: Commands, view_num: ViewNum);
}

pub struct Replica {
    config: ReplicaConfig,
    core: ReplicaCore,
    core_context: ReplicaCoreContext,
    reordered_prepares: HashMap<OpNum, Vec<Vote>>,
    reordered_commits: HashMap<OpNum, Vec<Vote>>,
}

pub struct ReplicaConfig {
    crypto: PeerConfig,
}

impl Replica {
    pub fn new(core_config: ReplicaCoreConfig, config: ReplicaConfig) -> Self {
        Self {
            config,
            core: ReplicaCore::new(core_config),
            core_context: ReplicaCoreContext(Default::default()),
            reordered_prepares: Default::default(),
            reordered_commits: Default::default(),
        }
    }

    pub fn submit(&mut self, command: Command, context: &mut impl Context) {
        self.core.submit(command, &mut self.core_context);
        self.perform_core_actions(context)
    }

    pub fn receive(&mut self, message: Message, context: &mut impl Context) {
        match message {
            Message::PrePrepare(pre_prepare, commands) => {
                if pre_prepare.view_num != self.core.view_num {
                    tracing::warn!(
                        %self.core.view_num,
                        %pre_prepare.view_num,
                        "received PrePrepare for wrong view number"
                    );
                    return;
                }
                if let Some(op) = self.core.ops.get(&pre_prepare.op_num) {
                    tracing::warn!(
                        %self.core.proposed_op_num,
                        %pre_prepare.op_num,
                        "received PrePrepare for already prepared op"
                    );
                    if op.pre_prepare.digest != pre_prepare.digest {
                        tracing::warn!(
                            %op.pre_prepare.digest,
                            %pre_prepare.digest,
                            "    ...and with wrong digest"
                        );
                    }
                    return;
                }
                if let Err(err) = verify(
                    &pre_prepare,
                    &self.config.crypto.public_keys
                        [self.core.config.primary_of(self.core.view_num) as usize],
                    &pre_prepare.sig,
                ) {
                    tracing::warn!(
                        %err,
                        "received PrePrepare with invalid signature"
                    );
                    return;
                }
                self.core
                    .handle_pre_prepare(pre_prepare, commands, &mut self.core_context);
            }
            Message::Prepare(vote) => {
                // it's a bit leaky to check can_commit here as ReplicaCore can check it by
                // itself as easily
                // however doing it anyway to avoid unnecessary verification overhead
                if self.core.can_commit(vote.op_num) {
                    return;
                }
                if vote.view_num != self.core.view_num {
                    tracing::warn!(
                        %self.core.view_num,
                        %vote.view_num,
                        "received Prepare for wrong view number"
                    );
                    return;
                }
                let Some(op) = self.core.ops.get(&vote.op_num) else {
                    self.reordered_prepares
                        .entry(vote.op_num)
                        .or_default()
                        .push(vote);
                    return;
                };
                if vote.digest != op.pre_prepare.digest {
                    tracing::warn!(
                        %op.pre_prepare.digest,
                        %vote.digest,
                        "received Prepare with wrong digest"
                    );
                    return;
                }
                if let Err(err) = verify(
                    &vote,
                    &self.config.crypto.public_keys[vote.replica_id as usize],
                    &vote.sig,
                ) {
                    tracing::warn!(
                        %err,
                        "received Prepare with invalid signature"
                    );
                    return;
                }
                self.core.handle_prepare(vote, &mut self.core_context);
            }
            Message::Commit(vote) => {
                if self.core.can_finalize(vote.op_num) {
                    return;
                }
                if vote.view_num != self.core.view_num {
                    tracing::warn!(
                        %self.core.view_num,
                        %vote.view_num,
                        "received Commit for wrong view number"
                    );
                    return;
                }
                let Some(op) = self.core.ops.get(&vote.op_num) else {
                    self.reordered_commits
                        .entry(vote.op_num)
                        .or_default()
                        .push(vote);
                    return;
                };
                if vote.digest != op.pre_prepare.digest {
                    tracing::warn!(
                        %op.pre_prepare.digest,
                        %vote.digest,
                        "received Commit with wrong digest"
                    );
                    return;
                }
                if let Err(err) = verify(
                    &vote,
                    &self.config.crypto.public_keys[vote.replica_id as usize],
                    &vote.sig,
                ) {
                    tracing::warn!(
                        %err,
                        "received Commit with invalid signature"
                    );
                    return;
                }
                self.core.handle_commit(vote, &mut self.core_context);
            }
        }
        self.perform_core_actions(context)
    }

    fn perform_core_actions(&mut self, context: &mut impl Context) {
        for action in take(&mut self.core_context.0) {
            match action {
                ReplicaCoreAction::Propose(op_num, commands) => {
                    let mut pre_prepare = PrePrepare {
                        view_num: self.core.view_num,
                        op_num,
                        digest: (&*commands).digest(),
                        sig: Default::default(),
                    };
                    pre_prepare.sig = sign(&pre_prepare, &self.config.crypto.secret_key);
                    context
                        .send_message(Message::PrePrepare(pre_prepare.clone(), commands.clone()));
                    self.core.handle_pre_prepare(
                        pre_prepare,
                        commands.clone(),
                        &mut self.core_context,
                    );
                }
                ReplicaCoreAction::Prepare(op_num) => {
                    let digest = self
                        .core
                        .ops
                        .get(&op_num)
                        .unwrap()
                        .pre_prepare
                        .digest
                        .clone();
                    let mut vote = Vote {
                        view_num: self.core.view_num,
                        op_num,
                        replica_id: self.core.config.id,
                        digest: digest.clone(),
                        sig: Default::default(),
                    };
                    vote.sig = sign(&vote, &self.config.crypto.secret_key);
                    context.send_message(Message::Prepare(vote.clone()));
                    self.core.handle_prepare(vote, &mut self.core_context);
                    if let Some(prepares) = self.reordered_prepares.remove(&op_num) {
                        for prepare in prepares {
                            if self.core.can_commit(op_num) {
                                break;
                            }
                            if prepare.digest != digest {
                                tracing::warn!(
                                    %digest,
                                    %prepare.digest,
                                    "received (reordered) Prepare with wrong digest"
                                );
                                continue;
                            }
                            if let Err(err) = verify(
                                &prepare,
                                &self.config.crypto.public_keys[prepare.replica_id as usize],
                                &prepare.sig,
                            ) {
                                tracing::warn!(%err, "received (reordered) Prepare with invalid signature");
                            }
                            self.core.handle_prepare(prepare, &mut self.core_context)
                        }
                    }
                }
                ReplicaCoreAction::Commit(op_num) => {
                    let digest = self
                        .core
                        .ops
                        .get(&op_num)
                        .unwrap()
                        .pre_prepare
                        .digest
                        .clone();
                    let mut vote = Vote {
                        view_num: self.core.view_num,
                        op_num,
                        replica_id: self.core.config.id,
                        digest: digest.clone(),
                        sig: Default::default(),
                    };
                    vote.sig = sign(&vote, &self.config.crypto.secret_key);
                    context.send_message(Message::Commit(vote.clone()));
                    self.core.handle_commit(vote, &mut self.core_context);
                    if let Some(commits) = self.reordered_commits.remove(&op_num) {
                        for commit in commits {
                            if self.core.can_finalize(op_num) {
                                break;
                            }
                            if commit.digest != digest {
                                tracing::warn!(
                                    %digest,
                                    %commit.digest,
                                    "received (reordered) Commit with wrong digest"
                                );
                                continue;
                            }
                            if let Err(err) = verify(
                                &commit,
                                &self.config.crypto.public_keys[commit.replica_id as usize],
                                &commit.sig,
                            ) {
                                tracing::warn!(%err, "received (reordered) Commit with invalid signature");
                            }
                            self.core.handle_commit(commit, &mut self.core_context)
                        }
                    }
                }
                ReplicaCoreAction::Finalize(op_num) => {
                    let commands = self.core.ops.get(&op_num).unwrap().commands.clone();
                    context.finalize(commands, self.core.view_num)
                }
            }
        }
        if !self.core_context.0.is_empty() {
            self.perform_core_actions(context)
        }
    }
}

struct ReplicaCore {
    config: ReplicaCoreConfig,
    view_num: ViewNum,
    // following original work, "op" refers to a batch of commands, while in other
    // works it is usually called block
    proposed_op_num: OpNum,  // maintained only by primary
    finalized_op_num: OpNum, // maintained by all
    ops: HashMap<OpNum, Op>,
    command_pool: CommandPool,
}

pub struct ReplicaCoreConfig {
    id: ReplicaId,
    num_replica: ReplicaId,
    num_faulty_replica: ReplicaId,
    num_inflight_block: OpNum,
    max_batch_size: usize,
}

struct Op {
    commands: Commands,
    pre_prepare: PrePrepare,
    prepare_quorum: HashMap<ReplicaId, Vote>,
    commit_quorum: HashMap<ReplicaId, Vote>,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct PrePrepare {
    view_num: ViewNum,
    op_num: OpNum,
    digest: Digest,
    sig: Sig,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Vote {
    view_num: ViewNum,
    op_num: OpNum,
    digest: Digest,
    replica_id: ReplicaId,
    sig: Sig,
}

enum ReplicaCoreAction {
    Propose(OpNum, Commands),
    Prepare(OpNum),
    Commit(OpNum),
    Finalize(OpNum),
}

struct ReplicaCoreContext(Vec<ReplicaCoreAction>);

impl ReplicaCoreContext {
    fn propose(&mut self, op_num: OpNum, commands: Commands) {
        self.0.push(ReplicaCoreAction::Propose(op_num, commands))
    }

    fn prepare(&mut self, op_num: OpNum) {
        self.0.push(ReplicaCoreAction::Prepare(op_num))
    }

    fn commit(&mut self, op_num: OpNum) {
        self.0.push(ReplicaCoreAction::Commit(op_num))
    }

    fn finalize(&mut self, op_num: OpNum) {
        self.0.push(ReplicaCoreAction::Finalize(op_num))
    }
}

impl ReplicaCoreConfig {
    fn is_primary_of(&self, view_num: ViewNum) -> bool {
        self.primary_of(view_num) == self.id as ViewNum
    }

    fn primary_of(&self, view_num: ViewNum) -> ViewNum {
        view_num % self.num_replica as ViewNum
    }
}

impl ReplicaCore {
    fn new(config: ReplicaCoreConfig) -> Self {
        Self {
            config,
            view_num: 0,
            proposed_op_num: 0,
            finalized_op_num: 0,
            ops: Default::default(),
            command_pool: CommandPool::close_loop(), // TODO: make it configurable
        }
    }

    // actions
    fn submit(&mut self, command: Command, context: &mut ReplicaCoreContext) {
        if !self.config.is_primary_of(self.view_num) {
            //
            return;
        }
        self.command_pool.push(command);
        self.propose(context)
    }

    // event handlers
    fn handle_pre_prepare(
        &mut self,
        pre_prepare: PrePrepare,
        commands: Commands,
        context: &mut ReplicaCoreContext,
    ) {
        let op_num = pre_prepare.op_num;
        let replaced = self.ops.insert(
            op_num,
            Op {
                commands,
                pre_prepare,
                prepare_quorum: Default::default(),
                commit_quorum: Default::default(),
            },
        );
        assert!(replaced.is_none());
        if !self.config.is_primary_of(self.view_num) {
            context.prepare(op_num)
        }
    }

    fn handle_prepare(&mut self, vote: Vote, context: &mut ReplicaCoreContext) {
        let op_num = vote.op_num;
        let op = self.ops.get_mut(&op_num).unwrap();
        op.prepare_quorum.insert(vote.replica_id, vote);
        if self.can_commit(op_num) {
            context.commit(op_num)
        }
    }

    fn handle_commit(&mut self, vote: Vote, context: &mut ReplicaCoreContext) {
        let op_num = vote.op_num;
        let op = self.ops.get_mut(&op_num).unwrap();
        op.commit_quorum.insert(vote.replica_id, vote);
        while self.can_finalize(self.finalized_op_num + 1) {
            self.finalized_op_num += 1;
            context.finalize(self.finalized_op_num)
        }
        self.propose(context)
    }

    // internal helpers
    fn can_propose(&self, view_num: ViewNum) -> bool {
        self.config.is_primary_of(view_num)
            && self.proposed_op_num < self.finalized_op_num + self.config.num_inflight_block
    }

    fn can_commit(&self, op_num: OpNum) -> bool {
        op_num <= self.finalized_op_num
            || if let Some(op) = self.ops.get(&op_num) {
                op.prepare_quorum.len() as ReplicaId + 1
                    >= self.config.num_replica - self.config.num_faulty_replica
            } else {
                false
            }
    }

    fn can_finalize(&self, op_num: OpNum) -> bool {
        op_num <= self.finalized_op_num
            || self.can_commit(op_num)
                && self.ops.get(&op_num).unwrap().commit_quorum.len() as ReplicaId
                    >= self.config.num_replica - self.config.num_faulty_replica
    }

    fn propose(&mut self, context: &mut ReplicaCoreContext) {
        while self.can_propose(self.view_num) {
            let Some(commands) = self.command_pool.close_batch(self.config.max_batch_size) else {
                return;
            };
            self.proposed_op_num += 1;
            context.propose(self.proposed_op_num, commands)
        }
    }
}

impl UpdateHash for PrePrepare {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.view_num.to_le_bytes());
        state.update(self.op_num.to_le_bytes());
        self.digest.update(state)
    }
}

impl UpdateHash for Vote {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.view_num.to_le_bytes());
        state.update(self.op_num.to_le_bytes());
        self.digest.update(state)
    }
}
