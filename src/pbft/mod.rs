use std::collections::HashMap;

use crate::crypto::Sig;

pub struct Request {}

// internal aliases for readability
type OpNum = u64;
type ViewNum = u64;
type ReplicaId = u16;

pub struct Block {
    op_num: OpNum,
    requests: Vec<Request>,
    view_num: ViewNum,
}

pub enum Message {}

pub trait Context {
    fn send_message(&mut self, message: Message);
    fn finalize(&mut self, block: &Block);
}

pub struct Replica {
    core: ReplicaCore,
    core_context: ReplicaCoreContext,
}

struct ReplicaCore {
    config: ReplicaCoreConfig,
    view_num: ViewNum,
    prepared_op_num: OpNum,
    committed_op_num: OpNum,
    pre_prepares: HashMap<OpNum, PrePrepare>,
    submitted_requests: Vec<Request>,
    prepared_quorums: HashMap<OpNum, HashMap<ReplicaId, Vote>>,
    committed_quorums: HashMap<OpNum, HashMap<ReplicaId, Vote>>,
}

struct PrePrepare {
    block: Block,
    sig: Sig,
}

struct Vote {
    view_num: ViewNum,
    op_num: OpNum,
    replica_id: ReplicaId,
    sig: Sig,
}

struct ReplicaCoreConfig {
    id: ReplicaId,
    num_replica: ReplicaId,
    num_faulty_replica: ReplicaId,
    num_inflight_block: OpNum,
    max_block_size: usize,
}

enum ReplicaCoreCommand {
    Propose(OpNum, Block),
    Prepare(OpNum),
    Commit(OpNum),
    Finalize(OpNum),
}

struct ReplicaCoreContext(Vec<ReplicaCoreCommand>);

impl ReplicaCoreContext {
    fn propose(&mut self, op_num: OpNum, block: Block) {
        self.0.push(ReplicaCoreCommand::Propose(op_num, block))
    }

    fn prepare(&mut self, op_num: OpNum) {
        self.0.push(ReplicaCoreCommand::Prepare(op_num))
    }

    fn commit(&mut self, op_num: OpNum) {
        self.0.push(ReplicaCoreCommand::Commit(op_num))
    }

    fn finalize(&mut self, op_num: OpNum) {
        self.0.push(ReplicaCoreCommand::Finalize(op_num))
    }
}

impl ReplicaCoreConfig {
    fn is_primary_of(&self, view_num: ViewNum) -> bool {
        view_num % self.num_replica as ViewNum == self.id as ViewNum
    }
}

impl ReplicaCore {
    // actions
    fn submit(&mut self, request: Request, context: &mut ReplicaCoreContext) {
        if !self.config.is_primary_of(self.view_num) {
            //
            return;
        }

        self.submitted_requests.push(request);
        if self.can_propose(self.view_num) {
            self.propose(context)
        }
    }

    // event handlers
    fn handle_pre_prepare(&mut self, pre_prepare: PrePrepare, context: &mut ReplicaCoreContext) {
        let op_num = pre_prepare.block.op_num;
        let replaced = self.pre_prepares.insert(op_num, pre_prepare);
        assert!(replaced.is_none());
        self.prepared_quorums.insert(op_num, Default::default());
        if !self.config.is_primary_of(self.view_num) {
            context.prepare(op_num)
        }
    }

    fn handle_prepare(&mut self, vote: Vote, context: &mut ReplicaCoreContext) {
        let op_num = vote.op_num;
        let quorum = self.prepared_quorums.get_mut(&op_num).unwrap();
        quorum.insert(vote.replica_id, vote);
        if self.can_commit(op_num) {
            self.committed_quorums.insert(op_num, Default::default());
            context.commit(op_num)
        }
    }

    fn handle_commit(&mut self, vote: Vote, context: &mut ReplicaCoreContext) {
        let op_num = vote.op_num;
        let quorum = self.committed_quorums.get_mut(&op_num).unwrap();
        quorum.insert(vote.replica_id, vote);
        while self.can_finalize(self.committed_op_num + 1) {
            self.committed_op_num += 1;
            context.finalize(self.committed_op_num)
        }
    }

    // internal helpers
    fn can_propose(&self, view_num: ViewNum) -> bool {
        self.config.is_primary_of(view_num)
            && self.prepared_op_num <= self.committed_op_num + self.config.num_inflight_block
    }

    fn can_commit(&self, op_num: OpNum) -> bool {
        if let Some(quorum) = self.prepared_quorums.get(&op_num) {
            quorum.len() as ReplicaId >= self.config.num_replica - self.config.num_faulty_replica
        } else {
            false
        }
    }

    fn can_finalize(&self, op_num: OpNum) -> bool {
        self.can_commit(op_num)
            && if let Some(quorum) = self.committed_quorums.get(&op_num) {
                quorum.len() as ReplicaId
                    >= self.config.num_replica - self.config.num_faulty_replica
            } else {
                false
            }
    }

    fn propose(&mut self, context: &mut ReplicaCoreContext) {
        self.prepared_op_num += 1;
        let requests = self
            .submitted_requests
            .drain(
                ..self
                    .submitted_requests
                    .len()
                    .min(self.config.max_block_size),
            )
            .collect();
        let block = Block {
            op_num: self.prepared_op_num,
            requests,
            view_num: self.view_num,
        };
        context.propose(self.prepared_op_num, block)
    }
}

impl Replica {
    pub fn submit(&mut self, request: Request, context: &mut impl Context) {}
}
