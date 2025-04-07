#![allow(unused)]
use std::{collections::HashMap, mem::replace};

use bincode::{Decode, Encode};

use crate::{
    common::{ClientId, ReplicaId, RequestPool, client},
    crypto::{Digest, Sha256Hash, threshold::Sig},
};

mod message;

pub use message::Reply as ToClient;

#[derive(Debug, Clone)]
pub struct Spec {
    pub num_faulty: ReplicaId,
    pub num_replica: ReplicaId,
}

pub enum ToReplica {
    Request(message::Request),
    // latency optimization: inline dissemination of block content for new blocks
    Generic(message::Generic, Option<Block>),
    VoteGeneric(message::VoteGeneric),
    // TODO dedicated missing block fetching
}

#[derive(Debug)]
pub struct ClientConfig {
    pub spec: Spec,
    pub id: ClientId,
}

pub struct Client {
    config: ClientConfig,
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
        ClientAction::SendToAllReplicas(ToReplica::Request(request))
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
    pub spec: Spec, // unused for now but probably useful for pacemaker
    pub id: ReplicaId,
}

// the event driven algorithm does not maintain view number anywhere (explicitly
// stated in the "Data structures" paragraph), although it does not provide a
// new version of MSG(..) and QC(..) to remove the access within them
// anyway, i will first go without view number and see how it goes
// type ViewNum = u32;
type BlockHeight = u32;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Block {
    parent: Digest,
    requests: Vec<message::Request>, // `cmd` in paper
    justify: QuorumCert,
    height: BlockHeight,
}

#[derive(Debug, Clone, Encode, Decode)]
struct QuorumCert {
    // view_num: ViewNum,
    node: Digest,
    sig: Sig,
}

struct ReplicaCore {
    config: ReplicaCoreConfig,

    // cSpell:disable-next-line
    vote_height: BlockHeight, // vheight
    block_lock: Digest,       // b_{lock}
    block_execute: Digest,    // b_{exec}
    block_leaf: Digest,       // b_{leaf}
    qc_high: QuorumCert,

    pool: RequestPool,
    blocks: HashMap<Digest, Block>,
}

#[derive(Debug)]
enum ReplicaCoreEvent {
    Request(message::Request),
    Proposal(Block),
    QuorumCert(QuorumCert),
}

enum ReplicaCoreAction {
    Propose(Block),
    Vote(Digest),
    Finalize(Digest),
}

type ReplicaCoreActions = Vec<ReplicaCoreAction>;

impl ReplicaCore {
    fn genesis() -> Digest {
        Digest(Default::default())
    }

    fn genesis_justify() -> QuorumCert {
        QuorumCert {
            node: ReplicaCore::genesis(),
            sig: Sig::Vec(Default::default()),
        }
    }

    fn new(config: ReplicaCoreConfig) -> Self {
        Self {
            config,
            vote_height: 0,
            block_lock: Self::genesis(),
            block_execute: Self::genesis(),
            block_leaf: Self::genesis(),
            qc_high: Self::genesis_justify(),
            pool: RequestPool::close_loop(), // TODO configurable
            blocks: [(
                Self::genesis(),
                Block {
                    parent: Self::genesis(),
                    requests: Default::default(),
                    justify: Self::genesis_justify(),
                    height: 0,
                },
            )]
            .into(),
        }
    }

    fn get_leader(&self) -> ReplicaId {
        0 // TODO
    }

    fn beat(&mut self, actions: &mut ReplicaCoreActions) {
        if self.qc_high.node == self.block_leaf {
            self.on_beat(actions)
        }
    }

    // in this implementation pool.close_batch(..) makes side effect, so fetch the
    // requests inside
    fn on_beat(&mut self, actions: &mut ReplicaCoreActions) {
        if self.config.id == self.get_leader() {
            // inlined onPropose
            // TODO
            if let Some(requests) = self.pool.close_batch(1) {
                // inlined createLeaf
                let block = Block {
                    parent: self.block_leaf.clone(),
                    requests,
                    justify: self.qc_high.clone(),
                    height: self.blocks[&self.block_leaf].height + 1,
                };
                actions.push(ReplicaCoreAction::Propose(block))
            }
        }
    }

    fn handle(&mut self, event: ReplicaCoreEvent, actions: &mut ReplicaCoreActions) {
        tracing::trace!(?event);
        match event {
            ReplicaCoreEvent::Request(request) => {
                self.pool.push(request);
                self.beat(actions)
            }
            ReplicaCoreEvent::Proposal(block) => {
                let block_digest = Digest(block.sha256().to_vec());
                if block.height > self.vote_height
                    && (self.extends(&block_digest, &self.block_lock)
                        || self.blocks[&block.justify.node].height
                            > self.blocks[&self.block_lock].height)
                {
                    self.vote_height = block.height;
                    actions.push(ReplicaCoreAction::Vote(block_digest.clone()))
                }
                let replaced = self.blocks.insert(block_digest.clone(), block);
                assert!(replaced.is_none());
                self.update(&block_digest, actions)
            }
            ReplicaCoreEvent::QuorumCert(qc) => self.update_qc_high(qc, actions),
        }
    }

    fn extends(&self, node: &Digest, other_node: &Digest) -> bool {
        if node == other_node {
            true
        } else if self.blocks[node].height <= self.blocks[other_node].height {
            false
        } else {
            self.extends(&self.blocks[node].parent, other_node)
        }
    }

    fn update(&mut self, /* block* */ block0: &Digest, actions: &mut ReplicaCoreActions) {
        self.update_qc_high(self.blocks[block0].justify.clone(), actions);
        let block1 = &self.blocks[block0].justify.node; // block''
        let block2 = &self.blocks[block1].justify.node; // block'
        let block3 = &self.blocks[block2].justify.node;
        if self.blocks[block2].height > self.blocks[&self.block_lock].height {
            self.block_lock = block2.clone()
        }
        if &self.blocks[block1].parent == block2 && &self.blocks[block2].parent == block3 {
            let block3 = block3.clone();
            self.on_commit(&block3, actions);
            self.block_execute = block3
        }
        self.beat(actions)
    }

    fn update_qc_high(&mut self, qc: QuorumCert, actions: &mut ReplicaCoreActions) {
        if self.blocks[&qc.node].height > self.blocks[&self.qc_high.node].height {
            self.qc_high = qc.clone();
            self.block_leaf = qc.node.clone();
            self.beat(actions)
        }
    }

    fn on_commit(&mut self, block: &Digest, actions: &mut ReplicaCoreActions) {
        if self.blocks[&self.block_execute].height < self.blocks[block].height {
            self.on_commit(&self.blocks[block].parent.clone(), actions);
            actions.push(ReplicaCoreAction::Finalize(block.clone()))
        }
    }
}
