#![allow(unused)]
use std::{collections::HashMap, mem::replace};

use bincode::{Decode, Encode};
use slab::Slab;

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
    pub spec: Spec, // unused for now but probably useful for a (responsible) pacemaker.getLeader
    pub id: ReplicaId,
}

// the event driven algorithm does not maintain view number anywhere (explicitly
// stated in the "Data structures" paragraph), although it does not provide a
// new version of Msg(..) and QC(..) to remove the access within them
// anyway, i will first go without view number and see how it goes
// type ViewNum = u32;
type BlockHeight = u32;

type NodeKey = usize;
type QuorumCertKey = usize;

// the paper is tricky on the term block vs. node
// it has been using "node" in all the text but using `b` or `b_{something}`
// (presumably for block) to name the node variables throughout the pseudocode
// so i will just naively follow this convention
#[derive(Debug, Clone, Encode, Decode)]
pub struct Node {
    parent: NodeKey,
    requests: Vec<message::Request>, // `cmd` in paper
    justify: QuorumCertKey,
    height: BlockHeight,
}

#[derive(Debug, Clone, Encode, Decode)]
struct QuorumCert {
    // view_num: ViewNum,
    node: NodeKey,
    sig: Sig,
}

struct ReplicaCore {
    config: ReplicaCoreConfig,

    // cSpell:disable-next-line
    vote_height: BlockHeight,        // vheight
    block_lock: NodeKey,             // b_{lock}
    block_execute: NodeKey,          // b_{exec}
    block_leaf: NodeKey,             // b_{leaf}
    quorum_cert_high: QuorumCertKey, // qc_{high}

    pool: RequestPool,
    nodes: Slab<Node>,
    node_index: HashMap<Digest, NodeKey>, // node.sha256() => node
    quorum_certs: Slab<QuorumCert>,
    quorum_cert_index: HashMap<Digest, QuorumCertKey>, // quorum_cert.node.sha256() => quorum_cert
    num_extra_round: u32,
}

#[derive(Debug)]
enum ReplicaCoreEvent {
    Request(message::Request),                  // onBeat (kind of)
    Proposal(message::Generic, message::Block), // onReceiveProposal
    QuorumCert(QuorumCert),                     // onReceiveVote (bottom half)
}

enum ReplicaCoreAction {
    Propose(Node),
    Vote(NodeKey),
    Finalize(NodeKey),
}

type ReplicaCoreActions = Vec<ReplicaCoreAction>;

impl ReplicaCore {
    fn new(config: ReplicaCoreConfig) -> Self {
        let mut nodes = Slab::new();
        let genesis_key = nodes.vacant_key();
        let mut quorum_certs = Slab::new();
        let genesis_justify_key = quorum_certs.vacant_key();
        nodes.insert(Node {
            // a more canonical design may be point to "void" instead of itself
            // not bother too much for now
            parent: genesis_key,
            requests: Default::default(),
            justify: genesis_justify_key,
            height: 0,
        });
        quorum_certs.insert(QuorumCert {
            node: genesis_key,
            sig: Sig::Vec(Default::default()),
        });
        Self {
            config,
            vote_height: 0,
            block_lock: genesis_key,
            block_execute: genesis_key,
            block_leaf: genesis_key,
            quorum_cert_high: genesis_justify_key,
            pool: RequestPool::close_loop(), // TODO configurable
            nodes,
            // genesis block and its justify are not indexed, as genesis block does not have
            // a real Block representation and doesn't have a Digest (only a NodeKey)
            // it is not expected to be transferred around anyway
            node_index: Default::default(),
            quorum_certs,
            quorum_cert_index: Default::default(),
            num_extra_round: 0,
        }
    }

    // hardcoded pacemaker baked into the protocol core. should be sufficient for
    // benchmark
    fn get_leader(&self) -> ReplicaId {
        0 // TODO
    }

    fn beat(&mut self, actions: &mut ReplicaCoreActions) {
        // "Based on some application-specific heuristics (to wait until the previously
        // proposed node gets a QC, for example), the current leader invokes onBeat to
        // propose a new node carrying the command to be executed."
        // although, the paper also said "Instead of the next leader always waiting for
        // a genericQC ... stable leader can skip this step and streamline proposals
        // across multiple heights"
        // however, the finalized point can only be advanced if the leader does _not_
        // "skip this step" (for 3 times consecutively), as the decide phase condition
        // is not (and probably is impossible to be) relaxed
        // then there's no clear answer on when to skip and why it's a good choice to
        // skip at that time. i decide to simply following what libhotstuff does
        // (PMRoundRobinProposer at https://github.com/hot-stuff/libhotstuff/blob/master/include/hotstuff/liveness.h#L230)
        // and always omit "skip this step streamline proposals across multiple heights"
        if self.quorum_certs[self.quorum_cert_high].node == self.block_leaf {
            self.on_beat(actions)
        }
    }

    // in this implementation pool.close_batch(..) makes side effect, so fetch the
    // requests inside
    fn on_beat(&mut self, actions: &mut ReplicaCoreActions) {
        if self.config.id == self.get_leader() {
            // inlined onPropose
            let requests = self.pool.close_batch(1); // TODO
            // ensure liveness of close loop clients: keep proposing (even empty nodes) as
            // long as there exists (nonempty) node that is not deep enough to be committed
            // the PMRoundRobinProposer seems to have similar consideration with
            // `do_new_consensus` although the code is obscure and i'm not sure
            if requests.is_some() || self.num_extra_round > 0 {
                let requests = requests.unwrap_or_default();
                self.num_extra_round = if requests.is_empty() {
                    self.num_extra_round - 1
                } else {
                    3
                };
                // inlined createLeaf
                let block = Node {
                    parent: self.block_leaf,
                    requests,
                    justify: self.quorum_cert_high,
                    height: self.nodes[self.block_leaf].height + 1,
                };
                actions.push(ReplicaCoreAction::Propose(block))
                // update of block_leaf will happen when the corresponding Proposal event
                // arrives back and triggers update(..) and then update_quorum_cert_high(..)
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
            ReplicaCoreEvent::Proposal(generic, block) => {
                let justify = *self
                    .quorum_cert_index
                    .entry(block.justify.node.clone())
                    .or_insert_with(|| {
                        self.quorum_certs.insert(QuorumCert {
                            node: self.node_index[&block.justify.node],
                            sig: block.justify.sig,
                        })
                    });
                let block_height = block.height;
                let block = self.nodes.insert(Node {
                    parent: self.node_index[&block.parent],
                    requests: block.requests,
                    justify,
                    height: block_height,
                });
                let replaced = self.node_index.insert(generic.block, block);
                assert!(replaced.is_none());
                if block_height > self.vote_height
                    && (self.extends(block, self.block_lock)
                        || self.nodes[self.quorum_certs[justify].node].height
                            > self.nodes[self.block_lock].height)
                {
                    self.vote_height = block_height;
                    actions.push(ReplicaCoreAction::Vote(block))
                }
                self.update(block, actions)
            }
            ReplicaCoreEvent::QuorumCert(quorum_cert) => {
                let quorum_cert = self.quorum_certs.insert(quorum_cert);
                self.update_quorum_cert_high(quorum_cert, actions)
            }
        }
    }

    fn extends(&self, node_key: NodeKey, other_node_key: NodeKey) -> bool {
        if node_key == other_node_key {
            true
        } else if self.nodes[node_key].height <= self.nodes[other_node_key].height {
            false
        } else {
            self.extends(self.nodes[node_key].parent, other_node_key)
        }
    }

    fn update(&mut self, /* block* */ block0: NodeKey, actions: &mut ReplicaCoreActions) {
        let block1 = self.quorum_certs[self.nodes[block0].justify].node; // block''
        let block2 = self.quorum_certs[self.nodes[block1].justify].node; // block'
        let block3 = self.quorum_certs[self.nodes[block2].justify].node; // block
        self.update_quorum_cert_high(self.nodes[block0].justify, actions);
        if self.nodes[block2].height > self.nodes[self.block_lock].height {
            self.block_lock = block2
        }
        if self.nodes[block1].parent == block2 && self.nodes[block2].parent == block3 {
            self.on_commit(block3, actions);
            self.block_execute = block3
        }
        self.beat(actions)
    }

    // updateQCHigh
    fn update_quorum_cert_high(
        &mut self,
        quorum_cert: QuorumCertKey,
        actions: &mut ReplicaCoreActions,
    ) {
        if self.nodes[self.quorum_certs[quorum_cert].node].height
            > self.nodes[self.quorum_certs[self.quorum_cert_high].node].height
        {
            self.quorum_cert_high = quorum_cert;
            self.block_leaf = self.quorum_certs[quorum_cert].node;
            self.beat(actions)
        }
    }

    fn on_commit(&mut self, block: NodeKey, actions: &mut ReplicaCoreActions) {
        if self.nodes[self.block_execute].height < self.nodes[block].height {
            self.on_commit(self.nodes[block].parent, actions);
            actions.push(ReplicaCoreAction::Finalize(block))
        }
    }
}

pub enum ToReplica {
    Request(message::Request),
    // latency optimization: inline dissemination of block content for new blocks
    Generic(message::Generic, message::Block),
    VoteGeneric(message::VoteGeneric),
    // TODO dedicated missing block fetching
}
