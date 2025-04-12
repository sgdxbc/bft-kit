use std::{
    collections::{BTreeMap, HashMap},
    iter::repeat_with,
    mem::take,
    ops::Deref,
};

use bincode::{Decode, Encode};
use slab::Slab;

use crate::{
    common::{CommandPool, ReplicaId},
    crypto::{
        Digest, Sha256Hash,
        threshold::{
            AggregateContext, GivreAggregateContext, GivreCiphersuite, GivreKeyShare,
            GivrePublicCommitments, GivreSecretNonces, GivreSigShare, PartialSig, PartialSigs,
            PublicMasterKey, Sig, verify,
        },
    },
};

mod message;
pub mod transport;

pub use crate::common::Command;
pub use message::Reply as ToClient;

#[derive(Debug, Clone)]
pub struct Spec {
    pub num_faulty: ReplicaId,
    pub num_replica: ReplicaId,
}

#[derive(Debug)]
pub struct ReplicaCoreConfig {
    pub spec: Spec, // unused for now but probably useful for a (responsible) pacemaker.getLeader
    pub id: ReplicaId,

    pub max_batch_size: usize,
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
#[derive(Debug, Clone)]
pub struct Node {
    parent: NodeKey,
    commands: Vec<Command>, // `cmd`
    justify: QuorumCertKey,
    height: BlockHeight,
}

#[derive(Debug, Clone)]
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

    pool: CommandPool,
    nodes: Slab<StoredNode>,
    node_index: HashMap<Digest, NodeKey>, // node.sha256() => node
    quorum_certs: Slab<QuorumCert>,
    quorum_cert_index: HashMap<Digest, QuorumCertKey>, // quorum_cert.node.sha256() => quorum_cert
    num_extra_round: u32,
}

#[derive(Debug)]
struct StoredNode(Digest, Node); // lack of a better name

impl StoredNode {
    fn digest(&self) -> &Digest {
        &self.0
    }
}

impl Deref for StoredNode {
    type Target = Node;

    fn deref(&self) -> &Self::Target {
        &self.1
    }
}

#[derive(Debug)]
enum ReplicaCoreEvent {
    Request(Command),                 // onBeat (kind of)
    Proposal(Digest, message::Block), // onReceiveProposal
    QuorumCert(message::QuorumCert),  // onReceiveVote (bottom half)
}

#[derive(Debug)]
enum ReplicaCoreAction {
    Propose(NodeKey),
    Vote(NodeKey),
    Finalize(NodeKey),
}

type ReplicaCoreActions = Vec<ReplicaCoreAction>;

impl ReplicaCore {
    fn new(config: ReplicaCoreConfig) -> Self {
        let genesis_digest = Digest(Default::default());
        let mut nodes = Slab::new();
        let genesis_key = nodes.vacant_key();
        let mut quorum_certs = Slab::new();
        let genesis_justify_key = quorum_certs.vacant_key();
        nodes.insert(StoredNode(
            genesis_digest.clone(),
            Node {
                // a more canonical design may be point to "void" instead of itself
                // not bother too much for now
                parent: genesis_key,
                commands: Default::default(),
                justify: genesis_justify_key,
                height: 0,
            },
        ));
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
            pool: CommandPool::close_loop(), // TODO configurable
            nodes,
            node_index: [(genesis_digest.clone(), genesis_key)].into(),
            quorum_certs,
            quorum_cert_index: [(genesis_digest, genesis_justify_key)].into(),
            num_extra_round: 0,
        }
    }

    // hardcoded pacemaker baked into the protocol core. should be sufficient for
    // benchmark
    fn get_leader(&self) -> ReplicaId {
        0 // TODO
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
    // commands inside (inlined) onPropose
    fn on_beat(&mut self, actions: &mut ReplicaCoreActions) {
        if self.config.id == self.get_leader() {
            // inlined onPropose
            let commands = self.pool.close_batch(self.config.max_batch_size);
            // ensure liveness of close loop clients: keep proposing (even empty nodes) as
            // long as there exists (nonempty) node that is not deep enough to be committed
            // the PMRoundRobinProposer seems to have similar consideration with
            // `do_new_consensus` although the code is obscure and i'm not sure
            if commands.is_some() || self.num_extra_round > 0 {
                let commands = commands.unwrap_or_default();
                self.num_extra_round = if commands.is_empty() {
                    self.num_extra_round - 1
                } else {
                    3
                };
                // inlined createLeaf
                let block = Node {
                    parent: self.block_leaf,
                    commands,
                    justify: self.quorum_cert_high,
                    height: self.nodes[self.block_leaf].height + 1,
                };
                // calculate block hash: exact mirroring message::Block::sha256(..) but make use
                // of managed state
                // any better way?
                use crate::crypto::UpdateHash;
                use sha2::Digest as _;
                let mut state = sha2::Sha256::new();
                state.update(self.nodes[block.parent].digest());
                (&*block.commands).update(&mut state);
                state.update(self.nodes[self.quorum_certs[block.justify].node].digest());
                state.update(block.height.to_le_bytes());
                let block_digest = Digest::from(state.finalize());
                self.block_leaf = self
                    .nodes
                    .insert(StoredNode(block_digest.clone(), block.clone()));
                self.node_index.insert(block_digest, self.block_leaf);
                actions.push(ReplicaCoreAction::Propose(self.block_leaf))
            }
        }
    }

    // TODO primary change stuff

    fn handle(&mut self, event: ReplicaCoreEvent, actions: &mut ReplicaCoreActions) {
        tracing::trace!(?event);
        match event {
            ReplicaCoreEvent::Request(command) => {
                self.pool.push(command);
                self.beat(actions)
            }
            ReplicaCoreEvent::Proposal(block_digest, block) => {
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
                let block = if let Some(&block) = self.node_index.get(&block_digest) {
                    // `Replica` only deliver Proposal that presents in self.nodes if it is just
                    // `Propose`d by the core itself
                    assert!(block == self.block_leaf);
                    block
                } else {
                    let block = self.nodes.insert(StoredNode(
                        block_digest.clone(),
                        Node {
                            parent: self.node_index[&block.parent],
                            commands: block.commands,
                            justify,
                            height: block_height,
                        },
                    ));
                    self.node_index.insert(block_digest, block);
                    block
                };
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
                let quorum_cert = self.quorum_certs.insert(QuorumCert {
                    node: self.node_index[&quorum_cert.node],
                    sig: quorum_cert.sig,
                });
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

    fn on_commit(&mut self, block: NodeKey, actions: &mut ReplicaCoreActions) {
        if self.nodes[self.block_execute].height < self.nodes[block].height {
            self.on_commit(self.nodes[block].parent, actions);
            for command in &self.nodes[block].commands {
                self.pool.commit(command)
            }
            actions.push(ReplicaCoreAction::Finalize(block))
        }
    }
}

#[derive(Debug, Clone, Encode, Decode)]
pub enum ToReplica {
    // latency optimization: inline dissemination of block content for new blocks
    Generic(message::Generic, message::Block),
    VoteGeneric(message::VoteGeneric),
    // TODO targeted fetch for missing block

    // background periodical message required by givre
    PublicCommitmentsSupply(givre::SignerIndex, Vec<GivrePublicCommitments>),
}

pub struct CryptoConfig {
    pub key_share: GivreKeyShare,
    pub num_supply_commit: usize,
    pub num_refill_threshold: usize,
}

pub struct Replica {
    core: ReplicaCore,
    core_actions: ReplicaCoreActions,

    crypto_config: CryptoConfig,

    // block.sha256() of b is pending for => b
    reordering_blocks: HashMap<Digest, Vec<(Digest, message::Block)>>,
    nonces: HashMap<GivrePublicCommitments, GivreSecretNonces>,
    block_signers: HashMap<Digest, Vec<(givre::SignerIndex, GivrePublicCommitments)>>,

    // primary exclusive
    quorum_cert_scratches: HashMap<Digest, QuorumCertScratch>,
    // require ordered keys to ensure consistently reach out the first 2f + 1
    // replicas for signing. round robin signing could have better performance, but
    // that would be _really_ strong assumption on fast path i.e. all replicas are
    // not byzantine
    public_commitments_pool: BTreeMap<givre::SignerIndex, Vec<GivrePublicCommitments>>,
}

struct QuorumCertScratch {
    partial_sigs: PartialSigs,
    signers: Vec<(givre::SignerIndex, GivrePublicCommitments)>,
}

pub type ReplicaAction = crate::common::ReplicaAction<ToReplica>;
pub type ReplicaActions = Vec<ReplicaAction>;

impl Replica {
    pub fn new(core_config: ReplicaCoreConfig, crypto_config: CryptoConfig) -> Self {
        Self {
            core: ReplicaCore::new(core_config),
            core_actions: Default::default(),
            crypto_config,
            reordering_blocks: Default::default(),
            nonces: Default::default(),
            block_signers: Default::default(),
            quorum_cert_scratches: Default::default(),
            public_commitments_pool: Default::default(),
        }
    }

    fn signer_index(&self) -> givre::SignerIndex {
        self.core.config.id as _
    }

    fn is_primary(&self) -> bool {
        self.core.get_leader() == self.core.config.id
    }

    pub fn init(&mut self, actions: &mut ReplicaActions) {
        self.refill_nonces(actions)
    }

    fn refill_nonces(&mut self, actions: &mut ReplicaActions) {
        let supply = repeat_with(|| {
            let (secret_nones, public_commitments) =
                givre::signing::round1::commit::<GivreCiphersuite>(
                    &mut rand08::thread_rng(),
                    &self.crypto_config.key_share,
                );
            (GivrePublicCommitments(public_commitments), secret_nones)
        })
        .take(self.crypto_config.num_supply_commit)
        .collect::<HashMap<_, _>>();
        let public_commitments = supply.keys().copied().collect();
        if self.is_primary() {
            self.public_commitments_pool
                .insert(self.signer_index(), public_commitments);
        } else {
            actions.push(ReplicaAction::SendToReplica(
                self.core.get_leader(),
                ToReplica::PublicCommitmentsSupply(self.signer_index(), public_commitments),
            ))
        }
        self.nonces.extend(supply)
    }

    pub fn request(&mut self, command: Command, actions: &mut ReplicaActions) {
        self.core
            .handle(ReplicaCoreEvent::Request(command), &mut self.core_actions);
        self.effect_core_actions(actions)
    }

    pub fn receive(&mut self, message: ToReplica, actions: &mut ReplicaActions) {
        match message {
            ToReplica::Generic(generic, block) => {
                if self.core.node_index.contains_key(&generic.block) {
                    return;
                }
                if Digest::from(block.sha256()) != generic.block {
                    return;
                }
                if !self
                    .core
                    .quorum_cert_index
                    .contains_key(&block.justify.node)
                {
                    if let Err(err) = verify(
                        block.justify.node.clone(),
                        &block.justify.sig,
                        &PublicMasterKey::givre(&self.crypto_config.key_share),
                    ) {
                        tracing::warn!(%err, "malformed QuorumCert in Generic");
                        return;
                    }
                }

                let replaced = self
                    .block_signers
                    .insert(generic.block.clone(), generic.signers);
                assert!(replaced.is_none());
                if !self.core.node_index.contains_key(&block.parent) {
                    self.reordering_blocks
                        .entry(block.parent.clone())
                        .or_default()
                        .push((generic.block, block));
                    return;
                }
                let mut pending = vec![(generic.block, block)];
                while let Some((block_digest, block)) = pending.pop() {
                    if !self.core.node_index.contains_key(&block.justify.node) {
                        self.reordering_blocks
                            .entry(block.justify.node.clone())
                            .or_default()
                            .push((block_digest, block));
                        continue;
                    }
                    if let Some(other_pending) = self.reordering_blocks.remove(&block_digest) {
                        pending.extend(other_pending);
                    }
                    self.core.handle(
                        ReplicaCoreEvent::Proposal(block_digest, block),
                        &mut self.core_actions,
                    )
                }
            }
            ToReplica::VoteGeneric(vote_generic) => self.handle_vote_generic(vote_generic),
            ToReplica::PublicCommitmentsSupply(index, supply) => self
                .public_commitments_pool
                .entry(index)
                .or_default()
                .extend(supply),
        }

        self.effect_core_actions(actions)
    }

    fn handle_vote_generic(&mut self, vote_generic: message::VoteGeneric) {
        let scratch = self
            .quorum_cert_scratches
            .get_mut(&vote_generic.node)
            .unwrap();
        let sig = match scratch.partial_sigs.add_partial(
            vote_generic.signer_index,
            vote_generic.partial_sig,
            AggregateContext::Givre(GivreAggregateContext {
                key_share: &self.crypto_config.key_share,
                signers: &scratch.signers,
                message: vote_generic.node.as_ref(),
            }),
        ) {
            Ok(None) => return,
            Ok(Some(sig)) => sig,
            Err(err) => {
                tracing::warn!(%err, "fail to aggregate signature");
                todo!("fallback to slower signature scheme")
            }
        };
        self.quorum_cert_scratches.remove(&vote_generic.node);
        let quorum_cert = message::QuorumCert {
            node: vote_generic.node,
            sig,
        };
        if !self.core.node_index.contains_key(&quorum_cert.node) {
            // probably never happen without primary change
            todo!("fetch missing justified node")
        }
        self.core.handle(
            ReplicaCoreEvent::QuorumCert(quorum_cert),
            &mut self.core_actions,
        )
    }

    fn effect_core_actions(&mut self, actions: &mut ReplicaActions) {
        while !self.core_actions.is_empty() {
            for action in take(&mut self.core_actions) {
                tracing::trace!(?action);
                match action {
                    ReplicaCoreAction::Propose(block) => {
                        let StoredNode(block_digest, block) = &self.core.nodes[block];
                        let justify = message::QuorumCert {
                            node: self.core.nodes[self.core.quorum_certs[block.justify].node]
                                .digest()
                                .clone(),
                            sig: self.core.quorum_certs[block.justify].sig.clone(),
                        };
                        let block = message::Block {
                            parent: self.core.nodes[block.parent].digest().clone(),
                            commands: block.commands.clone(),
                            justify,
                            height: block.height,
                        };
                        let num_signer = (self.core.config.spec.num_replica
                            - self.core.config.spec.num_faulty)
                            as _;
                        let signers = self
                            .public_commitments_pool
                            .iter_mut()
                            .filter_map(|(&index, supply)| {
                                let public_commitments = supply.pop()?;
                                Some((index, public_commitments))
                            })
                            .take(num_signer)
                            .collect::<Vec<_>>();
                        if signers.len() < num_signer {
                            todo!("fallback to slower signature scheme")
                        }
                        let generic = message::Generic {
                            block: block_digest.clone(),
                            signers,
                        };
                        actions.push(ReplicaAction::SendToAllReplicas(ToReplica::Generic(
                            generic.clone(),
                            block.clone(),
                        )));
                        self.quorum_cert_scratches.insert(
                            generic.block.clone(),
                            QuorumCertScratch {
                                partial_sigs: PartialSigs::Givre(Default::default()),
                                signers: generic.signers.clone(),
                            },
                        );
                        let replaced = self
                            .block_signers
                            .insert(generic.block.clone(), generic.signers);
                        assert!(replaced.is_none());
                        self.core.handle(
                            ReplicaCoreEvent::Proposal(generic.block, block),
                            &mut self.core_actions,
                        )
                    }
                    ReplicaCoreAction::Vote(block) => {
                        let digest = self.core.nodes[block].digest();
                        let signers = self.block_signers.remove(digest).unwrap();
                        let Some((_, public_commitments)) = signers
                            .iter()
                            .find(|&&(index, _)| index == self.signer_index())
                        else {
                            continue;
                        };
                        let nonce = self.nonces.remove(public_commitments).unwrap();
                        let signers = signers
                            .into_iter()
                            .map(|(index, GivrePublicCommitments(public_commitments))| {
                                (index, public_commitments)
                            })
                            .collect::<Vec<_>>();
                        let partial_sig = match givre::signing::round2::sign::<GivreCiphersuite>(
                            &self.crypto_config.key_share,
                            nonce,
                            digest.as_ref(),
                            &signers,
                        ) {
                            Ok(sig_share) => PartialSig::Givre(GivreSigShare(sig_share)),
                            Err(err) => {
                                tracing::warn!(%err, "failed to sign signature share");
                                continue;
                            }
                        };
                        let vote_generic = message::VoteGeneric {
                            node: digest.clone(),
                            partial_sig,
                            signer_index: self.signer_index(),
                        };
                        if self.is_primary() {
                            self.handle_vote_generic(vote_generic)
                        } else {
                            actions.push(ReplicaAction::SendToReplica(
                                self.core.get_leader(),
                                ToReplica::VoteGeneric(vote_generic),
                            ))
                        }
                        if self.nonces.len() < self.crypto_config.num_refill_threshold {
                            self.refill_nonces(actions)
                        }
                    }
                    ReplicaCoreAction::Finalize(block) => actions.push(ReplicaAction::Finalize(
                        self.core.nodes[block].commands.clone(),
                    )),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests;
