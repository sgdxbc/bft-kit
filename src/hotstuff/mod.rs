use std::{collections::HashMap, mem::replace};

use bincode::{Decode, Encode};

use crate::{
    common::{ClientId, ReplicaId, client},
    crypto::{
        Digest,
        threshold::{PublicMasterKey, PartialSecretKey, Sig},
    },
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
pub struct ReplicaConfig {
    pub spec: Spec,
    pub id: ReplicaId,

    pub secret_key: PartialSecretKey,
    pub public_master_key: PublicMasterKey,
}

// the event driven algorithm does not maintain view number anywhere (explicitly
// stated in the "Data structures" paragraph), although it does not provide a
// new version of MSG and QC to remove the access within them
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
