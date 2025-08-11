use bincode::{Decode, Encode};

use crate::{Never, replication::ReplicationState, state::State};

pub mod big;
pub mod transport;
pub mod unsharded;

pub type ServiceIndex = u16;

pub trait ServiceState<A: ServiceApp, R: ReplicationState<Self::Log>>:
    State<
        Send = ServiceSend<Self::ServiceMessage, A::Res, R::Metadata, R::Send>,
        Output = Never,
        Message = ServiceMessage<Self::ServiceMessage, A::Op, R::Message>,
    >
{
    type Log;
    type ServiceMessage;
}

pub trait ServiceApp {
    type Op;
    type Res;
}

pub enum ServiceSend<M, Res, RD = (), RS = Never> {
    Service(ServiceRecipient, M),
    Reply(ClientId, Reply<Res, RD>),
    Replication(RS),
}

pub enum ServiceRecipient {
    All, // broad?
    Multi(Vec<ServiceIndex>),
    Uni(ServiceIndex),
}

pub enum ServiceMessage<M, Op, RM = Never> {
    Service(M),
    Request(Request<Op>),
    Replication(RM),
}

// id is randomly assigned while index is continuously assigned
// index is statically assigned while sequence monotonically increases

pub type ClientId = u32;
pub type ClientSeq = u64;

#[derive(Debug, Clone, Encode, Decode)]
pub struct Request<Op> {
    pub client_id: ClientId,
    pub client_seq: ClientSeq,
    pub op: Op,
}

#[derive(Debug, Clone, Encode, Decode)]
pub struct Reply<Res, RD> {
    pub client_seq: ClientSeq,
    pub res: Res,
    pub replication_metadata: RD,
}
