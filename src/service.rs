use bincode::{Decode, Encode};

use crate::{Never, state::State};

pub mod big;
pub mod unsharded;

pub type ServiceIndex = u16;

pub trait ServiceState<A: ServiceApp, D>:
    State<
        Send = Send<Self::ServiceSend, Reply<A::Res, D>>,
        Output = Never,
        Message = Message<Self::ServiceMessage, Request<A::Op>>,
    >
{
    type Log;
    type ServiceSend;
    type ServiceMessage;
}

pub trait ServiceApp {
    type Op;
    type Res;
}

pub enum Send<S, R> {
    Service(S),
    Reply(ClientId, R),
}

pub enum ServiceRecipient {
    All, // broad?
    Multi(Vec<ServiceIndex>),
    Uni(ServiceIndex),
}

pub enum Message<M, R> {
    Service(M),
    Request(R),
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
pub struct Reply<Res, D> {
    pub client_seq: ClientSeq,
    pub res: Res,
    pub metadata: D,
}
