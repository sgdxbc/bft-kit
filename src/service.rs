use bincode::{Decode, Encode};

use crate::state::State;

pub mod big;
pub mod unsharded;

pub trait ServiceState<A: ServiceApp>:
    State<
        Send = Send<Reply<A::Res, Self::Metadata>, Self::ServiceSend>,
        Message = Message<Request<A::Op>, Self::ServiceMessage>,
    >
{
    type ServiceSend;
    type ServiceMessage;
    type Metadata;

    fn read_ok(&mut self, key: String, value: Vec<u8>);
    fn write_ok(&mut self, key: String);
}

pub trait ServiceApp {
    type Op;
    type Res;
}

pub enum Send<R, S> {
    Reply(ClientId, R),
    Intermediate(S),
}

pub type ServiceIndex = u16;

pub enum Dest {
    One(ServiceIndex),
    Multi(Vec<ServiceIndex>),
    All,
}

pub enum Output {
    Read(String),
    Write(String, Vec<u8>),
}

#[derive(Debug)]
pub enum Message<R, M> {
    Request(R),
    Intermediate(M),
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
