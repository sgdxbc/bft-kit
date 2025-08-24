use bincode::{Decode, Encode};
use tokio_util::bytes::Bytes;

use crate::{app::AppProtocol, state::State};

pub mod big;
pub mod unsharded;

pub trait ServiceState<A: AppProtocol>:
    State<
        Send = Send<Reply<A::Res, Self::Metadata>, Self::ServiceSend>,
        Message = Message<Request<A::Op>, Self::ServiceMessage>,
    >
{
    type ServiceSend;
    type ServiceMessage;
    type Metadata;

    fn read_ok(&mut self, key: String, value: Bytes);
    fn write_ok(&mut self, key: String);
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

#[derive(Debug)]
pub enum Output {
    Read(String),
    Write(String, Bytes),
}

#[derive(Debug)]
pub enum Message<R, M> {
    Request(R),
    Intermediate(M),
}

// id/index has no special meaning if id1 > id2, while seq1 > seq2 means seq1
// comes later
// index is statically assigned while id is dynamic

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
