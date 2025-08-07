use crate::{
    service::Request,
    state::{Never, State},
};

pub mod in_memory;
pub mod transport;
pub mod unreplicated;

pub type ReplicaIndex = u16;

pub enum Replicated<Op, M, E = Never> {
    Block(Vec<Request<Op>>, M),
    Event(E),
}

pub trait ReplicationState<Op, E = Never>:
    State<Output = Replicated<Op, Self::Metadata, E>>
{
    type Metadata;

    // naively, `submit` can be implemented using `trigger` with a "submit event"
    // this (redundant) interface gives replication protocol an opportunity to
    // optimize performance by batching requests and output them altogether
    // the practice is so common that i don't even think anyone treat this interface
    // as redundant
    fn submit(&mut self, request: Request<Op>);

    fn trigger(&mut self, event: E);
}

pub enum ReplicationRecipient {
    All,
    Index(ReplicaIndex),
}
