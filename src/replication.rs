use crate::{service::Request, state::State};

pub mod transport;
pub mod unreplicated;

pub type ReplicaIndex = u16;

pub struct ReplicationOutput<Op, M> {
    pub requests: Vec<Request<Op>>,
    pub metadata: M,
}

pub trait ReplicationState<Op>: State<Output = ReplicationOutput<Op, Self::Metadata>> {
    type Metadata;

    fn submit(&mut self, request: Request<Op>);
}

pub enum ReplicationRecipient {
    All,
    Index(ReplicaIndex),
}
