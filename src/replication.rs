use crate::state::State;

pub mod in_memory;
pub mod transport;
pub mod unreplicated;

pub type ReplicaIndex = u16;

pub struct Replicated<T, M> {
    pub block: Vec<T>,
    pub metadata: M,
}

pub trait ReplicationState<T>: State<Output = Replicated<T, Self::Metadata>> {
    type Metadata;

    fn submit(&mut self, entry: T);
}

pub enum ReplicationSend<M> {
    All(M),
    Index(ReplicaIndex, M),
}
