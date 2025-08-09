use crate::state::State;

pub mod in_memory;
pub mod transport;
pub mod unreplicated;

pub type ReplicaIndex = u16;

pub struct Replicated<L, D> {
    pub logs: Vec<L>,
    pub metadata: D, // D for data
}

pub trait ReplicationState<L>: State<Output = Replicated<L, Self::Metadata>> {
    type Metadata;

    fn submit(&mut self, log: L);
}

pub enum ReplicationRecipient {
    All,
    Index(ReplicaIndex),
}
