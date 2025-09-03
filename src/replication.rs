use crate::state::State;

pub mod replay;
pub mod replay2;
pub mod transport;
pub mod unreplicated;
pub mod unreplicated2;

pub type ReplicaIndex = crate::service::ServiceIndex;

pub struct Replicated<L, D> {
    pub logs: Vec<L>,
    pub metadata: D, // D for data
}

pub trait ReplicationState<L>: State<Output = Replicated<L, Self::Metadata>> {
    type Metadata;

    fn submit(&mut self, log: L);
}

pub enum Dest {
    All,
    One(ReplicaIndex),
}
