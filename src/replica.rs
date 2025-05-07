use crate::command::Command;

// theoretically there could be more than 2^16 replicas in the system, but (the
// common stuff of) this codebase is written without keeping such case in mind
// roll out protocol specific replica id in such case
// it's also probably not going to be more than 2^8 replicas, but be prepared
// for flat id address in potential sharding protocols, also u16 is easier for
// replica id <-> port mapping
pub type Id = u16;

#[derive(Debug)]
pub enum Action<M> {
    SendToReplica(Id, M),
    SendToAllReplicas(M), // except loopback
    // the commands are ready to be executed by the replicated service, i.e.,
    // reaching the "commit point"
    // ...but the term _commit_ has been overloaded by protocols such as PBFT as
    // "start the commit phase"
    // following the permissionless convention and saying the commands "reach the
    // finality"
    Finalize(Vec<Command>),
}

pub trait AbstractReplica {
    type Action;
    type Message;

    fn init(&mut self, actions: &mut Vec<Self::Action>);
    fn request(&mut self, command: Command, actions: &mut Vec<Self::Action>);
    fn receive(&mut self, message: Self::Message, actions: &mut Vec<Self::Action>);
    fn tick(&mut self, actions: &mut Vec<Self::Action>);
}

pub type Quorum<T> = std::collections::HashMap<Id, T>;
