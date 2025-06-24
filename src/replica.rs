use crate::Command;

pub trait ReplicaContext<M> {
    fn send(&mut self, message: M);
    fn finalize(&mut self, commands: Vec<Command>);
}

pub trait ReplicaProtocol {
    type Message;
    type FinalizeMetadata;

    fn init(&mut self, context: &mut impl ReplicaContext<Self::Message>);
    fn submit(&mut self, command: Command, context: &mut impl ReplicaContext<Self::Message>);
    fn receive(&mut self, message: Self::Message, context: &mut impl ReplicaContext<Self::Message>);
    fn tick(&mut self, context: &mut impl ReplicaContext<Self::Message>);
    fn finalize_metadata(&self) -> Self::FinalizeMetadata;
}
