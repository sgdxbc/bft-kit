use std::time::Duration;

use crate::{Command, command::ClientSeq};

pub trait ReplicaProtocol<C> {
    type Message;

    fn init(&mut self, context: &mut C);
    fn submit(&mut self, command: Command, context: &mut C);
    fn receive(&mut self, message: Self::Message, context: &mut C);
    fn tick(&mut self, duration: Duration, context: &mut C);

    type FinalizeMetadata;
    fn finalize_metadata(&self) -> Self::FinalizeMetadata;
}

pub trait ReplyProtocol {
    type FinalizeMetadata;
    type Reply;

    fn new_reply(
        seq: ClientSeq,
        result: Vec<u8>,
        finalize_metadata: &Self::FinalizeMetadata,
    ) -> Self::Reply;
}
