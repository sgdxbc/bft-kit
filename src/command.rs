//! https://github.com/sgdxbc/bft-kit/discussions/3
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
};

use bincode::{Decode, Encode};

use crate::crypto::UpdateHash;

pub mod pool;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Encode, Decode)]
pub struct ClientId(pub u32);

impl Display for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Client#{:08x}", self.0)
    }
}

impl Debug for ClientId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

pub type ClientSeq = u64;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct Command {
    pub client_id: ClientId,
    pub seq: ClientSeq,
    pub op: Vec<u8>,
}

pub trait Execute {
    fn execute(&mut self, op: &[u8]) -> Vec<u8>;
}

pub struct Service<E> {
    inner: E,
    replies: HashMap<ClientId, (ClientSeq, Vec<u8>)>,
}

impl<E> Service<E> {
    pub fn new(inner: E) -> Self {
        Self {
            inner,
            replies: Default::default(),
        }
    }
}

pub enum ReceiveAction {
    Ignore,
    Submit,
    Reply(Vec<u8>),
}

impl<E: Execute> Service<E> {
    pub fn receive(&self, command: &Command) -> ReceiveAction {
        match self.replies.get(&command.client_id) {
            Some((seq, _)) if seq > &command.seq => ReceiveAction::Ignore,
            Some((seq, reply)) if seq == &command.seq => ReceiveAction::Reply(reply.clone()),
            _ => ReceiveAction::Submit,
        }
    }

    pub fn execute(
        &mut self,
        commands: &[Command],
    ) -> impl Iterator<Item = (ClientId, ClientSeq, Vec<u8>)> {
        commands.iter().filter_map(|command| {
            if matches!(self.replies.get(&command.client_id), Some((seq, _)) if seq >= &command.seq)
            {
                None
            } else {
                let reply = self.inner.execute(&command.op);
                self.replies
                    .insert(command.client_id, (command.seq, reply.clone()));
                Some((command.client_id, command.seq, reply))
            }
        })
    }
}

#[cfg(test)]
impl Command {
    pub fn new(client_id: u32, seq: ClientSeq) -> Self {
        // produce a more informative compile error hopefully
        const _: () = assert!(
            size_of::<u32>() == size_of::<ClientId>(),
            "need to update `client_id` type to match ClientId size"
        );
        Self {
            client_id: ClientId(client_id),
            seq,
            op: format!("command@{client_id:x}#{seq}").into(),
        }
    }
}

impl UpdateHash for ClientId {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        state.update(self.0.to_le_bytes());
    }
}

impl UpdateHash for Command {
    fn update<D: sha2::Digest>(&self, state: &mut D) {
        self.client_id.update(state);
        state.update(self.seq.to_le_bytes());
        state.update(&self.op)
    }
}
