use std::time::Duration;

use bincode::{Decode, Encode};

pub trait State {
    type Send;
    type Output;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output>;

    type Message;
    fn receive(&mut self, message: Self::Message);
}

#[derive(Debug, Encode, Decode)]
pub enum Never {}

pub enum Proceed<S, O = Never> {
    Pending(Option<Duration>),
    Send(S),
    Output(O),
}
