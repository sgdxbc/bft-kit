use std::time::Duration;

use crate::Never;

pub trait State {
    type Send;
    type Output;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output>;

    type Message;
    fn receive(&mut self, message: Self::Message);
}

pub enum Proceed<S, O = Never> {
    Pending(Option<Duration>),
    Send(S),
    Output(O),
}

pub fn earliest(tick_afters: impl IntoIterator<Item = Option<Duration>>) -> Option<Duration> {
    tick_afters.into_iter().flatten().min()
}
