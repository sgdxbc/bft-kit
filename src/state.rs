use std::time::Duration;

use crate::Never;

pub trait State {
    fn tick(&mut self, since_start: Duration);
    fn tick_after(&self) -> Option<Duration>;

    type Send;
    type Output;
    fn proceed(&mut self) -> Option<impl Iterator<Item = Action<Self::Send, Self::Output>>>;

    type Message;
    fn receive(&mut self, message: Self::Message);
}

pub enum Action<S, O = Never> {
    Send(S),
    Output(O),
}

pub fn earliest(tick_afters: impl IntoIterator<Item = Option<Duration>>) -> Option<Duration> {
    tick_afters.into_iter().flatten().min()
}
