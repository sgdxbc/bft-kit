use std::time::Duration;

use crate::Never;

pub trait State {
    type Effect;
    type Output;
    fn proceed(&mut self, since_start: Duration) -> Action<Self::Effect, Self::Output>;

    type Message;
    fn receive(&mut self, message: Self::Message);
}

pub enum Action<E, O = Never> {
    Pending(Option<Duration>),
    Perform(E),
    Output(O),
}

pub fn earliest(tick_afters: impl IntoIterator<Item = Option<Duration>>) -> Option<Duration> {
    tick_afters.into_iter().flatten().min()
}
