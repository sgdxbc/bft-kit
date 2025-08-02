use std::{collections::VecDeque, time::Duration};

use bincode::{Decode, Encode};

pub trait State {
    type Send;
    type Output;
    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output>;

    type Message;
    fn receive(&mut self, msg: Self::Message);
}

#[derive(Debug, Encode, Decode)]
pub enum Never {}

pub enum Proceed<S, O = Never> {
    Pending(Option<Duration>),
    Send(S),
    Output(O),
}

pub trait AppState {
    type Op;
    type Res;
    fn update(&mut self, op: Self::Op) -> Self::Res;
}

pub struct AdaptedApp<A: AppState> {
    app: A,
    results: VecDeque<A::Res>,
}

impl<A: AppState> From<A> for AdaptedApp<A> {
    fn from(app: A) -> Self {
        Self {
            app,
            results: Default::default(),
        }
    }
}

impl<A: AppState> State for AdaptedApp<A> {
    type Send = Never;
    type Output = A::Res;
    fn proceed(&mut self, _since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        match self.results.pop_front() {
            Some(res) => Proceed::Output(res),
            None => Proceed::Pending(None),
        }
    }

    type Message = A::Op;
    fn receive(&mut self, msg: Self::Message) {
        let res = self.app.update(msg);
        self.results.push_back(res);
    }
}
