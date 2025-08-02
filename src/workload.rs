use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

use crate::{
    service::ClientSeq,
    state::{Proceed, State},
};

// pub mod transport;

pub trait ClientState<Op>: State {
    fn submit(&mut self, op: Op) -> ClientSeq;
}

pub trait Workload {
    type Op;
    type Res;

    fn next_op(&mut self) -> Option<Self::Op>;

    fn validate(&self, op: Self::Op, res: Self::Res) -> anyhow::Result<()>;
}

type Latencies = Histogram<u64>;

pub struct CloseLoopWorker<W: Workload, C> {
    workload: W,
    client: C,
    submitted: Option<(Instant, W::Op)>,
    latencies: Latencies,
}

impl<W: Workload, C> CloseLoopWorker<W, C> {
    pub fn new(workload: W, client: C) -> Self {
        Self {
            workload,
            client,
            submitted: None,
            latencies: Histogram::new(3).unwrap(),
        }
    }
}

impl<W: Workload, C> Into<Latencies> for CloseLoopWorker<W, C> {
    fn into(self) -> Latencies {
        self.latencies
    }
}

impl<C: ClientState<W::Op, Output = (ClientSeq, W::Res)>, W: Workload> State
    for CloseLoopWorker<W, C>
where
    W::Op: Clone,
{
    type Send = C::Send;
    type Output = anyhow::Result<()>;

    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        if self.submitted.is_none() {
            let Some(op) = self.workload.next_op() else {
                return Proceed::Output(Ok(()));
            };
            self.client.submit(op.clone());
            self.submitted = Some((Instant::now(), op))
        }
        match self.client.proceed() {
            Proceed::Pending(tick_after) => Proceed::Pending(tick_after),
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((_, res)) => {
                let Some((start, op)) = self.submitted.take() else {
                    unimplemented!("multiple outputs to close loop worker")
                };
                if let Err(err) = self.workload.validate(op, res) {
                    return Proceed::Output(Err(err));
                }
                self.latencies += start.elapsed().as_micros() as u64;
                self.proceed()
            }
        }
    }

    type Message = C::Message;
    fn receive(&mut self, msg: Self::Message) {
        self.client.receive(msg)
    }

    fn tick(&mut self, elapsed: Duration) {
        self.client.tick(elapsed)
    }
}

pub struct OpenLoopWorker<W: Workload, C> {
    workload: W,
    client: C,
    submitted: HashMap<ClientSeq, (Instant, W::Op)>,
    latencies: Latencies,
    next_submit: Option<Instant>,
    target_tput: f32,
}

impl<W: Workload, C> OpenLoopWorker<W, C> {
    pub fn new(workload: W, client: C, target_tput: f32) -> Self {
        Self {
            workload,
            client,
            submitted: Default::default(),
            latencies: Histogram::new(3).unwrap(),
            next_submit: Some(Instant::now()),
            target_tput,
        }
    }
}

impl<W: Workload, C> Into<Latencies> for OpenLoopWorker<W, C> {
    fn into(self) -> Latencies {
        self.latencies
    }
}

impl<W: Workload, C: ClientState<W::Op, Output = (ClientSeq, W::Res)>> State
    for OpenLoopWorker<W, C>
where
    W::Op: Clone,
{
    type Send = C::Send;
    type Output = anyhow::Result<()>;

    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        if let Some(next_submit) = &mut self.next_submit {
            let now = Instant::now();
            while *next_submit <= now {
                let Some(op) = self.workload.next_op() else {
                    self.next_submit = None;
                    break;
                };
                let seq = self.client.submit(op.clone());
                self.submitted.insert(seq, (now, op));
                // randomize interval?
                *next_submit += Duration::from_secs_f32(1. / self.target_tput)
            }
        }
        match self.client.proceed() {
            // probably could be written as some combinator over `Option`s but that would be
            // too hard to understand
            Proceed::Pending(tick_after) => match (
                tick_after,
                self.next_submit
                    .map(|at| at.saturating_duration_since(Instant::now())),
            ) {
                (None, None) => Proceed::Output(Ok(())),
                (Some(tick_after), None) | (None, Some(tick_after)) => {
                    Proceed::Pending(Some(tick_after))
                }
                (Some(tick_after), Some(submit_at)) => {
                    Proceed::Pending(Some(tick_after.min(submit_at)))
                }
            },
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((seq, res)) => {
                let Some((start, op)) = self.submitted.remove(&seq) else {
                    unimplemented!("output for unknown seq {seq}")
                };
                if let Err(err) = self.workload.validate(op, res) {
                    return Proceed::Output(Err(err));
                }
                self.latencies += start.elapsed().as_micros() as u64;
                self.proceed()
            }
        }
    }

    type Message = C::Message;
    fn receive(&mut self, msg: Self::Message) {
        self.client.receive(msg)
    }

    // the following `proceed` call will do the work
    fn tick(&mut self, _elapsed: Duration) {}
}
