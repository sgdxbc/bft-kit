use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

use crate::{
    service::ClientSeq,
    state::{Proceed, State},
};

pub trait ClientState<Op>: State {
    fn submit(&mut self, op: Op) -> ClientSeq;
}

pub trait Workload {
    type Op;
    type Res;

    fn next_op(&mut self) -> Option<Self::Op>;
    fn completed(&self) -> bool;

    fn validate(&self, op: Self::Op, res: Self::Res) -> anyhow::Result<()>;
}

pub struct CloseLoopWorker<W: Workload, C> {
    workload: W,
    client: C,
    submitted: Option<(Instant, W::Op)>,
    latencies: Histogram<u64>,
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

impl<C: ClientState<W::Op, Output = (ClientSeq, W::Res)>, W: Workload> State
    for CloseLoopWorker<W, C>
where
    W::Op: Clone,
{
    type Send = C::Send;
    type Output = Histogram<u64>;

    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        if self.submitted.is_none() {
            let Some(op) = self.workload.next_op() else {
                return Proceed::Output(self.latencies.clone());
            };
            self.client.submit(op.clone());
            self.submitted = Some((Instant::now(), op))
        }
        match self.client.proceed() {
            Proceed::Pending(tick_at) => Proceed::Pending(tick_at),
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((_, res)) => {
                let Some((start, op)) = self.submitted.take() else {
                    unimplemented!("multiple outputs to close loop worker")
                };
                if let Err(err) = self.workload.validate(op, res) {
                    tracing::error!(%err, "validation failure");
                    return Proceed::Output(self.latencies.clone());
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
    latencies: Histogram<u64>,
    next_submit: Instant,
    target_tput: f32,
}

impl<W: Workload, C> OpenLoopWorker<W, C> {
    pub fn new(workload: W, client: C, target_tput: f32) -> Self {
        Self {
            workload,
            client,
            submitted: HashMap::new(),
            latencies: Histogram::new(3).unwrap(),
            next_submit: Instant::now(),
            target_tput,
        }
    }
}

impl<W: Workload, C: ClientState<W::Op, Output = (ClientSeq, W::Res)>> State
    for OpenLoopWorker<W, C>
where
    W::Op: Clone,
{
    type Send = C::Send;
    type Output = Histogram<u64>;

    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        if self.submitted.is_empty() && self.workload.completed() {
            return Proceed::Output(self.latencies.clone());
        }
        let until_next_submit = self.next_submit.saturating_duration_since(Instant::now());
        match self.client.proceed() {
            Proceed::Pending(None) => Proceed::Pending(Some(until_next_submit)),
            Proceed::Pending(Some(tick_at)) => {
                Proceed::Pending(Some(tick_at.min(until_next_submit)))
            }
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((seq, res)) => {
                let Some((start, op)) = self.submitted.remove(&seq) else {
                    unimplemented!("output for unknown seq {seq}")
                };
                if let Err(err) = self.workload.validate(op, res) {
                    tracing::error!(%err, "validation failure");
                    return Proceed::Output(self.latencies.clone());
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

    fn tick(&mut self, _elapsed: Duration) {
        let now = Instant::now();
        while self.next_submit <= now {
            if let Some(op) = self.workload.next_op() {
                let seq = self.client.submit(op.clone());
                self.submitted.insert(seq, (now, op));
            }
            self.next_submit += Duration::from_secs_f32(1. / self.target_tput)
        }
    }
}

pub struct TimeLimited<W> {
    workload: W,
    start: Instant,
    duration: Duration,
}

impl<W: Workload> Workload for TimeLimited<W> {
    type Op = W::Op;
    type Res = W::Res;

    fn next_op(&mut self) -> Option<Self::Op> {
        if self.start.elapsed() < self.duration {
            self.workload.next_op()
        } else {
            None
        }
    }

    fn completed(&self) -> bool {
        self.start.elapsed() >= self.duration
    }

    fn validate(&self, op: Self::Op, res: Self::Res) -> anyhow::Result<()> {
        self.workload.validate(op, res)
    }
}
