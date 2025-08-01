use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

use crate::state::{Proceed, State};

pub trait ClientState<Op>: State {
    fn submit(&mut self, op: Op);
}

pub trait Workload {
    type Op;
    type Res;

    fn next_op(&mut self) -> Option<Self::Op>;

    fn validate(&self, op: Self::Op, res: Self::Res) -> anyhow::Result<()>;
}

pub struct CloseLoopWorker<C, W> {
    workload: W,
    client: C,
    submit_start: Option<Instant>,
    latencies: Histogram<u64>,
}

impl<C, W> CloseLoopWorker<C, W> {
    pub fn new(client: C, workload: W) -> Self {
        Self {
            workload,
            client,
            submit_start: None,
            latencies: Histogram::new(3).unwrap(),
        }
    }
}

impl<C: ClientState<W::Op, Output = (W::Op, W::Res)>, W: Workload> State for CloseLoopWorker<C, W> {
    type Send = C::Send;
    type Output = Histogram<u64>;

    fn proceed(&mut self) -> Proceed<Self::Send, Self::Output> {
        if self.submit_start.is_none() {
            let Some(op) = self.workload.next_op() else {
                return Proceed::Output(self.latencies.clone());
            };
            self.client.submit(op);
            self.submit_start = Some(Instant::now())
        }
        match self.client.proceed() {
            Proceed::Pending => Proceed::Pending,
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((op, res)) => {
                if let Err(err) = self.workload.validate(op, res) {
                    tracing::error!(%err, "validation failure");
                    return Proceed::Output(self.latencies.clone());
                }
                self.latencies += self.submit_start.take().unwrap().elapsed().as_micros() as u64;
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

    fn validate(&self, op: Self::Op, res: Self::Res) -> anyhow::Result<()> {
        self.workload.validate(op, res)
    }
}
