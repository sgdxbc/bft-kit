use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

use crate::{
    app::AppProtocol,
    service::ClientSeq,
    state::{Proceed, State, earliest},
};

pub mod transport;

pub trait ClientState<A: AppProtocol>: State<Output = (ClientSeq, A::Res)> {
    fn submit(&mut self, op: A::Op) -> ClientSeq;
}

pub trait WorkloadState: AppProtocol {
    type Metadata;

    fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)>;

    #[allow(unused_variables)]
    fn complete(&mut self, metadata: Self::Metadata, res: Self::Res) -> anyhow::Result<()> {
        Ok(())
    }
}

pub type NanoLatencies = Histogram<u64>;

pub struct CloseLoopWorker<W: WorkloadState, C> {
    workload: W,
    client: C,
    submitted: Option<W::Metadata>,
}

impl<W: WorkloadState, C> CloseLoopWorker<W, C> {
    pub fn new(workload: W, client: C) -> Self {
        Self {
            workload,
            client,
            submitted: None,
        }
    }
}

impl<C: ClientState<W>, W: WorkloadState> State for CloseLoopWorker<W, C> {
    type Send = C::Send;
    type Output = anyhow::Result<()>;

    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if self.submitted.is_none() {
            let Some((op, metadata)) = self.workload.next_op() else {
                return Proceed::Output(Ok(()));
            };
            self.client.submit(op);
            self.submitted = Some(metadata)
        }
        match self.client.proceed(since_start) {
            Proceed::Pending(tick_after) => {
                if tick_after.is_none() {
                    tracing::warn!("liveness issue detected in close loop worker")
                }
                Proceed::Pending(tick_after)
            }
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((_, res)) => {
                let Some(metadata) = self.submitted.take() else {
                    unimplemented!("multiple outputs to close loop worker")
                };
                if let Err(err) = self.workload.complete(metadata, res) {
                    return Proceed::Output(Err(err));
                }
                self.proceed(since_start)
            }
        }
    }

    type Message = C::Message;
    fn receive(&mut self, message: Self::Message) {
        self.client.receive(message)
    }
}

pub struct OpenLoopWorker<W: WorkloadState, C> {
    workload: W,
    client: C,
    submitted: HashMap<ClientSeq, W::Metadata>,
    next_submit: Option<Instant>,
    target_tput: f32,
}

impl<W: WorkloadState, C> OpenLoopWorker<W, C> {
    pub fn new(workload: W, client: C, target_tput: f32) -> Self {
        Self {
            workload,
            client,
            submitted: Default::default(),
            next_submit: Some(Instant::now()),
            target_tput,
        }
    }
}

impl<W: WorkloadState, C: ClientState<W>> State for OpenLoopWorker<W, C> {
    type Send = C::Send;
    type Output = anyhow::Result<()>;

    fn proceed(&mut self, since_start: Duration) -> Proceed<Self::Send, Self::Output> {
        if let Some(next_submit) = &mut self.next_submit {
            let now = Instant::now();
            while *next_submit <= now {
                let Some((op, metadata)) = self.workload.next_op() else {
                    self.next_submit = None;
                    break;
                };
                let seq = self.client.submit(op);
                self.submitted.insert(seq, metadata);
                // randomize interval?
                *next_submit += Duration::from_secs_f32(1. / self.target_tput)
            }
        }
        // this `proceed` is only called if either `self` or `self.client` may make
        // progress at this point (i.e. `since_start`). if it's `self.client` would
        // proceed, we should call recursively `proceed` it. otherwise, `self` should
        // have proceeded above, which probably have `submit` to `self.client`, after
        // what we should `proceed` it as well
        // the only exception is when `self.workload` is running out of operations. we
        // just let `self.client` receives a false positive `proceed` call in this case
        match self.client.proceed(since_start) {
            Proceed::Pending(tick_after) => Proceed::Pending(earliest([
                tick_after,
                self.next_submit
                    .map(|at| at.saturating_duration_since(Instant::now())),
            ])),
            Proceed::Send(send) => Proceed::Send(send),
            Proceed::Output((seq, res)) => {
                let Some(metadata) = self.submitted.remove(&seq) else {
                    unimplemented!("output for unknown seq {seq}")
                };
                if let Err(err) = self.workload.complete(metadata, res) {
                    return Proceed::Output(Err(err));
                }
                self.proceed(since_start)
            }
        }
    }

    type Message = C::Message;
    fn receive(&mut self, message: Self::Message) {
        self.client.receive(message)
    }
}

pub struct Take<W> {
    workload: W,
    count: usize,
}

impl<W> Take<W> {
    pub fn new(workload: W, count: usize) -> Self {
        Self { workload, count }
    }
}

impl<W: AppProtocol> AppProtocol for Take<W> {
    type Op = W::Op;
    type Res = W::Res;
}

impl<W: WorkloadState> WorkloadState for Take<W> {
    type Metadata = W::Metadata;

    fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)> {
        if self.count == 0 {
            return None;
        }
        self.count -= 1;
        self.workload.next_op()
    }

    fn complete(&mut self, metadata: Self::Metadata, res: Self::Res) -> anyhow::Result<()> {
        self.workload.complete(metadata, res)
    }
}

pub struct OpLatency<W> {
    inner: W,
    latencies: NanoLatencies,
}

impl<W> OpLatency<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            latencies: NanoLatencies::new(3).unwrap(),
        }
    }
}

impl<W: AppProtocol> AppProtocol for OpLatency<W> {
    type Op = W::Op;
    type Res = W::Res;
}

impl<W: WorkloadState> WorkloadState for OpLatency<W> {
    type Metadata = (Instant, W::Metadata);

    fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)> {
        self.inner
            .next_op()
            .map(|(op, metadata)| (op, (Instant::now(), metadata)))
    }

    fn complete(
        &mut self,
        (start, metadata): Self::Metadata,
        res: Self::Res,
    ) -> anyhow::Result<()> {
        self.inner.complete(metadata, res)?;
        self.latencies += start.elapsed().as_nanos() as u64;
        Ok(())
    }
}

impl<W> From<OpLatency<W>> for NanoLatencies {
    fn from(worker: OpLatency<W>) -> Self {
        worker.latencies
    }
}

impl<W: WorkloadState + Into<NanoLatencies>, C> From<CloseLoopWorker<W, C>> for NanoLatencies {
    fn from(worker: CloseLoopWorker<W, C>) -> Self {
        worker.workload.into()
    }
}

impl<W: WorkloadState + Into<NanoLatencies>, C> From<OpenLoopWorker<W, C>> for NanoLatencies {
    fn from(worker: OpenLoopWorker<W, C>) -> Self {
        worker.workload.into()
    }
}
