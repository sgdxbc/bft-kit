use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;

use crate::{
    app::AppProtocol,
    service::ClientSeq,
    state::{Action, State, earliest},
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

pub struct CloseLoopWorker<W: WorkloadState, C: ClientState<W>> {
    workload: W,
    client: C,

    submitted: Option<W::Metadata>,

    actions: Vec<Action<C::Send, anyhow::Result<()>>>,
}

impl<W: WorkloadState, C: ClientState<W>> CloseLoopWorker<W, C> {
    pub fn new(workload: W, client: C) -> Self {
        Self {
            workload,
            client,
            submitted: None,
            actions: Default::default(),
        }
    }
}

impl<C: ClientState<W>, W: WorkloadState> State for CloseLoopWorker<W, C> {
    type Send = C::Send;
    type Output = anyhow::Result<()>;

    fn proceed(&mut self) -> Option<impl Iterator<Item = Action<Self::Send, Self::Output>>> {
        if let Some(actions) = self.client.proceed() {
            for action in actions {
                match action {
                    Action::Send(send) => self.actions.push(Action::Send(send)),
                    Action::Output((_, res)) => {
                        let Some(metadata) = self.submitted.take() else {
                            unimplemented!("multiple outputs to close loop worker")
                        };
                        if let Err(err) = self.workload.complete(metadata, res) {
                            self.actions.push(Action::Output(Err(err)));
                            break;
                        }
                    }
                }
            }
        }

        if self.submitted.is_none() {
            if let Some((op, metadata)) = self.workload.next_op() {
                self.client.submit(op);
                self.submitted = Some(metadata)
            } else {
                self.actions.push(Action::Output(Ok(())))
            }
        }

        if !self.actions.is_empty() {
            Some(self.actions.drain(..))
        } else {
            None
        }
    }

    type Message = C::Message;
    fn receive(&mut self, message: Self::Message) {
        self.client.receive(message)
    }

    fn tick(&mut self, since_start: Duration) {
        self.client.tick(since_start)
    }
    fn tick_after(&self) -> Option<Duration> {
        self.client.tick_after()
    }
}

pub struct OpenLoopWorker<W: WorkloadState, C: ClientState<W>> {
    workload: W,
    client: C,

    target_tput: f32,

    submitted: HashMap<ClientSeq, W::Metadata>,
    next_submit: Option<Duration>,

    actions: Vec<Action<C::Send, anyhow::Result<()>>>,
}

impl<W: WorkloadState, C: ClientState<W>> OpenLoopWorker<W, C> {
    pub fn new(workload: W, client: C, target_tput: f32) -> Self {
        Self {
            workload,
            client,
            target_tput,
            submitted: Default::default(),
            next_submit: Some(Duration::ZERO),
            actions: Default::default(),
        }
    }
}

impl<W: WorkloadState, C: ClientState<W>> State for OpenLoopWorker<W, C> {
    fn tick(&mut self, since_start: Duration) {
        self.client.tick(since_start);
        while let Some(next_submit) = &mut self.next_submit
            && *next_submit <= since_start
        {
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

    fn tick_after(&self) -> Option<Duration> {
        earliest([self.next_submit, self.client.tick_after()])
    }

    type Send = C::Send;
    type Output = anyhow::Result<()>;

    fn proceed(&mut self) -> Option<impl Iterator<Item = Action<Self::Send, Self::Output>>> {
        if let Some(actions) = self.client.proceed() {
            for action in actions {
                match action {
                    Action::Send(send) => self.actions.push(Action::Send(send)),
                    Action::Output((seq, res)) => {
                        let Some(metadata) = self.submitted.remove(&seq) else {
                            unimplemented!("output for unknown seq {seq}")
                        };
                        if let Err(err) = self.workload.complete(metadata, res) {
                            self.actions.push(Action::Output(Err(err)));
                            break;
                        }
                    }
                }
            }
        }

        if !self.actions.is_empty() {
            Some(self.actions.drain(..))
        } else {
            None
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

impl<W: WorkloadState + Into<NanoLatencies>, C: ClientState<W>> From<CloseLoopWorker<W, C>>
    for NanoLatencies
{
    fn from(worker: CloseLoopWorker<W, C>) -> Self {
        worker.workload.into()
    }
}

impl<W: WorkloadState + Into<NanoLatencies>, C: ClientState<W>> From<OpenLoopWorker<W, C>>
    for NanoLatencies
{
    fn from(worker: OpenLoopWorker<W, C>) -> Self {
        worker.workload.into()
    }
}
