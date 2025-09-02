use std::{pin::pin, time::Duration};

use tokio::{
    select,
    sync::{
        mpsc::{Sender, channel},
        oneshot,
    },
    time::{Instant, sleep},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::workload::{NanoLatencies, WorkloadState};

pub async fn close_loop<W: WorkloadState + Into<NanoLatencies>>(
    mut workload: W,
    invoke_sender: Sender<(W::Op, oneshot::Sender<W::Res>)>,
    cancel: CancellationToken,
) -> anyhow::Result<NanoLatencies> {
    let fut = async {
        while let Some((op, metadata)) = workload.next_op() {
            let (res_sender, res_receiver) = oneshot::channel();
            if invoke_sender.capacity() == 0 {
                tracing::warn!("invoke channel congested")
            }
            if invoke_sender.send((op, res_sender)).await.is_err() {
                tracing::error!("invoke channel closed");
                break;
            }
            let Ok(res) = res_receiver.await else {
                tracing::error!("response channel closed");
                break;
            };
            workload.complete(metadata, res)?
        }
        anyhow::Ok(())
    };
    if let Some(result) = cancel.run_until_cancelled(fut).await {
        result?
    }
    Ok(workload.into())
}

pub async fn open_loop<W: WorkloadState + Into<NanoLatencies>>(
    mut workload: W,
    target_tput: f32, // ops/sec
    invoke_sender: Sender<(W::Op, oneshot::Sender<W::Res>)>,
    cancel: CancellationToken,
) -> anyhow::Result<NanoLatencies>
where
    W::Res: Send + 'static,
    W::Metadata: Send + 'static,
{
    let mut sleep = pin!(sleep(Duration::ZERO));
    let res_tracker = TaskTracker::new();
    let (waited_sender, mut waited_receiver) = channel(100);
    loop {
        enum Event<R> {
            Sleep,
            Waited(R),
            Cancel,
        }
        match select! {
            () = &mut sleep, if !res_tracker.is_closed() => Event::Sleep,
            Some(waited) = waited_receiver.recv() => Event::Waited(waited),
            () = res_tracker.wait() => Event::Cancel, // better name?
            () = cancel.cancelled() => Event::Cancel,
        } {
            Event::Sleep => {
                let Some((op, metadata)) = workload.next_op() else {
                    res_tracker.close();
                    continue;
                };
                let (res_sender, res_receiver) = oneshot::channel();
                if invoke_sender.send((op, res_sender)).await.is_err() {
                    tracing::error!("invoke channel closed");
                    break;
                }
                let wait_sender = waited_sender.clone();
                res_tracker.spawn(async move {
                    let Ok(res) = res_receiver.await else {
                        tracing::error!("result channel closed");
                        return;
                    };
                    if wait_sender.send((metadata, res)).await.is_err() {
                        tracing::error!("wait channel closed")
                    }
                });
                sleep
                    .as_mut()
                    .reset(Instant::now() + Duration::from_secs_f32(1.0 / target_tput));
                if sleep.is_elapsed() {
                    tracing::warn!("cannot keep up with target throughput")
                }
            }
            Event::Waited((metadata, res)) => workload.complete(metadata, res)?,
            Event::Cancel => break,
        }
    }
    Ok(workload.into())
}
