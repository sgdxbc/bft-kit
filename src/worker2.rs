use std::{pin::pin, time::Duration};

use tokio::{
    select,
    sync::{mpsc::Sender, oneshot},
    task::JoinSet,
    time::{Instant, sleep},
};
use tokio_util::sync::CancellationToken;

use crate::workload::{NanoLatencies, WorkloadState};

pub async fn close_loop<W: WorkloadState + Into<NanoLatencies>>(
    mut workload: W,
    invoke_sender: Sender<(W::Op, oneshot::Sender<W::Res>)>,
    cancel: CancellationToken,
) -> anyhow::Result<NanoLatencies> {
    let fut = async {
        while let Some((op, metadata)) = workload.next_op() {
            let (res_sender, res_receiver) = oneshot::channel();
            if invoke_sender.send((op, res_sender)).await.is_err() {
                anyhow::bail!("invoke channel closed")
            }
            let res = res_receiver.await?;
            workload.complete(metadata, res)?;
        }
        Ok(())
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
    let mut res_waits = JoinSet::new();
    let mut all_invoked = false;
    while !all_invoked || !res_waits.is_empty() {
        enum Event<R> {
            Sleep,
            Waited(R),
            Cancel,
        }
        match select! {
            () = &mut sleep, if !all_invoked => Event::Sleep,
            Some(waited) = res_waits.join_next() => Event::Waited(waited??),
            () = cancel.cancelled() => Event::Cancel,
        } {
            Event::Sleep => {
                let Some((op, metadata)) = workload.next_op() else {
                    all_invoked = true;
                    continue;
                };
                let (res_sender, res_receiver) = oneshot::channel();
                if invoke_sender.send((op, res_sender)).await.is_err() {
                    anyhow::bail!("invoke channel closed")
                }
                res_waits.spawn(async move {
                    let res = res_receiver.await?;
                    anyhow::Ok((metadata, res))
                });
                sleep
                    .as_mut()
                    .reset(Instant::now() + Duration::from_secs_f32(1.0 / target_tput))
            }
            Event::Waited((metadata, res)) => workload.complete(metadata, res)?,
            Event::Cancel => break,
        }
    }
    Ok(workload.into())
}
