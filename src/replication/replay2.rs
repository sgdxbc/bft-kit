use tokio::sync::mpsc::{Receiver, Sender};

use super::Replicated;

pub async fn replay_loop<L>(
    log_iter: impl IntoIterator<Item = L>,
    batch_size: usize,
    submit_receiver: Receiver<L>,
    replicated_sender: Sender<Replicated<L, ()>>,
) -> anyhow::Result<()> {
    drop(submit_receiver); // not support submit
    let mut log_iter = log_iter.into_iter();
    let mut logs;
    while {
        logs = log_iter.by_ref().take(batch_size).collect::<Vec<_>>();
        !logs.is_empty()
    } {
        if replicated_sender
            .send(Replicated { logs, metadata: () })
            .await
            .is_err()
        {
            tracing::error!("replicated channel closed");
            break;
        }
    }
    Ok(())
}
