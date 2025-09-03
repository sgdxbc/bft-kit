use tokio::{
    select,
    sync::mpsc::{Receiver, Sender},
};

use super::Replicated;

pub async fn replay_loop<L>(
    log_iter: impl IntoIterator<Item = L>,
    batch_size: usize,
    mut submit_receiver: Receiver<L>,
    replicated_sender: Sender<Replicated<L, ()>>,
) -> anyhow::Result<()> {
    let mut log_iter = log_iter.into_iter();
    let replicate = async {
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
                return Ok(());
            }
        }
        anyhow::bail!("all logs have been replayed")
    };
    let receive = async {
        while let Some(_) = submit_receiver.recv().await {
            anyhow::bail!("not supported")
        }
        Ok(())
    };
    select! {
        res = replicate => res,
        res = receive => res,
    }
}
