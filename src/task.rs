use tokio::sync::mpsc::{Receiver, Sender, channel};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct SegmentedTaskHandle {
    cancel: CancellationToken,
    _stopped_tx: Sender<()>,
}

pub struct SegmentedTask {
    cancel: CancellationToken,
    stopped_tx: Sender<()>,
    stopped_rx: Receiver<()>,
}

impl SegmentedTaskHandle {
    pub async fn wrap(self, segment: impl Future<Output = anyhow::Result<()>>) {
        if let Some(Err(err)) = self.cancel.run_until_cancelled(segment).await {
            tracing::error!("{err}\n{}", err.backtrace());
            self.cancel.cancel()
        }
    }

    pub fn cancel(&self) {
        self.cancel.cancel()
    }
}

impl SegmentedTask {
    pub fn new() -> Self {
        let cancel = CancellationToken::new();
        let (stopped_tx, stopped_rx) = channel(1);
        Self {
            cancel,
            stopped_tx,
            stopped_rx,
        }
    }

    pub fn handle(&self) -> SegmentedTaskHandle {
        SegmentedTaskHandle {
            cancel: self.cancel.clone(),
            _stopped_tx: self.stopped_tx.clone(),
        }
    }

    pub async fn stopped(mut self) {
        // assert!(self.cancel.is_cancelled());
        self.cancel.cancelled().await;
        drop(self.stopped_tx);
        let _ = self.stopped_rx.recv().await;
    }
}
