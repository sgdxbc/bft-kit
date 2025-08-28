use quinn::{Connection, ConnectionError};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

pub const BINCODE_CONFIG: bincode::config::Configuration = bincode::config::standard();

pub async fn read_loop<E: Send + Sync + 'static>(
    connection: Connection,
    event_sender: mpsc::Sender<E>,
    into_event: impl Fn(Vec<u8>) -> E,
    close_event: impl Into<Option<E>>,
) -> anyhow::Result<()> {
    loop {
        let mut stream = match connection.accept_uni().await {
            Ok(stream) => stream,
            Err(ConnectionError::LocallyClosed | ConnectionError::ApplicationClosed(_)) => break,
            Err(err) => Err(err)?,
        };
        // spawn per message read task if that matters a lot to performance
        let message = stream.read_to_end(1 << 20).await?;
        if event_sender.capacity() == 0 {
            tracing::warn!("message channel congested")
        }
        event_sender
            .send(into_event(message))
            .await
            .map_err(|_| anyhow::format_err!("message channel closed, stopping"))?
    }
    if let Some(close_event) = close_event.into() {
        event_sender.send(close_event).await?
    } else {
        tracing::debug!("connection closed without close event")
    }
    Ok(())
}

pub async fn run_write(connection: Connection, message: impl AsRef<[u8]>) -> anyhow::Result<()> {
    connection
        .open_uni()
        .await?
        .write_all(message.as_ref())
        .await?;
    Ok(())
}

pub trait PerformSend<S> {
    fn perform(&self, send: S, send_tracker: &TaskTracker) -> anyhow::Result<()>;
}

pub async fn trace_error(label: &str, task: impl Future<Output = anyhow::Result<()>>) {
    if let Err(err) = task.await {
        tracing::error!(%label, %err)
    }
}
