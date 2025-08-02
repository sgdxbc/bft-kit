use bincode::Encode;
use quinn::{Connection, ConnectionError};
use tokio::sync::mpsc;
use tokio_util::task::TaskTracker;

use crate::{service::ReplicaIndex, state::Never};

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
        let message = stream.read_to_end(1 << 16).await?;
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

pub async fn run_write(connection: Connection, message: Vec<u8>) -> anyhow::Result<()> {
    connection.open_uni().await?.write_all(&message).await?;
    Ok(())
}

pub async fn trace_error<T>(label: &str, task: impl Future<Output = anyhow::Result<T>>) {
    if let Err(err) = task.await {
        tracing::error!(%label, %err)
    }
}

pub trait ReplicaConnections {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection>;
}

impl ReplicaConnections for [Connection] {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection> {
        self.get(index as usize)
    }
}

pub trait ReplicationSend {
    fn apply(
        self,
        replica_connections: &(impl ReplicaConnections + ?Sized),
        tracker: &TaskTracker,
    ) -> anyhow::Result<()>;
}

impl ReplicationSend for Never {
    fn apply(
        self,
        _replica_connections: &(impl ReplicaConnections + ?Sized),
        _tracker: &TaskTracker,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}

impl<M: Encode> ReplicationSend for (ReplicaIndex, M) {
    fn apply(
        self,
        replica_connections: &(impl ReplicaConnections + ?Sized),
        tracker: &TaskTracker,
    ) -> anyhow::Result<()> {
        let (index, message) = self;
        let Some(connection) = replica_connections.get(index) else {
            anyhow::bail!("unknown replica index {index}");
        };
        tracker.spawn(trace_error(
            "replica connection write",
            run_write(
                connection.clone(),
                bincode::encode_to_vec(message, BINCODE_CONFIG)?,
            ),
        ));
        Ok(())
    }
}
