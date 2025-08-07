use bincode::Encode;
use quinn::Connection;
use tokio_util::task::TaskTracker;

use crate::{
    Never,
    replication::{ReplicaIndex, ReplicationRecipient},
    transport::{BINCODE_CONFIG, run_write, trace_error},
};

pub trait ReplicaTable {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection>;

    fn get_all(&self) -> impl Iterator<Item = &Connection>;
}

pub trait ReplicationSend {
    fn apply(
        self,
        replica_connections: &(impl ReplicaTable + ?Sized),
        tracker: &TaskTracker,
    ) -> anyhow::Result<()>;
}

impl ReplicationSend for Never {
    fn apply(
        self,
        _replica_connections: &(impl ReplicaTable + ?Sized),
        _tracker: &TaskTracker,
    ) -> anyhow::Result<()> {
        unreachable!()
    }
}

impl<M: Encode> ReplicationSend for (ReplicaIndex, M) {
    fn apply(
        self,
        replica_connections: &(impl ReplicaTable + ?Sized),
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

impl<M: Encode> ReplicationSend for (ReplicationRecipient, M) {
    fn apply(
        self,
        replica_connections: &(impl ReplicaTable + ?Sized),
        tracker: &TaskTracker,
    ) -> anyhow::Result<()> {
        let (recipient, message) = self;
        match recipient {
            ReplicationRecipient::Index(index) => {
                (index, message).apply(replica_connections, tracker)?
            }
            ReplicationRecipient::All => {
                let bytes = bincode::encode_to_vec(message, BINCODE_CONFIG)?;
                for connection in replica_connections.get_all() {
                    tracker.spawn(trace_error(
                        "replica connection write",
                        run_write(connection.clone(), bytes.clone()),
                    ));
                }
            }
        }
        Ok(())
    }
}
