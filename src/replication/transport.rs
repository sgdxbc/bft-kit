use bincode::Encode;
use quinn::Connection;
use tokio_util::task::TaskTracker;

use crate::{
    Never,
    replication::{ReplicaIndex, ReplicationSend},
    transport::{BINCODE_CONFIG, PerformSend, run_write, trace_error},
};

pub trait ReplicaTable {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection>;

    fn get_all(&self) -> impl Iterator<Item = &Connection>;
}

impl<T: ReplicaTable + ?Sized> PerformSend<Never> for T {
    fn perform(&self, _send: Never, _send_tracker: &TaskTracker) -> anyhow::Result<()> {
        unreachable!()
    }
}

// disabled for conflicting
// impl<T: ReplicaTable, M: Encode> PerformSend<M> for T {
//     fn perform(&self, message: M, send_tracker: &TaskTracker) -> anyhow::Result<()> {
//         self.perform(ReplicationSend::All(message), send_tracker)
//     }
// }

// we can also have a similar impl for `(index, message)` (which does not
// conflict hopefully), but cannot think of any protocol that only unicast
// without broadcast

impl<T: ReplicaTable + ?Sized, M: Encode> PerformSend<ReplicationSend<M>> for T {
    fn perform(&self, send: ReplicationSend<M>, send_tracker: &TaskTracker) -> anyhow::Result<()> {
        match send {
            ReplicationSend::Index(index, message) => {
                let Some(connection) = self.get(index) else {
                    anyhow::bail!("unknown replica index {index}");
                };
                send_tracker.spawn(trace_error(
                    "replica connection write",
                    run_write(
                        connection.clone(),
                        bincode::encode_to_vec(message, BINCODE_CONFIG)?,
                    ),
                ));
            }
            ReplicationSend::All(message) => {
                let bytes = bincode::encode_to_vec(message, BINCODE_CONFIG)?;
                for connection in self.get_all() {
                    send_tracker.spawn(trace_error(
                        "replica connection write",
                        run_write(connection.clone(), bytes.clone()),
                    ));
                }
            }
        }
        Ok(())
    }
}
