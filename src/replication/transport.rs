use bincode::Encode;
use quinn::Connection;
use tokio_util::{bytes::Bytes, task::TaskTracker};

use crate::{
    Never,
    replication::ReplicaIndex,
    transport::{BINCODE_CONFIG, PerformSend, run_write, trace_error},
};

use super::ReplicationRecipient;

pub trait ReplicaTable {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection>;

    fn get_all(&self) -> impl Iterator<Item = &Connection>;
}

impl<T: ReplicaTable + ?Sized> PerformSend<Never> for T {
    fn perform(&self, _send: Never, _send_tracker: &TaskTracker) -> anyhow::Result<()> {
        unreachable!()
    }
}

// disabled due to conflict
// impl<T: ReplicaTable, M: Encode> PerformSend<M> for T {
//     fn perform(&self, message: M, send_tracker: &TaskTracker) -> anyhow::Result<()> {
//         self.perform((ReplicationRecipient::All, message), send_tracker)
//     }
// }

// we can also have a similar impl for `(index, message)` (which does not
// conflict hopefully), but cannot think of any protocol that only unicast
// without broadcast

impl<T: ReplicaTable + ?Sized, M: Encode> PerformSend<(ReplicationRecipient, M)> for T {
    fn perform(
        &self,
        (recipient, message): (ReplicationRecipient, M),
        send_tracker: &TaskTracker,
    ) -> anyhow::Result<()> {
        let bytes = Bytes::from(bincode::encode_to_vec(message, BINCODE_CONFIG)?);
        match recipient {
            ReplicationRecipient::Index(index) => {
                let Some(connection) = self.get(index) else {
                    anyhow::bail!("unknown replica index {index}");
                };
                send_tracker.spawn(trace_error(
                    "replica connection write",
                    run_write(connection.clone(), bytes),
                ));
            }
            ReplicationRecipient::All => {
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
