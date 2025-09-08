use std::{collections::HashSet, sync::Arc};

use rand::rngs::StdRng;
use rocksdb::DB;
use tokio::{sync::mpsc::channel, task::JoinHandle};

use crate::{
    app::AppRunner,
    kv::{Kv, ycsb::Ycsb},
    replica::replay::Replay,
    storage::{NodeIndex, ShardedStorageConfig, Storage},
    task::TaskGroup,
};

pub struct ReplayNode;

impl ReplayNode {
    pub fn spawn(
        group: TaskGroup,
        db: impl Into<Arc<DB>>,
        storage_config: ShardedStorageConfig,
        storage_node_indices: HashSet<NodeIndex>,
        rng: StdRng,
    ) -> Vec<JoinHandle<()>> {
        let (tx_workload, rx_workload) = channel(1);
        let (tx_request, rx_request) = channel(1);
        let (tx_op, rx_op) = channel(1);
        let (tx_state_op, rx_state_op) = channel(1);
        let (tx_storage_op, rx_storage_op) = channel(1);

        let workload = Ycsb::spawn(group.clone(), rng, tx_workload);
        let replay = Replay::<Kv>::spawn(group.clone(), rx_workload, tx_request);
        let app_runner =
            AppRunner::<Kv>::spawn(group.clone(), rx_request, tx_op, rx_state_op, tx_storage_op);
        let app = Kv::spawn(group.clone(), rx_op, tx_state_op);
        let mut handles = vec![workload, replay, app_runner, app];
        let storage_handles = Storage::spawn(
            group,
            db,
            storage_config,
            storage_node_indices,
            rx_storage_op,
        );
        handles.extend(storage_handles);
        handles
    }
}
