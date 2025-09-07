use rand::rngs::StdRng;
use tokio::{sync::mpsc::channel, task::JoinHandle};

use crate::{
    app::AppRunner,
    kv::{Kv, ycsb::Ycsb},
    replica::replay::Replay,
    storage::Storage,
    task::TaskGroup,
};

pub struct ReplayNode {
    //
}

impl ReplayNode {
    pub fn spawn(group: TaskGroup, rng: StdRng) -> Vec<JoinHandle<()>> {
        let (tx_workload, rx_workload) = channel(100);
        let (tx_request, rx_request) = channel(100);
        let (tx_op, rx_op) = channel(100);
        let (tx_state_op, rx_state_op) = channel(100);
        let (tx_storage_op, rx_storage_op) = channel(100);

        let workload = Ycsb::spawn(group.clone(), rng, tx_workload);
        let replay = Replay::<Kv>::spawn(group.clone(), rx_workload, tx_request);
        let app_runner =
            AppRunner::<Kv>::spawn(group.clone(), rx_request, tx_op, rx_state_op, tx_storage_op);
        let app = Kv::spawn(group.clone(), rx_op, tx_state_op);
        let storage = Storage::spawn(group, rx_storage_op);

        vec![workload, replay, app_runner, app, storage]
    }
}
