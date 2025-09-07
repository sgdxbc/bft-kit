use rand::rngs::StdRng;
use tokio::{sync::mpsc::channel, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    app::AppRunner,
    kv::{Kv, ycsb::Ycsb},
    replica::replay::Replay,
    storage::Storage,
};

pub struct ReplayNode {
    //
}

impl ReplayNode {
    pub fn spawn(cancel: CancellationToken, rng: StdRng) -> Vec<JoinHandle<()>> {
        let (tx_workload, rx_workload) = channel(100);
        let (tx_request, rx_request) = channel(100);
        let (tx_op, rx_op) = channel(100);
        let (tx_state_op, rx_state_op) = channel(100);
        let (tx_storage_op, rx_storage_op) = channel(100);

        let workload = Ycsb::spawn(cancel.clone(), rng, tx_workload);
        let replay = Replay::<Kv>::spawn(cancel.clone(), rx_workload, tx_request);
        let app_runner = AppRunner::<Kv>::spawn(
            cancel.clone(),
            rx_request,
            tx_op,
            rx_state_op,
            tx_storage_op,
        );
        let app = Kv::spawn(cancel.clone(), rx_op, tx_state_op);
        let storage = Storage::spawn(cancel, rx_storage_op);

        vec![workload, replay, app_runner, app, storage]
    }
}
