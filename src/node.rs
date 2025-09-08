use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use rand::rngs::StdRng;
use rocksdb::DB;
use tokio::{sync::mpsc::channel, task::JoinHandle};

use crate::{
    app::AppRunner,
    kv::{Kv, ycsb::Ycsb},
    network::Network,
    replica::{ReplicaIndex, replay::Replay},
    storage::{NodeIndex, ShardedStorageConfig, Storage},
    task::TaskGroup,
};

pub struct ReplayNode;

impl ReplayNode {
    pub fn spawn(
        group: TaskGroup,
        db: impl Into<Arc<DB>>,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        node_table: Vec<ReplicaIndex>,
        storage_config: ShardedStorageConfig,
        storage_node_indices: HashSet<NodeIndex>,
        rng: StdRng,
    ) -> Vec<JoinHandle<()>> {
        let (tx_workload, rx_workload) = channel(1);
        let (tx_request, rx_request) = channel(1);
        let (tx_op, rx_op) = channel(1);
        let (tx_state_op, rx_state_op) = channel(1);
        let (tx_storage_op, rx_storage_op) = channel(1);
        let (tx_incoming_messages, rx_incoming_messages) = channel(1);
        let (tx_outgoing_messages, rx_outgoing_messages) = channel(1);

        let workload = Ycsb::spawn(group.clone(), rng, tx_workload);
        let replay = Replay::<Kv>::spawn(group.clone(), rx_workload, tx_request);
        let app_runner =
            AppRunner::<Kv>::spawn(group.clone(), rx_request, tx_op, rx_state_op, tx_storage_op);
        let app = Kv::spawn(group.clone(), rx_op, tx_state_op);
        let mut handles = vec![workload, replay, app_runner, app];
        let storage_handles = Storage::spawn(
            group.clone(),
            db,
            storage_config,
            storage_node_indices,
            node_table,
            rx_storage_op,
            tx_outgoing_messages,
            rx_incoming_messages,
        );
        handles.extend(storage_handles);
        let network = Network::spawn_replica(
            group,
            tx_incoming_messages,
            rx_outgoing_messages,
            replica_addrs,
            replica_index,
        );
        handles.push(network);
        handles
    }
}
