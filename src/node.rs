use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use rand::rngs::StdRng;
use rocksdb::DB;
use tokio::{spawn, sync::mpsc::channel, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    app::AppRunner,
    kv::{Kv, ycsb::Ycsb},
    network::Network,
    replica::{ReplicaIndex, replay::Replay},
    storage::{NodeIndex, ShardedStorageConfig, Storage},
    task::SegmentedTask,
};

pub struct ReplayNode;

impl ReplayNode {
    pub fn spawn(
        task: SegmentedTask,
        db: impl Into<Arc<DB>> + Send + 'static,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        node_table: Vec<ReplicaIndex>,
        storage_config: ShardedStorageConfig,
        storage_node_indices: HashSet<NodeIndex>,
        rng: StdRng,
    ) -> JoinHandle<()> {
        spawn(task.clone().wrap_fallible(Self::start(
            task,
            db,
            replica_addrs,
            replica_index,
            node_table,
            storage_config,
            storage_node_indices,
            rng,
        )))
    }

    async fn start(
        task: SegmentedTask,
        db: impl Into<Arc<DB>> + Send + 'static,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        node_table: Vec<ReplicaIndex>,
        storage_config: ShardedStorageConfig,
        storage_node_indices: HashSet<NodeIndex>,
        rng: StdRng,
    ) -> anyhow::Result<()> {
        let (tx_workload, rx_workload) = channel(1);
        let (tx_request, rx_request) = channel(1);
        let (tx_op, rx_op) = channel(1);
        let (tx_state_op, rx_state_op) = channel(1);
        let (tx_storage_op, rx_storage_op) = channel(1);
        let (tx_incoming_messages, rx_incoming_messages) = channel(1);
        let (tx_outgoing_messages, rx_outgoing_messages) = channel(1);

        let connected = CancellationToken::new();
        let network = Network::spawn_replica(
            task.clone(),
            tx_incoming_messages,
            rx_outgoing_messages,
            replica_addrs,
            replica_index,
            connected.clone(),
        );

        connected.cancelled().await;

        let workload = Ycsb::spawn(task.clone(), rng, tx_workload);
        let replay = Replay::<Kv>::spawn(task.clone(), rx_workload, tx_request);
        let app_runner =
            AppRunner::<Kv>::spawn(task.clone(), rx_request, tx_op, rx_state_op, tx_storage_op);
        let app = Kv::spawn(task.clone(), rx_op, tx_state_op);

        let storage = Storage::spawn(
            task,
            db,
            storage_config,
            storage_node_indices,
            node_table,
            rx_storage_op,
            tx_outgoing_messages,
            rx_incoming_messages,
        );

        network.await?;
        workload.await?;
        replay.await?;
        app_runner.await?;
        app.await?;
        storage.await?;
        Ok(())
    }
}
