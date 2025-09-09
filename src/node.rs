use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use rand::rngs::StdRng;
use rocksdb::DB;
use tokio::{spawn, sync::mpsc::channel, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    app::AppRunner,
    kv::{
        Kv,
        ycsb::{Ycsb, YcsbConfig},
    },
    network::Network,
    replica::{ReplicaIndex, replay::Replay},
    storage::{NodeIndex, ShardedStorageConfig, Storage, full::FullStorage},
    task::SegmentedTaskHandle,
};

pub struct ReplayFullNode;

impl ReplayFullNode {
    pub fn spawn(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>> + Send + 'static,
        ycsb_config: YcsbConfig,
        rng: StdRng,
    ) -> JoinHandle<()> {
        spawn(
            task_handle
                .clone()
                .wrap(Self::start(task_handle, db, ycsb_config, rng)),
        )
    }

    async fn start(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>> + Send + 'static,
        ycsb_config: YcsbConfig,
        rng: StdRng,
    ) -> anyhow::Result<()> {
        let (tx_workload, rx_workload) = channel(1);
        let (tx_request, rx_request) = channel(1);
        let (tx_op, rx_op) = channel(1);
        let (tx_state_op, rx_state_op) = channel(1);
        let (tx_storage_op, rx_storage_op) = channel(1);

        let workload = Ycsb::spawn(task_handle.clone(), ycsb_config, rng, tx_workload);
        let replay = Replay::<Kv>::spawn(task_handle.clone(), rx_workload, tx_request);
        let app_runner = AppRunner::<Kv>::spawn(
            task_handle.clone(),
            rx_request,
            tx_op,
            rx_state_op,
            tx_storage_op,
        );
        let app = Kv::spawn(task_handle.clone(), rx_op, tx_state_op);

        let storage = FullStorage::spawn(task_handle, db, rx_storage_op);

        workload.await?;
        replay.await?;
        app_runner.await?;
        app.await?;
        storage.await?;
        Ok(())
    }
}

pub struct ReplayNode;

impl ReplayNode {
    pub fn spawn(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>> + Send + 'static,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        ycsb_config: YcsbConfig,
        node_table: Vec<ReplicaIndex>,
        storage_config: ShardedStorageConfig,
        storage_node_indices: HashSet<NodeIndex>,
        rng: StdRng,
    ) -> JoinHandle<()> {
        spawn(task_handle.clone().wrap(Self::start(
            task_handle,
            db,
            replica_addrs,
            replica_index,
            ycsb_config,
            node_table,
            storage_config,
            storage_node_indices,
            rng,
        )))
    }

    async fn start(
        task_handle: SegmentedTaskHandle,
        db: impl Into<Arc<DB>> + Send + 'static,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        ycsb_config: YcsbConfig,
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
            task_handle.clone(),
            tx_incoming_messages,
            rx_outgoing_messages,
            replica_addrs,
            replica_index,
            connected.clone(),
        );
        connected.cancelled().await;
        tracing::info!("network connected");

        let workload = Ycsb::spawn(task_handle.clone(), ycsb_config, rng, tx_workload);
        let replay = Replay::<Kv>::spawn(task_handle.clone(), rx_workload, tx_request);
        let app_runner = AppRunner::<Kv>::spawn(
            task_handle.clone(),
            rx_request,
            tx_op,
            rx_state_op,
            tx_storage_op,
        );
        let app = Kv::spawn(task_handle.clone(), rx_op, tx_state_op);

        let storage = Storage::spawn(
            task_handle,
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
