use std::time::Duration;

use bft_testbed::pbft::{
    Client, ClientConfig, Replica, ReplicaConfig, Spec,
    net::{ClientTask, TaskConfig, server_task},
};
use tokio::{task::JoinSet, time::timeout};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let task_config = TaskConfig {
        replica_external_addresses: (0..spec.num_replica)
            .map(|i| ([127, 0, 0, 1], 50000 + i as u16).into())
            .collect(),
        replica_internal_addresses: (0..spec.num_replica)
            .map(|i| ([127, 0, 0, 1], 8000 + i as u16).into())
            .collect(),
        tick_interval: Duration::from_secs(365 * 24 * 60 * 60), // effectively disable ticks
    };
    let mut server_tasks = JoinSet::new();
    for i in 0..spec.num_replica {
        let config = ReplicaConfig {
            spec: spec.clone(),
            id: i,
            max_num_inflight: 1,
            max_batch_size: 1,
        };
        let replica = Replica::new(config);
        server_tasks.spawn(server_task(replica, task_config.clone()));
    }
    tracing::info!("wait servers up");
    match timeout(Duration::from_secs(1), server_tasks.join_next()).await {
        Ok(Some(result)) => {
            result??;
            unreachable!()
        }
        Ok(None) => unreachable!(),
        Err(_) => {}
    }
    tracing::info!("invoke");
    let config = ClientConfig { spec, id: 0 };
    let client = Client::new(config);
    let mut client_task = ClientTask::init(client, task_config).await?;
    let result = client_task.invoke(Default::default()).await?;
    tracing::info!(?result);
    Ok(())
}
