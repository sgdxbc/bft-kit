use std::time::Duration;

use bft_testbed::{
    ClientId,
    pbft::{
        Client, ClientConfig, Replica, ReplicaConfig, Spec,
        net::{ClientTask, TaskConfig, server_task},
    },
};
use tokio::{task::JoinSet, time::timeout};
use tracing::{Instrument, field};
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let task_config = TaskConfig {
        // these two values unused. this example sends single request from one client
        num_client: 0,
        client_duration: Duration::ZERO,

        // effectively disable ticks
        client_tick_interval: Duration::from_secs(365 * 24 * 60 * 60),
        replica_tick_interval: Duration::from_secs(365 * 24 * 60 * 60),
        replica_external_addresses: (0..spec.num_replica)
            .map(|i| ([127, 0, 0, 1], 50000 + i as u16).into())
            .collect(),
        replica_internal_addresses: (0..spec.num_replica)
            .map(|i| ([127, 0, 0, 1], 8000 + i as u16).into())
            .collect(),
        replica_connect_delay: Duration::from_millis(100),
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
    let span = tracing::info_span!("invoke", result = field::Empty);
    let result = async {
        let config = ClientConfig {
            spec,
            id: ClientId(0),
        };
        let client = Client::new(config);
        let mut client_task = ClientTask::init(client, task_config).await?;
        anyhow::Ok(client_task.invoke(Default::default()).await?)
    }
    .instrument(span.clone())
    .await?;
    span.record("result", &*result);
    Ok(())
}
