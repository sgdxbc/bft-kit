use std::{pin::pin, time::Duration};

use bft_testbed::pbft::{
    Replica, ReplicaConfig, Spec,
    net::{TaskConfig, server_task},
};
use tokio::{signal::ctrl_c, time::sleep};
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
        replica_external_addresses: (0..spec.num_replica)
            .map(|i| ([127, 0, 0, 1], 50000 + i as u16).into())
            .collect(),
        replica_internal_addresses: (0..spec.num_replica)
            .map(|i| ([127, 0, 0, 1], 8000 + i as u16).into())
            .collect(),
        tick_interval: Duration::from_secs(365 * 24 * 60 * 60), // effectively disable ticks
        replica_connect_delay: Duration::from_millis(100),
    };
    let config = ReplicaConfig {
        spec,
        id: 0,
        max_num_inflight: 1,
        max_batch_size: 1,
    };
    let replica = Replica::new(config);
    let mut server = pin!(server_task(replica, task_config));
    'server: {
        tokio::select! {
            result = &mut server => result?,
            result = ctrl_c() => break 'server result?,
        }
        unreachable!()
    }
    tracing::info!("exit");
    // before dropping `server` (and breaking every established connections), wait a
    // while until every server stop polling (hopefully)
    sleep(Duration::from_secs(1)).await;
    Ok(())
}
