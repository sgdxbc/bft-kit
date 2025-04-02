use std::{env::args, time::Duration};

use bft_testbed::{
    common::ClientId,
    init_logging,
    pbft::{
        Client, ClientConfig, Replica, ReplicaConfig, Spec,
        transport::{ClientTask, Server, TaskConfig, server_task, tcp},
    },
};
use futures::FutureExt;
use tokio::{task::JoinSet, time::timeout};
use tracing::{Instrument, field};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let use_tcp = args().nth(1).as_deref() == Some("tcp");
    if use_tcp {
        tracing::info!("use tcp")
    }

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
        let config = ReplicaConfig::new_basic(spec.clone(), i);
        let replica = Replica::new(config);
        server_tasks.spawn(if !use_tcp {
            server_task::<Server>(replica, task_config.clone()).left_future()
        } else {
            server_task::<tcp::Server>(replica, task_config.clone()).right_future()
        });
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
    let config = ClientConfig {
        spec,
        id: ClientId(0),
    };
    let client = Client::new(config);
    let span = tracing::info_span!("invoke", result = field::Empty);
    let invoke_task = if !use_tcp {
        async {
            let mut client_task = ClientTask::init(client, task_config).await?;
            tracing::info!("client initialized");
            anyhow::Ok(client_task.invoke(Default::default()).await?)
        }
        .left_future()
    } else {
        async {
            let mut client_task = tcp::ClientTask::init(client, task_config).await?;
            tracing::info!("client initialized");
            anyhow::Ok(client_task.invoke(Default::default()).await?)
        }
        .right_future()
    }
    .instrument(span.clone());
    let result = tokio::select! {
        result = invoke_task => result?,
        Some(result) = server_tasks.join_next() => {
            result??;
            unreachable!()
        }
    };
    span.record("result", &*result);
    server_tasks.abort_all();
    Ok(())
}
