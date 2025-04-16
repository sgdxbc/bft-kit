use std::{env::args, pin::pin, time::Duration};

use bft_kit::{
    common::ClientId,
    init_logging,
    pbft::{
        Replica, ReplicaCoreConfig, Spec,
        transport::{TaskConfig, client_task, server_task, tcp},
    },
    transport::{ClientConfig, ReplicaConfig, ServiceConfig},
};
use futures::FutureExt;
use tokio::{sync::mpsc, task::JoinSet, time::timeout};
use tracing::Instrument;

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
        client: ClientConfig::CloseLoop,

        replica: ReplicaConfig {
            server_internal_addresses: (0..spec.num_replica)
                .map(|i| ([127, 0, 0, 1], 8000 + i as u16).into())
                .collect(),
            server_interconnect_delay: Duration::from_millis(100),
        },
        service: ServiceConfig {
            server_external_addresses: (0..spec.num_replica)
                .map(|i| ([127, 0, 0, 1], 50000 + i as u16).into())
                .collect(),
        },
        use_tcp,

        // these two values unused. this example sends single request from one client
        num_client: 0,
        client_duration: Duration::ZERO,

        // effectively disable ticks
        tick_interval: Duration::from_secs(365 * 24 * 60 * 60),
    };
    let mut server_tasks = JoinSet::new();
    for i in 0..spec.num_replica {
        let config = ReplicaCoreConfig::new_basic(spec.clone(), i);
        let replica = Replica::new(config);
        server_tasks.spawn(if !task_config.use_tcp {
            server_task(replica, task_config.clone()).left_future()
        } else {
            tcp::server_task(replica, task_config.clone()).right_future()
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
    let (invoke_sender, invoke_receiver) = mpsc::channel(1);
    let (commit_sender, mut commit_receiver) = mpsc::channel(1);
    let mut client_task = pin!(if !task_config.use_tcp {
        client_task(
            spec,
            task_config.client,
            task_config.service,
            ClientId(0),
            invoke_receiver,
            commit_sender,
        )
        .left_future()
    } else {
        tcp::client_task(
            spec,
            task_config.client,
            task_config.service,
            ClientId(0),
            invoke_receiver,
            commit_sender,
        )
        .right_future()
    });
    invoke_sender
        .send((Default::default(), Some(Default::default())))
        .await?;
    async {
        tokio::select! {
            commit = commit_receiver.recv() => anyhow::Ok(commit.unwrap()),
            result = &mut client_task => {
                result?;
                unreachable!()
            }
            Some(result) = server_tasks.join_next() => {
                result??;
                unreachable!()
            }
        }
    }
    .instrument(tracing::info_span!("invoke"))
    .await?;
    drop(invoke_sender);
    let latencies = client_task.await?;
    tracing::info!(latency = ?Duration::from_micros(latencies.mean() as _));
    server_tasks.abort_all();
    Ok(())
}
