use std::{env::args, pin::pin, time::Duration};

use bft_kit::{
    ClientId, init_logging,
    pbft::{
        Replica, ReplicaCoreConfig, Spec,
        transport::{ClientTask, TaskConfig, server_task, tcp},
    },
    transport::{ReplicaConfig, ServiceConfig},
    workload::{self, ClientConfig::CloseLoop, ClientTask as _},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::timeout,
};
use tokio_util::{either::Either, sync::CancellationToken};
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
        workload: workload::Config {
            client: CloseLoop,
            // these two values unused. this example sends single request from one client
            num_client: 0,
            duration: Duration::ZERO,
        },

        service: ServiceConfig {
            server_external_addresses: (0..spec.num_replica)
                .map(|i| (i, ([127, 0, 0, 1], 50000 + i).into()))
                .collect(),
        },
        replica: ReplicaConfig {
            server_internal_addresses: (0..spec.num_replica)
                .map(|i| (i, ([127, 0, 0, 1], 8000 + i).into()))
                .collect(),
            server_interconnect_delay: Duration::from_millis(100),
        },
        use_tcp,

        // effectively disable ticks
        tick_interval: Duration::from_secs(365 * 24 * 60 * 60),
    };
    let mut server_tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    for i in 0..spec.num_replica {
        let config = ReplicaCoreConfig::new_basic(spec.clone(), i);
        let replica = Replica::new(config);
        server_tasks.spawn(if !task_config.use_tcp {
            Either::Left(server_task(replica, task_config.clone(), cancel.clone()))
        } else {
            Either::Right(tcp::server_task(
                replica,
                task_config.clone(),
                cancel.clone(),
            ))
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
    let (commit_sender, commit_receiver) = oneshot::channel();
    let mut client_task = pin!(if !task_config.use_tcp {
        Either::Left(
            ClientTask {
                spec,
                service_config: task_config.service,
            }
            .run(
                ClientId(0),
                task_config.workload.client,
                invoke_receiver,
                Some(commit_sender),
            ),
        )
    } else {
        Either::Right(
            tcp::ClientTask {
                spec,
                service_config: task_config.service,
            }
            .run(
                ClientId(0),
                task_config.workload.client,
                invoke_receiver,
                Some(commit_sender),
            ),
        )
    });
    invoke_sender
        .send((Default::default(), Some(Default::default())))
        .await?;
    async {
        tokio::select! {
            commit = commit_receiver => anyhow::Ok(commit?),
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
    client_task.await?;
    cancel.cancel();
    while let Some(result) = server_tasks.join_next().await {
        result??
    }
    Ok(())
}
