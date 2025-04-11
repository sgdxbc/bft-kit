use std::time::Duration;

use bft_testbed::{
    common::{
        ClientId,
        transport::{BootServerConfig, ServiceConfig},
    },
    crypto::threshold::givre_replica_key_shares,
    hotstuff::{
        CryptoConfig, Replica, ReplicaCoreConfig, Spec,
        transport::{TaskConfig, client_task, server_task},
    },
    init_logging,
};
use tokio::{sync::mpsc, task::JoinSet, time::timeout};
use tracing::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let task_config = TaskConfig {
        // these two values unused. this example sends single request from one client
        num_client: 0,
        client_duration: Duration::ZERO,

        // effectively disable ticks
        replica_tick_interval: Duration::from_secs(365 * 24 * 60 * 60),
        service: ServiceConfig {
            server_external_addresses: (0..spec.num_replica)
                .map(|i| ([127, 0, 0, 1], 50000 + i as u16).into())
                .collect(),
        },
        boot_server: BootServerConfig {
            server_internal_addresses: (0..spec.num_replica)
                .map(|i| ([127, 0, 0, 1], 8000 + i as u16).into())
                .collect(),
            server_interconnect_delay: Duration::from_millis(100),
        },
    };
    let key_shares = givre_replica_key_shares(spec.num_replica, spec.num_faulty);

    let mut server_tasks = JoinSet::new();
    for i in 0..spec.num_replica {
        let core_config = ReplicaCoreConfig {
            spec: spec.clone(),
            id: i,
            max_batch_size: 1,
        };
        let crypto_config = CryptoConfig {
            key_share: key_shares[i as usize].clone(),
            num_supply_commit: 10,
            num_refill_threshold: 10,
        };
        let replica = Replica::new(core_config, crypto_config);
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
    let (invoke_sender, invoke_receiver) = mpsc::channel(1);
    let (commit_sender, mut commit_receiver) = mpsc::channel(1);
    let client_task = client_task(
        spec,
        task_config.service,
        ClientId(0),
        invoke_receiver,
        commit_sender,
    );
    invoke_sender
        .send((Default::default(), Some(Default::default())))
        .await?;
    async {
        tokio::select! {
            commit = commit_receiver.recv() => anyhow::Ok(commit.unwrap()),
            result = client_task => {
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
    server_tasks.abort_all();
    Ok(())
}
