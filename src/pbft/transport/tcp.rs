use std::time::Duration;

use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::sleep,
};
use tokio_util::sync::CancellationToken;

use crate::transport::tcp::{boot_client, boot_replica};

use super::{
    ClientConfig, ClientId, ConcurrentClients, Invoke, Latencies, Replica, ReplicaTask,
    ServiceConfig, ServiceKit, ServiceTask, Spec, TaskConfig,
};

pub async fn client_task(
    spec: Spec,
    config: ClientConfig,
    service_config: ServiceConfig,
    id: ClientId,
    invoke_receiver: Receiver<Invoke>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Latencies> {
    super::client_task_with_bootstrap(
        spec,
        config,
        id,
        invoke_receiver,
        commit_sender,
        |message_sender| async {
            let boot = boot_client(id, service_config, message_sender).await?;
            // when there are many clients backup replicas may send replies before accepting
            // connections from those clients, so wait a bit to request
            sleep(Duration::from_millis(100)).await;
            Ok(boot)
        },
    )
    .await
}

pub async fn clients_task(spec: Spec, config: TaskConfig) -> anyhow::Result<Vec<Latencies>> {
    let mut concurrent_clients = ConcurrentClients::new();
    for _ in 0..config.num_client {
        concurrent_clients.spawn(|id, invoke_receiver, commit_sender| {
            client_task(
                spec.clone(),
                config.client.clone(),
                config.service.clone(),
                id,
                invoke_receiver,
                commit_sender,
            )
        })
    }
    match config.client {
        ClientConfig::CloseLoop => concurrent_clients.close_loop(config.client_duration).await,
        ClientConfig::OpenLoop(client_config) => {
            concurrent_clients
                .open_loop(config.client_duration, client_config.sending_rate)
                .await
        }
    }
}

pub async fn server_task(
    replica: Replica,
    config: TaskConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (request_sender, request_receiver) = mpsc::channel(100);
    let (finalized_sender, finalized_receiver) = mpsc::channel(100);

    let replica_id = replica.core.config.id;
    let service_task = ServiceTask::<ServiceKit>::new(replica_id, request_sender).run_tcp(
        config.service,
        finalized_receiver,
        cancel.clone(),
    );
    let replica_task = ReplicaTask::new(replica, finalized_sender).run_with_bootstrap(
        config.tick_interval,
        request_receiver,
        |message_sender| boot_replica(replica_id, config.replica, message_sender),
    );
    tokio::try_join!(service_task, replica_task)?;
    Ok(())
}
