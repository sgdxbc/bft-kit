use std::{convert::identity, time::Duration};

use bft_kit::{
    app::null::Null,
    crypto::cert::quinn::{client_config, server_config},
    init_logging,
    parse::Configs,
    replication::unreplicated2::{self, unreplicated_loop},
    service::unsharded2::unsharded_loop,
    worker2::close_loop,
    workload::{OpLatency, Take},
};
use quinn::Endpoint;
use tokio::{spawn, sync::mpsc::channel};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let mut configs = Configs::new();
    configs.parse("client.timeout 1.");
    configs.parse("unreplicated.batch-size 1");

    let service_addr = ([127, 0, 0, 1], 5000).into();
    let service_endpoint = Endpoint::server(server_config(), service_addr)?;
    let (submit_sender, submit_receiver) = channel(100);
    let (replicated_sender, replicated_receiver) = channel(100);
    let service = spawn(unsharded_loop(
        service_endpoint.clone(),
        Null,
        submit_sender,
        replicated_receiver,
    ));
    let replication = spawn(unreplicated_loop(1, submit_receiver, replicated_sender));

    let mut client_endpoint = Endpoint::client(([127, 0, 0, 1], 0).into())?;
    client_endpoint.set_default_client_config(client_config());
    let connection = client_endpoint
        .connect(service_addr, "server.example")?
        .await?;
    connection
        .open_uni()
        .await?
        .write_all(&0u32.to_le_bytes())
        .await?;
    let (invoke_sender, invoke_receiver) = channel(100);
    let client = spawn(unreplicated2::client_loop::<Null>(
        0,
        Duration::from_secs_f32(configs.get("client.timeout")?),
        connection,
        invoke_receiver,
    ));
    let workload = OpLatency::new(Take::new(Null, 100));
    let worker = close_loop(workload, invoke_sender, CancellationToken::new());
    match worker.await {
        Ok(latencies) => {
            println!("latency: {:?}", Duration::from_nanos(latencies.min()))
        }
        Err(err) => {
            tracing::error!("worker error: {err}")
        }
    }
    if let Err(err) = client.await.map_err(Into::into).and_then(identity) {
        tracing::error!("client error: {err}")
    }
    service_endpoint.close(0u32.into(), b"service shutdown");
    if let Err(err) = service.await.map_err(Into::into).and_then(identity) {
        tracing::error!("service error: {err}")
    }
    if let Err(err) = replication.await.map_err(Into::into).and_then(identity) {
        tracing::error!("replication error: {err}")
    }
    Ok(())
}
