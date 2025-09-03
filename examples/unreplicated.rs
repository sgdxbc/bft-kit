use std::time::Duration;

use bft_kit::{
    app::null::Null,
    crypto::cert::quinn::{client_config, server_config},
    init_logging,
    parse::Configs,
    replication::unreplicated2::{self, unreplicated_loop},
    service::unsharded2::unsharded_loop,
    worker2::close_loop,
    // worker2::open_loop,
    workload::{OpLatency, Take},
};
use quinn::Endpoint;
use tokio::{join, sync::mpsc::channel, time::sleep};
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
    let service = unsharded_loop(
        service_endpoint.clone(),
        Null,
        submit_sender,
        replicated_receiver,
    );
    let replication = unreplicated_loop(1, submit_receiver, replicated_sender);

    let workload = async {
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
        let client = unreplicated2::client_loop::<Null>(
            0,
            Duration::from_secs_f32(configs.get("client.timeout")?),
            connection,
            invoke_receiver,
        );
        let workload = OpLatency::new(Take::new(Null, 100));
        let worker = close_loop(workload, invoke_sender, CancellationToken::new());
        // let worker = open_loop(workload, 100.0, invoke_sender, CancellationToken::new());
        let (worker_result, client_result) = join!(worker, client);
        sleep(Duration::from_millis(10)).await;
        service_endpoint.close(0u32.into(), b"service shutdown");
        client_result?;
        worker_result
    };
    let result = join!(workload, service, replication);
    match result {
        (Ok(latencies), Ok(()), Ok(())) => {
            println!("latency: {:?}", Duration::from_nanos(latencies.mean() as _))
        }
        result => tracing::error!(?result),
    }
    Ok(())
}
