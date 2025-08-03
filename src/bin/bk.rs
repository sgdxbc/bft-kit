use std::{env::args, time::Duration};

use bft_kit::{
    app::null::Null, init_logging, parse::Settings, service::Service, unreplicated,
    workload::CloseLoopWorker,
};
use rand::random;
use tokio::{signal::ctrl_c, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    match args().nth(1).as_deref() {
        Some("workload") => run_worker().await,
        Some("service") => run_service().await,
        _ => anyhow::bail!("Usage: bk [workload|service]"),
    }
}

async fn run_worker() -> anyhow::Result<()> {
    let mut settings = Settings::new();
    settings.parse("addr 10.0.0.7:5000");
    settings.parse("client.timeout 1");
    settings.parse("worker.duration 10");

    let client_id = random();
    let client = unreplicated::Client::<Null>::new(client_id, settings.extract()?);
    let worker = CloseLoopWorker::new(Null, client);
    let cancel = CancellationToken::new();

    let worker_task = bft_kit::workload::transport::run_worker(
        worker,
        client_id,
        settings.get_values("addr")?,
        cancel.clone(),
    );
    let duration = Duration::from_secs_f32(settings.get("worker.duration")?);
    let cancel_task = async move {
        sleep(duration).await;
        cancel.cancel();
        Ok(())
    };
    let (latencies, _) = try_join!(worker_task, cancel_task)?;
    println!(
        "{} ops/sec, 50th {:?}",
        latencies.len(),
        latencies.value_at_quantile(0.5)
    );
    Ok(())
}

async fn run_service() -> anyhow::Result<()> {
    let mut settings = Settings::new();
    settings.parse("addr 10.0.0.7:5000");
    settings.parse("replica.id 0");

    let replica = unreplicated::Replica::<Null>::new();
    let service = Service::new(replica, Null);
    let cancel = CancellationToken::new();

    let service_task = bft_kit::service::transport::run_replicated_service(
        service,
        settings.get("replica.id")?,
        settings.get_values("addr")?,
        cancel.clone(),
    );
    let cancel_task = async move {
        ctrl_c().await?;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(service_task, cancel_task)?;
    Ok(())
}
