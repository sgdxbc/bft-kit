use std::{env::args, time::Duration};

use bft_kit::{
    app::null::Null,
    init_logging,
    parse::Settings,
    service::{Service, transport::run_replicated_service},
    unreplicated,
    workload::{CloseLoopWorker, transport::run_worker},
};
use rand::random;
use tokio::{signal::ctrl_c, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    match args().nth(1).as_deref() {
        Some("workload") => worker().await,
        Some("service") => service().await,
        _ => anyhow::bail!("Usage: bk [workload|service]"),
    }
}

async fn worker() -> anyhow::Result<()> {
    let mut settings = Settings::new();
    settings.parse("addr 10.0.0.7:5000");
    settings.parse("client.timeout 1");
    settings.parse("worker.duration 10");

    let client_id = random();
    let client = unreplicated::Client::<Null>::new(client_id, settings.extract()?);
    let worker = CloseLoopWorker::new(Null, client);
    let cancel = CancellationToken::new();

    let worker_task = run_worker(
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
        Duration::from_nanos(latencies.value_at_quantile(0.5))
    );
    Ok(())
}

async fn service() -> anyhow::Result<()> {
    let mut settings = Settings::new();
    settings.parse("addr 10.0.0.7:5000");
    settings.parse("replica.id 0");

    let replica = unreplicated::Replica::<Null>::new();
    let service = Service::new(replica, Null);
    let cancel = CancellationToken::new();

    let service_task = run_replicated_service(
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
