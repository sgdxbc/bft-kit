use std::{env::args, fs::File, time::Duration};

use anyhow::Context;
use bft_kit::{
    app::null::Null,
    init_logging_file,
    parse::Settings,
    service::{ReplicaIndex, Service, transport::run_replicated_service},
    unreplicated,
    workload::{CloseLoopWorker, transport::run_worker},
};
use rand::random;
use tokio::{fs::read_to_string, signal::ctrl_c, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging_file(File::create("bftk.log")?);
    match args().nth(1).as_deref() {
        Some("workload") => worker().await,
        Some("service") => {
            let index = args()
                .nth(2)
                .ok_or(anyhow::format_err!("missing index"))?
                .parse()?;
            service(index).await
        }
        _ => anyhow::bail!("unknown command"),
    }
}

async fn worker() -> anyhow::Result<()> {
    let mut settings = Settings::new();
    for name in ["addr", "client", "protocol", "workload"] {
        settings.parse(&read_to_string(format!("bftk-configs/{name}.conf")).await?);
        if let Ok(s) = read_to_string(format!("bftk-configs/{name}.override.conf")).await {
            settings.parse(&s)
        }
    }

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
    let duration = Duration::from_secs_f32(settings.get("workload.duration")?);
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

async fn service(index: ReplicaIndex) -> anyhow::Result<()> {
    let mut settings = Settings::new();
    for name in ["addr", "protocol"] {
        settings.parse(
            &read_to_string(format!("bftk-configs/{name}.conf"))
                .await
                .context(name)?,
        );
        if let Ok(s) = read_to_string(format!("bftk-configs/{name}.override.conf")).await {
            settings.parse(&s)
        }
    }

    let replica = unreplicated::Replica::<Null>::new();
    let service = Service::new(replica, Null);
    let cancel = CancellationToken::new();

    let service_task =
        run_replicated_service(service, index, settings.get_values("addr")?, cancel.clone());
    let cancel_task = async move {
        ctrl_c().await?;
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(service_task, cancel_task)?;
    Ok(())
}
