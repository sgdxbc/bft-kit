use std::{env::args, fs::File, sync::Arc, time::Duration};

use anyhow::Context;
use bft_kit::{
    app::null::Null,
    init_logging_file,
    parse::Settings,
    replication::{ReplicaIndex, unreplicated},
    service::{Request, Service, transport::run_replicated_service},
    set_affinity_block_on,
    workload::{CloseLoopWorker, Latencies, OpenLoopWorker, transport::run_worker},
};
use rand::random;
use tokio::{fs::read_to_string, signal::ctrl_c, task::JoinSet, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

fn main() -> anyhow::Result<()> {
    init_logging_file(File::create("/tmp/bftk-log")?);
    set_affinity_block_on(async {
        match args().nth(1).as_deref() {
            Some("workload") => workload().await,
            Some("service") => {
                let index = args()
                    .nth(2)
                    .ok_or(anyhow::format_err!("missing index"))?
                    .parse()?;
                service(index).await
            }
            _ => anyhow::bail!("unknown command"),
        }
    })
}

async fn workload() -> anyhow::Result<()> {
    let mut settings = Settings::new();
    for name in ["addr", "client", "protocol", "workload"] {
        settings.parse(&read_to_string(format!("bftk-configs/{name}.conf")).await?);
        if let Ok(s) = read_to_string(format!("bftk-configs/{name}.override.conf")).await {
            settings.parse(&s)
        }
    }
    let settings = Arc::new(settings);

    let cancel = CancellationToken::new();

    let mut worker_set = JoinSet::new();
    for _ in 0..settings.get("workload.concurrency")? {
        let client_id = random();
        let client = unreplicated::Client::<Null>::new(client_id, settings.extract()?);
        worker_set.spawn(worker(settings.clone(), client_id, client, cancel.clone()));
    }

    let worker_task = async {
        let mut latencies = Latencies::new(3).unwrap();
        while let Some(result) = worker_set.join_next().await {
            latencies += result??
        }
        anyhow::Ok(latencies)
    };
    let duration = Duration::from_secs_f32(settings.get("workload.duration")?);
    let cancel_task = async move {
        sleep(duration).await;
        cancel.cancel();
        Ok(())
    };
    let (latencies, _) = try_join!(worker_task, cancel_task)?;
    println!(
        "{} ops/sec, 50th {:?}",
        latencies.len() as f32 / duration.as_secs_f32(),
        Duration::from_nanos(latencies.value_at_quantile(0.5))
    );
    Ok(())
}

async fn worker(
    settings: impl AsRef<Settings>,
    client_id: u32,
    client: unreplicated::Client<Null>,
    cancel: CancellationToken,
) -> anyhow::Result<Latencies> {
    let settings = settings.as_ref();
    if settings.get("workload.close-loop")? {
        let worker = CloseLoopWorker::new(Null, client);
        run_worker(worker, client_id, settings.get_values("addr")?, cancel).await
    } else {
        let target_tput = settings.get("workload.open-loop.target-tput")?;
        let worker = OpenLoopWorker::new(Null, client, target_tput);
        run_worker(worker, client_id, settings.get_values("addr")?, cancel).await
    }
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

    let replica = unreplicated::Replica::new();
    let service = Service::new(replica, Null);
    let cancel = CancellationToken::new();

    let service_task = run_replicated_service::<_, unreplicated::Replica<Request<()>>, _, Null>(
        service,
        index,
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
