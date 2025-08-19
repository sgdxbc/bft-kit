use std::{env::args, fs::File, sync::Arc, time::Duration};

use bft_kit::{
    app::null::Null,
    init_logging_file,
    parse::Settings,
    replication::{ReplicaIndex, in_memory, unreplicated},
    service::{
        big::{
            self, FullReplicationStorage, ShardedStorage,
            app::{DataShardingSchema, Kv},
        },
        unsharded,
    },
    set_affinity_block_on,
    workload::{CloseLoopWorker, NanoLatencies, OpenLoopWorker, transport::run_worker},
};
use rand::random;
use tokio::{fs::read_to_string, signal::ctrl_c, task::JoinSet, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

fn main() -> anyhow::Result<()> {
    init_logging_file(File::create("/tmp/bftk-log")?);
    set_affinity_block_on(async {
        let mut settings = Settings::new();
        for name in ["addr", "task"] {
            settings.parse(&read_to_string(format!("bftk-configs/{name}.conf")).await?);
            if let Ok(s) = read_to_string(format!("bftk-configs/{name}.override.conf")).await {
                settings.parse(&s)
            }
        }
        match args().nth(1).as_deref() {
            Some("workload") => workload(settings).await,
            Some("service") => {
                let index = args()
                    .nth(2)
                    .ok_or(anyhow::format_err!("missing index"))?
                    .parse()?;
                service(index, settings).await
            }
            _ => anyhow::bail!("unknown command"),
        }
    })
}

async fn workload(settings: Settings) -> anyhow::Result<()> {
    let settings = Arc::new(settings);

    let cancel = CancellationToken::new();

    let mut worker_set = JoinSet::new();
    for _ in 0..settings.get("workload.concurrency")? {
        let client_id = random();
        let client = unreplicated::Client::<Null>::new(client_id, settings.extract()?);
        worker_set.spawn(worker(settings.clone(), client_id, client, cancel.clone()));
    }

    let worker_task = async {
        let mut latencies = NanoLatencies::new(3).unwrap();
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
) -> anyhow::Result<NanoLatencies> {
    let settings = settings.as_ref();
    if settings.get("workload.close-loop")? {
        let worker = CloseLoopWorker::new(Null, client);
        run_worker(worker, client_id, settings.get_values("addr")?, cancel).await
    } else {
        let target_tput = settings.get("open-loop.target-tput")?;
        let worker = OpenLoopWorker::new(Null, client, target_tput);
        run_worker(worker, client_id, settings.get_values("addr")?, cancel).await
    }
}

async fn service(index: ReplicaIndex, settings: Settings) -> anyhow::Result<()> {
    let settings = Arc::new(settings);
    let cancel = CancellationToken::new();
    let service_task = {
        let settings = settings.clone();
        let cancel = cancel.clone();
        async move {
            match &*settings.get::<String>("protocol")? {
                "unsharded" => service_unsharded(index, &settings, cancel).await,
                "big" => service_big(index, &settings, cancel).await,
                _ => anyhow::bail!("unknown protocol"),
            }
        }
    };
    let cancel_task = async move {
        if settings.get::<String>("protocol")? != "big" {
            ctrl_c().await?
        } else {
            sleep(Duration::from_secs_f32(settings.get("workload.duration")?)).await
        }
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(service_task, cancel_task)?;
    Ok(())
}

async fn service_unsharded(
    index: ReplicaIndex,
    settings: &Settings,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let service = unsharded::Service::new(Null, unreplicated::Replica::new());
    unsharded::transport::run_service(service, index, settings.get_values("addr")?, cancel).await
}

async fn service_big(
    index: ReplicaIndex,
    settings: &Settings,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let num_shard = settings.get("big.num-shard")?;
    let app = DataShardingSchema::<Kv>::new(num_shard);
    let replica = in_memory::Replica::new(Workload, settings.get("in-memory.batch-size")?);
    let addrs = settings.get_values("addr")?;
    if settings.get("big.sharded")? {
        let storage = ShardedStorage::new(settings.extract()?, index, [index].into(), &app);
        let service = big::Service::new(app, replica, storage);
        big::transport::run_service(service, index, addrs, cancel, false).await
    } else {
        let storage = FullReplicationStorage::new(num_shard, &app);
        let service = big::Service::new(app, replica, storage);
        big::transport::run_service(service, index, addrs, cancel, false).await
    }
}

struct Workload;

mod workload {
    use bft_kit::{
        service::{
            ServiceApp,
            big::app::{DataShardingSchema, Kv, KvOp},
        },
        workload::WorkloadState,
    };
    use rand::{Rng as _, seq::IteratorRandom};
    use rand_distr::Alphanumeric;

    impl WorkloadState for super::Workload {
        type App = DataShardingSchema<Kv>;

        fn next_op(&mut self) -> Option<<Self::App as ServiceApp>::Op> {
            let mut rng = rand::rng();
            let k = format!("k{:04}", (0..10_000).choose(&mut rng).unwrap());

            Some(vec![if rng.random_ratio(50, 100) {
                let v = rng
                    .sample_iter(Alphanumeric)
                    .take(10)
                    .map(char::from)
                    .collect();
                KvOp::Put(k, v)
            } else {
                KvOp::Get(k)
            }])
        }

        fn validate(
            &self,
            _op: <Self::App as ServiceApp>::Op,
            _res: <Self::App as ServiceApp>::Res,
        ) -> anyhow::Result<()> {
            Ok(())
        }
    }
}
