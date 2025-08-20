use std::{env::args, fs::File, sync::Arc, time::Duration};

use bft_kit::{
    app::null::Null,
    init_logging_file,
    parse::Configs,
    replication::{ReplicaIndex, in_memory, unreplicated},
    service::{
        big::{
            self, FullReplicationStorage, ShardedStorage,
            app::{DataShardingSchema, Kv},
        },
        unsharded,
    },
    set_affinity_block_on,
    workload::{self, CloseLoopWorker, NanoLatencies, OpenLoopWorker, transport::run_worker},
};
use rand::{SeedableRng as _, random, rngs::StdRng};
use tokio::{fs::read_to_string, signal::ctrl_c, task::JoinSet, time::sleep, try_join};
use tokio_util::sync::CancellationToken;

fn main() -> anyhow::Result<()> {
    init_logging_file(File::create("/tmp/bftk-log")?);
    set_affinity_block_on(async {
        let mut configs = Configs::new();
        for name in ["addr", "task"] {
            configs.parse(&read_to_string(format!("bftk-configs/{name}.conf")).await?);
            if let Ok(s) = read_to_string(format!("bftk-configs/{name}.override.conf")).await {
                configs.parse(&s)
            }
        }
        match args().nth(1).as_deref() {
            Some("workers") => workers(configs).await,
            Some("service") => {
                let index = args()
                    .nth(2)
                    .ok_or(anyhow::format_err!("missing index"))?
                    .parse()?;
                service(index, configs).await
            }
            _ => anyhow::bail!("unknown command"),
        }
    })
}

async fn workers(configs: Configs) -> anyhow::Result<()> {
    let configs = Arc::new(configs);

    let cancel = CancellationToken::new();

    let mut worker_set = JoinSet::new();
    for _ in 0..configs.get("workload.concurrency")? {
        let client_id = random();
        let client = unreplicated::Client::<Null>::new(client_id, configs.extract()?);
        worker_set.spawn(worker(configs.clone(), client_id, client, cancel.clone()));
    }

    let worker_task = async {
        let mut latencies = NanoLatencies::new(3).unwrap();
        while let Some(result) = worker_set.join_next().await {
            latencies += result??
        }
        anyhow::Ok(latencies)
    };
    let duration = Duration::from_secs_f32(configs.get("workload.duration")?);
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
    configs: impl AsRef<Configs>,
    client_id: u32,
    client: unreplicated::Client<Null>,
    cancel: CancellationToken,
) -> anyhow::Result<NanoLatencies> {
    let configs = configs.as_ref();
    let workload = workload::OpLatency::new(Null);
    if configs.get("workload.close-loop")? {
        let worker = CloseLoopWorker::new(workload, client);
        run_worker(worker, client_id, configs.get_values("addr")?, cancel).await
    } else {
        let target_tput = configs.get("open-loop.target-tput")?;
        let worker = OpenLoopWorker::new(workload, client, target_tput);
        run_worker(worker, client_id, configs.get_values("addr")?, cancel).await
    }
}

async fn service(index: ReplicaIndex, configs: Configs) -> anyhow::Result<()> {
    let configs = Arc::new(configs);
    let cancel = CancellationToken::new();
    let service_task = {
        let configs = configs.clone();
        let cancel = cancel.clone();
        async move {
            match &*configs.get::<String>("protocol")? {
                "unsharded" => service_unsharded(index, &configs, cancel).await,
                "big" => service_big(index, &configs, cancel).await,
                _ => anyhow::bail!("unknown protocol"),
            }
        }
    };
    let cancel_task = async move {
        if configs.get::<String>("protocol")? != "big" {
            ctrl_c().await?
        } else {
            sleep(Duration::from_secs_f32(configs.get("workload.duration")?)).await
        }
        cancel.cancel();
        anyhow::Ok(())
    };
    try_join!(service_task, cancel_task)?;
    Ok(())
}

async fn service_unsharded(
    index: ReplicaIndex,
    configs: &Configs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let service = unsharded::Service::new(Null, unreplicated::Replica::new());
    unsharded::transport::run_service(service, index, configs.get_values("addr")?, cancel).await
}

async fn service_big(
    index: ReplicaIndex,
    configs: &Configs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let num_shard = configs.get("big.num-shard")?;
    let app = DataShardingSchema::<Kv>::new(num_shard);
    let replica = in_memory::Replica::new(
        Workload(StdRng::seed_from_u64(117418)),
        configs.get("in-memory.batch-size")?,
    );
    let addrs = configs.get_values("addr")?;
    let service_config = configs.extract()?;
    if configs.get("big.sharded")? {
        let storage = ShardedStorage::new(configs.extract()?, index, [index].into(), &app);
        let service = big::Service::new(app, replica, storage, service_config);
        big::transport::run_service(service, index, addrs, cancel, false).await
    } else {
        let storage = FullReplicationStorage::new(num_shard, &app);
        let service = big::Service::new(app, replica, storage, service_config);
        big::transport::run_service(service, index, addrs, cancel, false).await
    }
}

struct Workload(StdRng);

mod workload_impl {
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
        type Metadata = ();

        fn next_op(&mut self) -> Option<(<Self::App as ServiceApp>::Op, Self::Metadata)> {
            let k = format!("k{:04}", (0..1_000).choose(&mut self.0).unwrap());
            let v = (&mut self.0)
                .sample_iter(Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();
            Some((vec![KvOp::Put(k, v)], ()))
        }
    }
}
