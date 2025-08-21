use std::{env::args, fs::File, iter, sync::Arc, time::Duration};

use bft_kit::{
    app::{null::Null, ycsb::YcsbWorkload},
    init_logging_file,
    parse::Configs,
    replication::{
        ReplicaIndex,
        replay::ReplayReplica,
        unreplicated::{UnreplicatedClient, UnreplicatedReplica},
    },
    service::{
        Request,
        big::{
            self, BigService, FullReplicationStorage, ShardedStorage,
            app::{DataShardingSchema, Kv, ycsb::AdaptKv},
        },
        unsharded::{self, UnshardedService},
    },
    set_affinity_block_on,
    workload::{
        CloseLoopWorker, NanoLatencies, OpLatency, OpenLoopWorker, WorkloadState,
        transport::run_worker,
    },
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
        worker_set.spawn(worker(configs.clone(), cancel.clone()));
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
    cancel: CancellationToken,
) -> anyhow::Result<NanoLatencies> {
    let configs = configs.as_ref();
    let workload = OpLatency::new(Null);
    let client_id = random();
    let client = UnreplicatedClient::new(client_id, configs.extract()?);

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
    let service = UnshardedService::new(Null, UnreplicatedReplica::new());
    unsharded::transport::run_service(service, index, configs.get_values("addr")?, cancel).await
}

async fn service_big(
    index: ReplicaIndex,
    configs: &Configs,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let app = Kv(DataShardingSchema::new(configs.get("big.num-shard")?));
    let mut workload = AdaptKv(YcsbWorkload::new(
        configs.extract()?,
        StdRng::seed_from_u64(117418),
    ));
    let logs = iter::from_fn(|| workload.next_op())
        .enumerate()
        .map(|(index, (op, _))| Request {
            client_id: 0,
            client_seq: index as _,
            op,
        });
    let replica = ReplayReplica::new(logs, configs.get("in-memory.batch-size")?);
    let addrs = configs.get_values("addr")?;
    let service_config = configs.extract()?;
    if configs.get("big.sharded")? {
        let storage = ShardedStorage::new(configs.extract()?, index, [index].into());
        let service = BigService::new(app, replica, storage, service_config);
        big::transport::run_service(service, index, addrs, cancel, false).await
    } else {
        let storage = FullReplicationStorage::new();
        let service = BigService::new(app, replica, storage, service_config);
        big::transport::run_service(service, index, addrs, cancel, false).await
    }
}
