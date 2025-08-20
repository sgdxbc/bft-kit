use std::time::Duration;

use bft_kit::{
    init_logging,
    parse::Settings,
    replication::in_memory::Replica,
    service::{
        ServiceApp,
        big::{
            // Service, ShardedStorage,
            Service,
            app::{DataShardingSchema, Kv, KvOp},
            transport::run_service,
        },
    },
    workload::WorkloadState,
};
use rand::{Rng, SeedableRng, rngs::StdRng, seq::IteratorRandom};
use rand_distr::Alphanumeric;
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let mut settings = Settings::new();
    settings.parse(
        "
big.num-node            4
big.num-shard           100
big.num-active-copy     1
big.num-cached-shard    0
",
    );
    let addrs = (0..settings.get("big.num-node")?)
        .map(|i| ([127, 0, 0, 1], 5000 + i).into())
        .collect::<Vec<_>>();

    let mut service_tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    for index in 0..settings.get("big.num-node")? {
        let app = DataShardingSchema::<Kv>::new(settings.get("big.num-shard")?);
        // let storage = ShardedStorage::new(settings.extract()?, index, [index].into(), &app);
        let storage = bft_kit::service::big::FullReplicationStorage::new(
            settings.get("big.num-shard")?,
            &app,
        );
        let service = Service::new(
            app,
            Replica::new(Workload(StdRng::seed_from_u64(117418)), 1),
            storage,
            settings.extract()?,
        );
        service_tasks.spawn(run_service(
            service,
            index,
            addrs.clone(),
            cancel.clone(),
            false,
        ));
    }

    sleep(Duration::from_secs(1)).await;
    cancel.cancel();
    while let Some(result) = service_tasks.join_next().await {
        result??;
    }
    Ok(())
}

struct Workload(StdRng);

impl WorkloadState for Workload {
    type App = DataShardingSchema<Kv>;

    fn next_op(&mut self) -> Option<<Self::App as ServiceApp>::Op> {
        let k = format!("k{:04}", (0..1_000).choose(&mut self.0).unwrap());
        let v = (&mut self.0)
            .sample_iter(Alphanumeric)
            .take(10)
            .map(char::from)
            .collect();
        Some(vec![KvOp::Put(k, v)])
    }

    fn validate(
        &self,
        _op: <Self::App as ServiceApp>::Op,
        _res: <Self::App as ServiceApp>::Res,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
