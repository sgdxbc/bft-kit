use std::time::Duration;

use bft_kit::{
    app::ycsb::YcsbWorkload,
    init_logging,
    parse::Configs,
    replication::replay::Replica,
    service::big::{
        Service,
        app::{DataShardingSchema, Kv, ycsb::AdaptKv},
        transport::run_service,
    },
};
use rand::{SeedableRng, rngs::StdRng};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    //     let mut configs = Configs::new();
    //     configs.parse(
    //         "
    // big.num-node            4
    // big.num-shard           100
    // big.num-active-copy     1
    // big.num-cached-shard    0

    // ycsb.num-key            100
    // ycsb.value-len          10
    // ",
    //     );
    //     let addrs = (0..configs.get("big.num-node")?)
    //         .map(|i| ([127, 0, 0, 1], 5000 + i).into())
    //         .collect::<Vec<_>>();

    //     let mut service_tasks = JoinSet::new();
    //     let cancel = CancellationToken::new();
    //     for index in 0..configs.get("big.num-node")? {
    //         let app = DataShardingSchema::<Kv>::new(configs.get("big.num-shard")?);
    //         // let storage = ShardedStorage::new(settings.extract()?, index, [index].into(), &app);
    //         let storage =
    //             bft_kit::service::big::FullReplicationStorage::new(configs.get("big.num-shard")?, &app);
    //         let workload = AdaptKv(YcsbWorkload::new(
    //             configs.extract()?,
    //             StdRng::seed_from_u64(117418),
    //         ));
    //         let service = Service::new(app, Replica::new(workload, 1), storage, configs.extract()?);
    //         service_tasks.spawn(run_service(
    //             service,
    //             index,
    //             addrs.clone(),
    //             cancel.clone(),
    //             false,
    //         ));
    //     }

    //     sleep(Duration::from_secs(1)).await;
    //     cancel.cancel();
    //     while let Some(result) = service_tasks.join_next().await {
    //         result??;
    //     }
    Ok(())
}
