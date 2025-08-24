use std::{iter, time::Duration};

use bft_kit::{
    app::{
        kv::{Kv, ycsb::AdaptKv},
        ycsb::YcsbWorkload,
    },
    init_logging,
    parse::Configs,
    replication::replay::ReplayReplica,
    service::{
        Request,
        big::{BigService, storage::ShardedStorage, transport::run_service},
    },
    workload::WorkloadState,
};
use rand::{SeedableRng, rngs::StdRng};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let mut configs = Configs::new();
    configs.parse(
        "
big.num-node            4
big.num-active-copy     1
big.num-cached-value    0
big.num-max-will-fetch  0

ycsb.num-key            100
ycsb.value-len          10
    ",
    );
    let addrs = (0..configs.get("big.num-node")?)
        .map(|i| ([127, 0, 0, 1], 5000 + i).into())
        .collect::<Vec<_>>();

    let mut service_tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    for index in 0..configs.get("big.num-node")? {
        let app = Kv;
        let storage = ShardedStorage::new(configs.extract()?, index, [index].into());
        // let storage = bft_kit::service::big::storage::FullReplicationStorage::new();
        let mut workload = AdaptKv(YcsbWorkload::new(
            configs.extract()?,
            StdRng::seed_from_u64(117418),
        ));
        let logs = iter::from_fn(move || workload.next_op())
            .enumerate()
            .map(|(index, (op, _))| Request {
                client_id: 0,
                client_seq: index as _,
                op,
            });
        let service = BigService::new(
            app,
            ReplayReplica::new(logs, 1),
            storage,
            configs.extract()?,
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
