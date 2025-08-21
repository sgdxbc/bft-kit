use std::time::Duration;

use bft_kit::{
    app::null::Null,
    init_logging,
    parse::Configs,
    replication::unreplicated::{Replica, UnreplicatedClient},
    service::unsharded::{UnshardedService, transport::run_service},
    workload::{CloseLoopWorker, OpLatency, Take, transport::run_worker},
};
use tokio::spawn;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let addrs = vec![([127, 0, 0, 1], 5000).into()];

    let cancel = CancellationToken::new();

    let service = UnshardedService::new(Null, Replica::new());
    let service_task = spawn(run_service(service, 0, addrs.clone(), cancel.clone()));

    let mut configs = Configs::new();
    configs.parse("client.timeout 1.");
    let client = UnreplicatedClient::<Null>::new(0, configs.extract()?);
    let workload = OpLatency::new(Take::new(Null, 100));
    let worker = CloseLoopWorker::new(workload, client);
    let latencies = run_worker(worker, 0, addrs, CancellationToken::new()).await?;
    println!("latency: {:?}", Duration::from_nanos(latencies.min()));

    cancel.cancel();
    service_task.await?
}
