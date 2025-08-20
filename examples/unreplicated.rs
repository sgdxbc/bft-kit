use std::time::Duration;

use bft_kit::{
    app::null::Null,
    init_logging,
    parse::Configs,
    replication::unreplicated::{Client, Replica},
    service::unsharded::{Service, transport::run_service},
    workload::{self, CloseLoopWorker, transport::run_worker},
};
use tokio::spawn;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let addrs = vec![([127, 0, 0, 1], 5000).into()];

    let cancel = CancellationToken::new();

    let service = Service::new(Null, Replica::new());
    let service_task = spawn(run_service(service, 0, addrs.clone(), cancel.clone()));

    let mut configs = Configs::new();
    configs.parse("client.timeout 1.");
    let client = Client::<Null>::new(0, configs.extract()?);
    let workload = workload::OpLatency::new(workload::Take::new(Null, 100));
    let worker = CloseLoopWorker::new(workload, client);
    let latencies = run_worker(worker, 0, addrs, CancellationToken::new()).await?;
    println!("latency: {:?}", Duration::from_nanos(latencies.min()));

    cancel.cancel();
    service_task.await?
}
