use std::time::Duration;

use bft_kit::{
    app::null::Null,
    init_logging,
    parse::Settings,
    replication::unreplicated::{Client, Replica},
    service::{Service, transport::run_replicated_service},
    workload::{self, CloseLoopWorker, transport::run_worker},
};
use tokio::spawn;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let addrs = vec![([127, 0, 0, 1], 5000).into()];

    let cancel = CancellationToken::new();

    let replica = Replica::new();
    let service = Service::new(replica, Null);
    let service_task = spawn(run_replicated_service(
        service,
        0,
        addrs.clone(),
        cancel.clone(),
    ));

    let mut settings = Settings::new();
    settings.parse("client.timeout 1.");
    let client = Client::<Null>::new(0, settings.extract()?);
    let workload = workload::Take::new(Null, 100);
    let worker = CloseLoopWorker::new(workload, client);
    let latencies = run_worker(worker, 0, addrs, CancellationToken::new()).await?;
    println!("latency: {:?}", Duration::from_nanos(latencies.min()));

    cancel.cancel();
    service_task.await?
}
