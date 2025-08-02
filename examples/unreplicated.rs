use std::time::Duration;

use bft_kit::{
    app::null::Null,
    init_logging,
    parse::Settings,
    service::{Service, transport::run_replicated_service},
    unreplicated::{Client, Replica},
    worker::{CloseLoopWorker, transport::run_worker, workload},
};
use tokio::spawn;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let addrs = vec![([127, 0, 0, 1], 5000).into()];

    let cancel = CancellationToken::new();

    let replica = Replica::<Null>::new();
    let service = Service::new(replica, Null);
    let service_task = spawn(run_replicated_service(
        service,
        0,
        addrs.clone(),
        cancel.clone(),
    ));

    let mut settings = Settings::new();
    settings.parse("client.timeout 1");
    let client = Client::<Null>::new(0, settings.extract()?);
    let workload = workload::Take::new(Null, 1);
    let worker = CloseLoopWorker::new(workload, client);
    let latencies = run_worker(worker, 0, addrs, CancellationToken::new()).await?;
    println!("latency: {:?}", Duration::from_micros(latencies.min()));

    cancel.cancel();
    service_task.await?
}
