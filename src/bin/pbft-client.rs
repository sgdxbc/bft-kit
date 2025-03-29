use std::{env::args, path::PathBuf};

use bft_testbed::pbft::{net::concurrent_close_loop_clients_task, parse::Options};
use hdrhistogram::Histogram;
use tokio::fs::read_to_string;
use tracing::Instrument;
use tracing_subscriber::fmt::format::FmtSpan;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        .init();
    let task_config_path = PathBuf::from(args().nth(1).unwrap_or("task.conf".into()));
    let mut options = Options::new();
    options.parse(&read_to_string(&task_config_path).await?)?;
    options.parse(&read_to_string(task_config_path.with_file_name("spec.conf")).await?)?;

    let client_latencies =
        concurrent_close_loop_clients_task(options.clone().try_into()?, options.try_into()?)
            .instrument(tracing::info_span!("concurrent close loops"))
            .await?;
    let mut latencies = Histogram::new(3)?;
    for client_latencies in client_latencies {
        let throughput = 1_000_000. / client_latencies.mean();
        tracing::info!(throughput, "client");
        latencies += client_latencies
    }
    let throughput = 1_000_000. / latencies.mean();
    let p50 = latencies.value_at_quantile(0.5);
    let p99 = latencies.value_at_quantile(0.99);
    tracing::info!(throughput, p50, p99);
    Ok(())
}
