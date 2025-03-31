use std::{env::args, path::PathBuf, time::Duration};

use bft_testbed::{
    init_logging,
    pbft::{
        net::{TaskConfig, concurrent_close_loop_clients_task},
        parse::Options,
    },
};
use hdrhistogram::Histogram;
use tokio::fs::read_to_string;
use tracing::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let task_config_path = PathBuf::from(args().nth(1).unwrap_or("task.conf".into()));
    let mut options = Options::new();
    options.parse(&read_to_string(&task_config_path).await?)?;
    options.parse(&read_to_string(task_config_path.with_file_name("spec.conf")).await?)?;

    let task_config = TaskConfig::try_from(options.clone())?;
    let client_latencies =
        concurrent_close_loop_clients_task(options.try_into()?, task_config.clone())
            .instrument(tracing::info_span!("concurrent close loops"))
            .await?;
    let mut latencies = Histogram::new(3)?;
    for client_latencies in client_latencies {
        let throughput = client_latencies.len() as f32 / task_config.client_duration.as_secs_f32();
        let throughput2 = 1_000_000. / client_latencies.mean();
        tracing::info!("client throughput {throughput:.2} ({throughput2:.2})");
        latencies += client_latencies
    }
    let throughput = latencies.len() as f32 / task_config.client_duration.as_secs_f32();
    let throughput2 = 1_000_000. / latencies.mean();
    tracing::info!("throughput {throughput:.2} ({throughput2:.2})");
    for value in latencies.iter_quantiles(1) {
        if value.count_since_last_iteration() == 0 {
            continue;
        }
        tracing::info!(
            "quantile {:8.6} latency {:10?}",
            value.quantile(),
            Duration::from_micros(value.value_iterated_to()),
        )
    }
    for value in latencies.iter_log(1, 2.) {
        let count = value.count_since_last_iteration();
        if count == 0 {
            continue;
        }
        tracing::info!(
            "{:8} ({:4.2}) requests <= {:?}",
            count,
            count as f32 / latencies.len() as f32,
            Duration::from_micros(value.value_iterated_to())
        );
    }
    Ok(())
}
