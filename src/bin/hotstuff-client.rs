use std::{env::args, path::PathBuf};

use bft_kit::{
    hotstuff::transport::{TaskConfig, WARMUP_DURATION, clients_task},
    init_logging,
    parse::Options,
    workload::report_latencies,
};
use tokio::fs::read_to_string;
use tracing::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config_path = PathBuf::from(args().nth(1).unwrap_or("client.conf".into()));
    let mut options = Options::new();
    if config_path.exists() {
        options.parse(&read_to_string(&config_path).await?);
    }
    options.parse(&read_to_string(config_path.with_file_name("common.conf")).await?);
    options.parse(&read_to_string(config_path.with_file_name("network.conf")).await?);

    let config = TaskConfig::try_from(options.clone())?;
    let client_latencies = clients_task(options.try_into()?, config.clone())
        .instrument(tracing::info_span!("concurrent close loops"))
        .await?;

    report_latencies(client_latencies, config.client_duration - WARMUP_DURATION);
    Ok(())
}
