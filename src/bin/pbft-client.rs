use std::{env::args, path::PathBuf};

use bft_kit::{
    init_logging,
    parse::Options,
    pbft::transport::{TaskConfig, WARMUP_DURATION, clients_task, tcp},
    transport::ClientConfig,
    workload::report_latencies,
};
use futures::FutureExt;
use tokio::fs::read_to_string;
use tracing::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config_path = PathBuf::from(args().nth(1).unwrap_or("client.conf".into()));
    let mut options = Options::new();
    if config_path.is_file() {
        options.parse(&read_to_string(&config_path).await?);
    }
    options.parse(&read_to_string(config_path.with_file_name("common.conf")).await?);
    options.parse(&read_to_string(config_path.with_file_name("network.conf")).await?);

    let config = TaskConfig::try_from(options.clone())?;
    anyhow::ensure!(
        matches!(config.client, ClientConfig::CloseLoop),
        "yet unimplemented"
    );
    let client_latencies = if !config.use_tcp {
        clients_task(options.try_into()?, config.clone()).left_future()
    } else {
        tcp::clients_task(options.try_into()?, config.clone()).right_future()
    }
    .instrument(tracing::info_span!("concurrent close loops"))
    .await?;

    report_latencies(client_latencies, config.client_duration - WARMUP_DURATION);
    Ok(())
}
