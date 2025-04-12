use std::{env::args, path::PathBuf};

use bft_testbed::{
    common::{parse::Options, workload::report_latencies},
    init_logging,
    pbft::transport::{TaskConfig, WARMUP_DURATION, run_close_loop_clients, tcp},
};
use futures::FutureExt;
use tokio::fs::read_to_string;
use tracing::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let client_config_path = PathBuf::from(args().nth(1).unwrap_or("client.conf".into()));
    let mut options = Options::new();
    if client_config_path.exists() {
        options.parse(&read_to_string(&client_config_path).await?);
    }
    options.parse(&read_to_string(client_config_path.with_file_name("common.conf")).await?);
    options.parse(&read_to_string(client_config_path.with_file_name("network.conf")).await?);

    let config = TaskConfig::try_from(options.clone())?;
    let client_latencies = if !config.use_tcp {
        run_close_loop_clients(options.try_into()?, config.clone()).left_future()
    } else {
        tcp::run_close_loop_clients(options.try_into()?, config.clone()).right_future()
    }
    .instrument(tracing::info_span!("concurrent close loops"))
    .await?;

    report_latencies(client_latencies, config.client_duration - WARMUP_DURATION);
    Ok(())
}
