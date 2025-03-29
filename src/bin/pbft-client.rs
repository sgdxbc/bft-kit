use std::{env::args, path::PathBuf};

use bft_testbed::pbft::{net::concurrent_close_loop_clients_task, parse::Options};
use tokio::fs::read_to_string;
use tracing::{Instrument, field};
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

    let span = tracing::info_span!("concurrent close loops", counts = field::Empty);
    let counts =
        concurrent_close_loop_clients_task(options.clone().try_into()?, options.try_into()?)
            .instrument(span.clone())
            .await?;
    span.record("counts", format!("{counts:?}"));
    Ok(())
}
