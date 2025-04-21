use std::{env::args, path::PathBuf, pin::pin};

use bft_kit::{init_logging, parse::Options, unreplicated::transport::server_task};
use tokio::{fs::read_to_string, signal::ctrl_c};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config_path = PathBuf::from(args().nth(1).unwrap_or("replica.conf".into()));
    let mut options = Options::new();
    if config_path.is_file() {
        options.parse(&read_to_string(&config_path).await?);
    }
    options.parse(&read_to_string(config_path.with_file_name("common.conf")).await?);
    options.parse(&read_to_string(config_path.with_file_name("network.conf")).await?);

    let cancel = CancellationToken::new();
    let mut server = pin!(server_task(options.try_into()?, cancel.clone()));
    'server: {
        tokio::select! {
            result = &mut server => result?,
            result = ctrl_c() => break 'server result?,
        }
        unreachable!()
    }
    tracing::info!("exit");
    cancel.cancel();
    server.await
}
