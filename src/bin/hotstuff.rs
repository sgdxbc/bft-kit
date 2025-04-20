use std::{env::args, path::PathBuf, pin::pin};

use bft_kit::{
    hotstuff::{CryptoConfig, Replica, transport::server_task},
    init_logging,
    parse::Options,
};
use tokio::{fs::read_to_string, signal::ctrl_c};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config_path = PathBuf::from(args().nth(1).unwrap_or("replica.conf".into()));
    let mut options = Options::new();
    options.parse(&read_to_string(&config_path).await?);
    options.parse(&read_to_string(config_path.with_file_name("common.conf")).await?);
    options.parse(&read_to_string(config_path.with_file_name("network.conf")).await?);

    let replica = Replica::new(
        options.clone().try_into()?,
        CryptoConfig::new(options.clone())?,
    );
    let cancel = CancellationToken::new();
    let mut server = pin!(server_task(replica, options.try_into()?, cancel.clone()));
    'server: {
        tokio::select! {
            result = &mut server => result?,
            result = ctrl_c() => break 'server result?,
        }
        unreachable!()
    }
    tracing::info!("exit");
    server.await
}
