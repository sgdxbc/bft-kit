use std::{env::args, path::PathBuf, pin::pin, time::Duration};

use bft_kit::{
    init_logging,
    parse::Options,
    pbft::{
        Replica,
        transport::{TaskConfig, server_task, tcp},
    },
};
use futures::FutureExt;
use tokio::{fs::read_to_string, signal::ctrl_c, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let config_path = PathBuf::from(args().nth(1).unwrap_or("replica.conf".into()));
    let mut options = Options::new();
    options.parse(&read_to_string(&config_path).await?);
    options.parse(&read_to_string(config_path.with_file_name("common.conf")).await?);
    options.parse(&read_to_string(config_path.with_file_name("network.conf")).await?);
    let replica = Replica::new(options.clone().try_into()?);
    let config = TaskConfig::try_from(options)?;
    let use_tcp = config.use_tcp;
    let cancel = CancellationToken::new();
    let mut server = pin!(if !use_tcp {
        server_task(replica, config, cancel.clone()).left_future()
    } else {
        tcp::server_task(replica, config).right_future()
    });
    'server: {
        tokio::select! {
            result = &mut server => result?,
            result = ctrl_c() => break 'server result?,
        }
        unreachable!()
    }
    tracing::info!("exit");
    if !use_tcp {
        cancel.cancel();
        server.await
    } else {
        // before dropping `server` (and breaking every established connections), wait a
        // while until every server stop polling (hopefully)
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}
