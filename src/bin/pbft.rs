use std::{env::args, path::PathBuf, pin::pin, time::Duration};

use bft_testbed::{
    init_logging,
    pbft::{Replica, transport::server_task, parse::Options},
};
use tokio::{fs::read_to_string, signal::ctrl_c, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let replica_config_path = PathBuf::from(args().nth(1).unwrap_or("replica.conf".into()));
    let mut options = Options::new();
    options.parse(&read_to_string(&replica_config_path).await?)?;
    options.parse(&read_to_string(replica_config_path.with_file_name("spec.conf")).await?)?;
    options.parse(&read_to_string(replica_config_path.with_file_name("task.conf")).await?)?;
    let replica = Replica::new(options.clone().try_into()?);
    let mut server = pin!(server_task(replica, options.try_into()?));
    'server: {
        tokio::select! {
            result = &mut server => result?,
            result = ctrl_c() => break 'server result?,
        }
        unreachable!()
    }
    tracing::info!("exit");
    // before dropping `server` (and breaking every established connections), wait a
    // while until every server stop polling (hopefully)
    sleep(Duration::from_secs(1)).await;
    Ok(())
}
