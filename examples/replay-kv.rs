use std::{sync::Arc, time::Duration};

use bft_kit::{init_logging, node::ReplayNode, parse::Configs, task::TaskGroup};
use rand::{SeedableRng, rngs::StdRng};
use rocksdb::{DB, properties::LIVE_SST_FILES_SIZE};
use tempfile::tempdir;
use tokio::{spawn, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let mut configs = Configs::new();
    configs.parse(
        "
replica.addrs       127.0.0.1:5000

big.num-node        1
big.num-faulty-node 0
big.num-active-copy 1
big.num-stripe      1
big.bypass-vote     true
",
    );

    let temp_dir = tempdir()?;
    let db = Arc::new(DB::open_default(temp_dir.path())?);

    let cancel = CancellationToken::new();
    let handles = ReplayNode::spawn(
        TaskGroup(cancel.clone()),
        db.clone(),
        configs.get_values("replica.addrs")?,
        0,
        (0..configs.get("big.num-node")?).collect(),
        configs.extract()?,
        [0].into(),
        StdRng::seed_from_u64(117418),
    );
    spawn({
        let cancel = cancel.clone();
        async move {
            for _ in 0..10 {
                sleep(Duration::from_secs(1)).await;
                let Ok(Some(live_sst_files_size)) = db.property_int_value(LIVE_SST_FILES_SIZE)
                else {
                    tracing::error!("failed to get LIVE_SST_FILES_SIZE");
                    continue;
                };
                tracing::info!("LIVE_SST_FILES_SIZE = {live_sst_files_size}")
            }
            cancel.cancel()
        }
    });

    cancel.cancelled().await;
    for handle in handles {
        handle.await?
    }
    temp_dir.close()?;
    Ok(())
}
