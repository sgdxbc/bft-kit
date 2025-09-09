use std::{sync::Arc, time::Duration};

use bft_kit::{init_logging, node::ReplayNode, parse::Configs, task::SegmentedTask};
use rand::{SeedableRng, rngs::StdRng};
use rocksdb::{DB, properties::LIVE_SST_FILES_SIZE};
use tempfile::tempdir;
use tokio::{fs::create_dir, spawn, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();
    let mut configs = Configs::new();
    configs.parse(
        "
addrs   127.0.0.1:5000
addrs   127.0.0.1:5001
addrs   127.0.0.1:5002
addrs   127.0.0.1:5003

big.num-node        4
big.num-faulty-node 1
big.num-active-copy 1
big.num-stripe      1000
big.bypass-vote     true

ycsb.get-ratio      0.5
ycsb.num-key        100000
ycsb.value-size     1000
",
    );
    let temp_dir = tempdir()?;

    let task = SegmentedTask::new();
    let mut dbs = vec![];
    for replica_index in 0..configs.get("big.num-node")? {
        let path = temp_dir.path().join(format!("replica-{replica_index}"));
        create_dir(&path).await?;
        let db = Arc::new(DB::open_default(path)?);
        dbs.push(db.clone());
        ReplayNode::spawn(
            task.handle(),
            db.clone(),
            configs.get_values("addrs")?,
            replica_index,
            configs.extract()?,
            (0..configs.get("big.num-node")?).collect(),
            configs.extract()?,
            [replica_index].into(),
            StdRng::seed_from_u64(117418),
        );
    }
    let handle = task.handle();
    spawn(handle.clone().wrap(async move {
        for _ in 0..10 {
            sleep(Duration::from_secs(1)).await;
            let sizes = dbs
                .iter()
                .map(|db| db.property_int_value(LIVE_SST_FILES_SIZE))
                .collect::<Vec<_>>();
            tracing::info!("LIVE_SST_FILES_SIZE {sizes:?}")
        }
        handle.cancel();
        Ok(())
    }));

    task.stopped().await;
    temp_dir.close()?;
    Ok(())
}
