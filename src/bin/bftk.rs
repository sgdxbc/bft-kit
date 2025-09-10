use std::{env::args, path::Path, sync::Arc, time::Duration};

use bft_kit::{
    app::storage_item,
    init_logging_file,
    kv::ycsb::preload_iter,
    node::{ReplayFullNode, ReplayNode},
    parse::Configs,
    storage::{full, preload},
    task::SegmentedTask,
};
use rand::{SeedableRng, rngs::StdRng};
use rocksdb::{DB, properties::LIVE_SST_FILES_SIZE};
use tempfile::TempDir;
use tokio::{fs, process::Command, spawn, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging_file(std::fs::File::create("/tmp/bftk-log")?);
    let role = args().nth(1);
    let index = args().nth(2).map(|s| s.parse::<u16>());

    let mut configs = Configs::new();
    for section in ["task", "addr"] {
        configs.parse(&fs::read_to_string(format!("bftk-configs/{section}.conf")).await?);
        if let Ok(s) = fs::read_to_string(format!("bftk-configs/{section}.override.conf")).await {
            tracing::info!("overriding config section {section}");
            configs.parse(&s)
        }
    }

    match (role.as_deref(), index) {
        (Some("replica"), Some(index)) => start_replica(index?, configs).await?,
        (Some("preload"), Some(index)) => start_preload(index?, configs).await?,
        (role, index) => anyhow::bail!("unknown role: {role:?} index: {index:?}"),
    }

    Ok(())
}

const PRELOAD_DIR: &str = "bftk-preload-db";

async fn start_replica(index: u16, configs: Configs) -> anyhow::Result<()> {
    if index >= configs.get("big.num-node")? {
        return Ok(());
    }

    let task = SegmentedTask::new();
    let temp_dir = TempDir::with_prefix("big-storage")?;
    if Path::new(PRELOAD_DIR).exists() {
        tracing::info!("Using preloaded DB");
        let status = Command::new("cp")
            .arg("-rT")
            .arg(PRELOAD_DIR)
            .arg(temp_dir.path())
            .status()
            .await?;
        anyhow::ensure!(status.success())
    }
    let db = Arc::new(DB::open_default(temp_dir.path())?);

    if configs.get("big.full-storage")? {
        ReplayFullNode::spawn(
            task.handle(),
            db.clone(),
            configs.extract()?,
            StdRng::seed_from_u64(117418),
        );
    } else {
        let mut replica_addrs = configs.get_values("addrs")?;
        replica_addrs.truncate(configs.get("big.num-node")?);
        ReplayNode::spawn(
            task.handle(),
            db.clone(),
            replica_addrs,
            index,
            configs.extract()?,
            (0..configs.get("big.num-node")?).collect(),
            configs.extract()?,
            [index].into(),
            StdRng::seed_from_u64(117418),
        );
    }

    let handle = task.handle();
    spawn(handle.clone().wrap(async move {
        for _ in 0..configs.get("big.replay-duration")? {
            sleep(Duration::from_secs(1)).await;
            let size = db.property_int_value(LIVE_SST_FILES_SIZE);
            tracing::info!("LIVE_SST_FILES_SIZE {size:?}")
        }
        handle.cancel();
        Ok(())
    }));

    task.stopped().await;
    temp_dir.close()?;
    Ok(())
}

async fn start_preload(index: u16, configs: Configs) -> anyhow::Result<()> {
    let _ = fs::remove_dir_all(PRELOAD_DIR).await;
    fs::create_dir(PRELOAD_DIR).await?;
    let db = DB::open_default(PRELOAD_DIR)?;
    let items = preload_iter(configs.extract()?, StdRng::seed_from_u64(117418))
        .map(|(key, value)| storage_item(key, value));
    if configs.get("big.full-storage")? {
        full::preload(&db, items)
    } else {
        preload(&db, items, configs.extract()?, [index].into())
    }
}
