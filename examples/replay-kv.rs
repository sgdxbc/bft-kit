use std::time::Duration;

use bft_kit::{init_logging, node::ReplayNode, task::TaskGroup};
use rand::{SeedableRng, rngs::StdRng};
use rocksdb::DB;
use tempfile::tempdir;
use tokio::{spawn, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let temp_dir = tempdir()?;
    let db = DB::open_default(temp_dir.path())?;

    let cancel = CancellationToken::new();
    let handles = ReplayNode::spawn(TaskGroup(cancel.clone()), db, StdRng::seed_from_u64(117418));
    spawn({
        let cancel = cancel.clone();
        async move {
            sleep(Duration::from_secs(3)).await;
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
