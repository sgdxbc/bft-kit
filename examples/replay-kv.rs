use std::time::Duration;

use bft_kit::{init_logging, node::ReplayNode, task::TaskGroup};
use rand::{SeedableRng, rngs::StdRng};
use tokio::{spawn, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let cancel = CancellationToken::new();
    let handles = ReplayNode::spawn(TaskGroup(cancel.clone()), StdRng::seed_from_u64(117418));
    spawn({
        let cancel = cancel.clone();
        async move {
            sleep(Duration::from_secs(1)).await;
            cancel.cancel()
        }
    });

    cancel.cancelled().await;
    for handle in handles {
        handle.await?
    }
    Ok(())
}
