use std::time::Duration;

use bft_kit::{init_logging, node::ReplayNode};
use rand::{SeedableRng, rngs::StdRng};
use tokio::{spawn, time::sleep};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let cancel = CancellationToken::new();
    let handles = ReplayNode::spawn(cancel.clone(), StdRng::seed_from_u64(117418));
    spawn(async move {
        sleep(Duration::from_secs(1)).await;
        cancel.cancel()
    });
    for handle in handles {
        handle.await?
    }
    Ok(())
}
