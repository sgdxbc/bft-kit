use std::{iter, sync::Arc, time::Duration};

use bft_kit::{
    Never,
    app::{
        kv::{Kv, ycsb::AdaptKv},
        ycsb::YcsbWorkload,
    },
    crypto::cert::quinn::server_config,
    init_logging,
    parse::Configs,
    replication::replay2::replay_loop,
    service::{
        Request,
        big2::{BigServiceLog, StorageHandle, big_loop, storage::full_replication_loop},
    },
    workload::WorkloadState,
};
use quinn::Endpoint;
use rand::{SeedableRng, rngs::StdRng};
use rocksdb::{DB, properties::LIVE_SST_FILES_SIZE};
use tempfile::tempdir;
use tokio::{join, sync::mpsc::channel, time::sleep};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_logging();

    let mut configs = Configs::new();
    configs.parse(
        "
replay.batch-size       1

ycsb.num-key            100
ycsb.value-len          4096
    ",
    );
    let endpoint = Endpoint::server(server_config(), ([127, 0, 0, 1], 5000).into())?;
    let mut workload = AdaptKv(YcsbWorkload::new(
        configs.extract()?,
        StdRng::seed_from_u64(117418),
    ));
    let logs = iter::from_fn(move || workload.next_op())
        .enumerate()
        .map(|(index, (op, _))| {
            BigServiceLog::<_, Never>::Request(Request {
                client_id: 0,
                client_seq: index as _,
                op,
            })
        });
    let temp_dir = tempdir()?;
    println!("{}", temp_dir.path().display());
    let db = Arc::new(DB::open_default(temp_dir.path())?);

    let (submit_sender, submit_receiver) = channel(100);
    let (replicated_sender, replicated_receiver) = channel(100);
    let (storage_invoke_sender, storage_invoke_receiver) = channel(100);
    let (storage_ordered_message_sender, _storage_ordered_message_receiver) = channel(100);
    let service = big_loop(
        endpoint.clone(),
        Kv,
        submit_sender,
        replicated_receiver,
        StorageHandle(storage_invoke_sender),
        storage_ordered_message_sender,
    );
    let replication = replay_loop(
        logs,
        configs.get("replay.batch-size")?,
        submit_receiver,
        replicated_sender,
    );
    let storage = full_replication_loop(db.clone(), storage_invoke_receiver);
    let sleep = async {
        for _ in 0..10 {
            sleep(Duration::from_secs(1)).await;
            let total_size = db.property_int_value(LIVE_SST_FILES_SIZE)?;
            if let Some(total_size) = total_size {
                tracing::info!(
                    "live SST files size {} MB",
                    total_size as f32 / 1000. / 1000.
                )
            }
        }
        endpoint.close(0u32.into(), b"service shutdown");
        anyhow::Ok(())
    };
    let result = join!(service, replication, storage, sleep);
    tracing::info!(?result);

    temp_dir.close()?;
    Ok(())
}
