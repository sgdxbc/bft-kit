use std::net::SocketAddr;

use bincode::Decode;
use hdrhistogram::Histogram;
use quinn::Endpoint;
use tokio::select;
use tokio_util::task::TaskTracker;

use crate::{
    crypto::cert::quinn::client_config,
    service::ClientId,
    state::{AppState, State},
    transport::{BINCODE_CONFIG, read_loop, trace_error},
};

pub async fn run_worker<S: State<Output = Histogram<u64>>, A: AppState>(
    mut worker: S,
    client_id: ClientId,
    addrs: Vec<SocketAddr>,
) -> anyhow::Result<Histogram<u64>>
where
    S::Message: Decode<()>,
{
    let endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());

    let mut connections = Vec::new();
    let tracker = TaskTracker::new();

    enum Event {
        Message(Vec<u8>),
        Tick,
    }
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(100);

    for addr in addrs {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        connection
            .open_uni()
            .await?
            .write_all(&client_id.to_le_bytes())
            .await?;
        tracker.spawn(trace_error(
            "connection read",
            read_loop(
                connection.clone(),
                event_sender.clone(),
                Event::Message,
                None,
            ),
        ));
        connections.push(connection)
    }

    loop {
        match select! {
            Some(event) = event_receiver.recv() => event,
        } {
            Event::Message(bytes) => {
                let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                worker.receive(message);
            }
            Event::Tick => {}
        }
    }

    Ok(())
}
