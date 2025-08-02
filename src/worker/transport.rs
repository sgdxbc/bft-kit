use std::{net::SocketAddr, time::Duration};

use bincode::Decode;
use quinn::{Connection, Endpoint};
use tokio::{
    select, spawn,
    time::{Instant, sleep},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    crypto::cert::quinn::client_config,
    service::ClientId,
    state::{Proceed, State},
    transport::{BINCODE_CONFIG, ReplicationSend, read_loop, trace_error},
    worker::Latencies,
};

pub async fn run_worker<S: State<Output = anyhow::Result<()>> + Into<Latencies>>(
    mut worker: S,
    client_id: ClientId,
    addrs: Vec<SocketAddr>,
    cancel: CancellationToken,
) -> anyhow::Result<Latencies>
where
    S::Message: Decode<()>,
    S::Send: ReplicationSend,
{
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());

    enum Event {
        Message(Vec<u8>),
        Tick,
    }
    let (event_sender, mut event_receiver) = tokio::sync::mpsc::channel(100);

    let mut connections = Vec::new();
    let mut read_tasks = Vec::new();
    for addr in addrs {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        connection
            .open_uni()
            .await?
            .write_all(&client_id.to_le_bytes())
            .await?;
        let task = spawn(trace_error(
            "connection read",
            read_loop(
                connection.clone(),
                event_sender.clone(),
                Event::Message,
                None,
            ),
        ));
        connections.push(connection);
        read_tasks.push(task)
    }

    let write_tracker = TaskTracker::new();
    let start = Instant::now();
    let mut option_tick_at =
        worker_proceed(&mut worker, start.elapsed(), &connections, &write_tracker)?;
    while let Some(tick_at) = option_tick_at {
        match select! {
            Some(event) = event_receiver.recv() => event,
            () = sleep(tick_at) => Event::Tick,
            () = cancel.cancelled() => break,
        } {
            Event::Message(bytes) => {
                let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                worker.receive(message)
            }
            Event::Tick => {}
        }
        option_tick_at = worker_proceed(&mut worker, start.elapsed(), &connections, &write_tracker)?
    }

    for (connection, task) in connections.into_iter().zip(read_tasks) {
        connection.close(0u32.into(), b"worker stopped");
        task.await.unwrap() // not cancelled anywhere and propagate panics
    }
    Ok(worker.into())
}

fn worker_proceed<S: State<Output = anyhow::Result<()>>>(
    worker: &mut S,
    since_start: Duration,
    connections: &[Connection],
    write_tracker: &TaskTracker,
) -> anyhow::Result<Option<Duration>>
where
    S::Send: ReplicationSend,
{
    loop {
        match worker.proceed(since_start) {
            Proceed::Pending(tick_after) => {
                anyhow::ensure!(tick_after.is_some(), "workload halted without output");
                return Ok(tick_after);
            }
            Proceed::Send(send) => send.apply(connections, write_tracker)?,
            Proceed::Output(output) => {
                output?;
                return Ok(None);
            }
        }
    }
}
