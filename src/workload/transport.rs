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
    replication::{ReplicaIndex, transport::ReplicaTable},
    service::ClientId,
    state::{Action, State},
    transport::{BINCODE_CONFIG, PerformSend, read_loop, trace_error},
};

use super::NanoLatencies;

pub async fn run_worker<S: State<Output = anyhow::Result<()>> + Into<NanoLatencies>>(
    mut worker: S,
    client_id: ClientId,
    addrs: Vec<SocketAddr>,
    cancel: CancellationToken,
) -> anyhow::Result<NanoLatencies>
where
    S::Message: Decode<()>,
    [Connection]: PerformSend<S::Send>,
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

    worker.tick(Duration::ZERO);
    let mut option_tick_at = worker_proceed(&mut worker, &connections, &write_tracker)?;
    let start = Instant::now();
    while let Some(tick_at) = option_tick_at {
        match select! {
            Some(event) = event_receiver.recv() => event,
            () = sleep(tick_at) => Event::Tick,
            () = cancel.cancelled() => break,
        } {
            Event::Message(bytes) => {
                let (message, len) = bincode::decode_from_slice(&bytes, BINCODE_CONFIG)?;
                anyhow::ensure!(len == bytes.len());
                worker.tick(start.elapsed());
                worker.receive(message)
            }
            Event::Tick => worker.tick(start.elapsed()),
        }
        option_tick_at = worker_proceed(&mut worker, &connections, &write_tracker)?
    }

    for (connection, task) in connections.into_iter().zip(read_tasks) {
        connection.close(0u32.into(), b"worker stopped");
        task.await.unwrap() // not cancelled anywhere and propagate panics
    }
    Ok(worker.into())
}

fn worker_proceed<S: State<Output = anyhow::Result<()>>>(
    worker: &mut S,
    connections: &[Connection],
    write_tracker: &TaskTracker,
) -> anyhow::Result<Option<Duration>>
where
    [Connection]: PerformSend<S::Send>,
{
    while let Some(actions) = worker.proceed() {
        for action in actions {
            match action {
                Action::Send(send) => connections.perform(send, write_tracker)?,
                Action::Output(output) => {
                    output?;
                    return Ok(None);
                }
            }
        }
    }
    let Some(tick_after) = worker.tick_after() else {
        anyhow::bail!("worker halted without output");
    };
    if tick_after == Duration::ZERO {
        tracing::warn!("zero interval tick detected, worker overloaded")
    }
    Ok(Some(tick_after))
}

impl ReplicaTable for [Connection] {
    fn get(&self, index: ReplicaIndex) -> Option<&Connection> {
        self.get(index as usize)
    }

    fn get_all(&self) -> impl Iterator<Item = &Connection> {
        self.iter()
    }
}
