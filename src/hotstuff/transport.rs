use std::{collections::HashMap, net::SocketAddr, time::Duration};

use quinn::Endpoint;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::Instant,
};

use crate::{
    common::{
        ClientId, Command, Quorum, ReplicaId,
        transport::{WriteMessage, read_task},
    },
    crypto::cert::quinn::client_config,
    hotstuff::ToReplica,
};

use super::{Spec, ToClient};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub spec: Spec,
    pub num_client: usize,
    pub client_duration: Duration,
    pub replica_tick_interval: Duration,
    pub replica_external_addresses: Vec<SocketAddr>,
    pub replica_internal_addresses: Vec<SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. set longer in higher latency environments (or human
    // action is involved)
    pub replica_connect_delay: Duration,
}

pub const WARMUP_DURATION: Duration = Duration::from_secs(1);

pub async fn client_task(
    config: TaskConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<(Vec<u8>, Option<Vec<u8>>)>,
    commit_sender: Sender<(ClientId, Duration)>,
) -> anyhow::Result<()> {
    let mut replica_egresses = Vec::new();
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    let (message_sender, mut message_receiver) = mpsc::channel(64);
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());
    for &addr in &config.replica_external_addresses {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        read_tasks.spawn(read_task(connection.clone(), message_sender.clone(), false));
        replica_egresses.push(connection);
    }
    for egress in &mut replica_egresses {
        egress
            .open_uni()
            .await?
            .write_all(&id.to_le_bytes())
            .await?;
    }
    let mut write_message = WriteMessage::new();

    let mut seq = 0;
    struct SeqScratch {
        results: Quorum<Vec<u8>>,
        expected_result: Option<Vec<u8>>,
        start: Instant,
    }
    let mut seq_scratch = HashMap::new();
    loop {
        enum Select {
            Invoke(Option<(Vec<u8>, Option<Vec<u8>>)>),
            Message(Option<ToClient>),
            JoinNext(()),
        }
        use Select::*;
        match tokio::select! {
            invoke = invoke_receiver.recv() => Invoke(invoke),
            message = message_receiver.recv() => Message(message),
            Some(result) = read_tasks.join_next() => JoinNext(result??)
        } {
            Invoke(None) => break Ok(()),
            Invoke(Some((op, result))) => {
                seq += 1;
                let command = Command {
                    client_id: id,
                    seq,
                    op,
                };
                write_message
                    .run(ToReplica::Request(command), &replica_egresses)
                    .await?;
                seq_scratch.insert(
                    seq,
                    SeqScratch {
                        results: Default::default(),
                        expected_result: result,
                        start: Instant::now(),
                    },
                );
            }
            Message(reply) => {
                let Some(reply) = reply else {
                    anyhow::bail!("message receive channel close")
                };
                let Some(scratch) = seq_scratch.get_mut(&reply.seq) else {
                    continue;
                };
                scratch
                    .results
                    .insert(reply.replica_id, reply.result.clone());
                if scratch
                    .results
                    .values()
                    .filter(|&result| result == &reply.result)
                    .count() as ReplicaId
                    == config.spec.num_faulty + 1
                {
                    let scratch = seq_scratch.remove(&reply.seq).unwrap();
                    if let Some(result) = scratch.expected_result {
                        anyhow::ensure!(reply.result == result)
                    }
                    commit_sender.send((id, scratch.start.elapsed())).await?
                }
            }
            JoinNext(()) => unreachable!(),
        }
    }
}
