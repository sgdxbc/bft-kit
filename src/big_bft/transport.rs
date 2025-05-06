#![allow(unused)]
use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    net::SocketAddr,
    pin::pin,
    time::Duration,
};

use hdrhistogram::Histogram;
use quinn::{Endpoint, TransportConfig};
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::{Instant, timeout_at},
};
use tokio_util::sync::CancellationToken;

use crate::{
    big_bft::DigestHash,
    common::{ClientId, ReplicaId},
    crypto::cert::quinn::server_config,
    transport::{ReplicaConfig, ServiceConfig, Transport, boot_client, boot_replica},
    workload::Latencies,
};

use super::{Spec, Txn, message, replica::Replica};

#[derive(Debug, Clone)]
pub struct TaskConfig {
    pub service: ServiceConfig,
    pub replica: ReplicaConfig,
    pub num_concurrent: usize,
    pub client_duration: Duration,
}

pub const WARMUP_DURATION: Duration = Duration::from_secs(1);

pub async fn client_task(
    spec: Spec,
    service_config: ServiceConfig,
    mut invoke_receiver: Receiver<Txn>,
    finish_sender: Sender<()>,
) -> anyhow::Result<Latencies> {
    let (message_sender, mut message_receiver) = mpsc::channel(1000);
    let (mut transport, replica_egresses) =
        boot_client(ClientId(0), service_config, message_sender).await?;

    let mut latencies = Histogram::new(3)?;
    let start = Instant::now();
    struct Scratch {
        start: Instant,
        hashes: HashMap<usize, DigestHash>,
    }
    let mut scratches = BTreeMap::new();
    loop {
        enum Select {
            Invoke(Option<Txn>),
            Message(Option<message::Reply>),
            TransportJoinNext(()),
        }
        match tokio::select! {
            invoke = invoke_receiver.recv() => Select::Invoke(invoke),
            message = message_receiver.recv() => Select::Message(message),
            result = transport.join_next() => Select::TransportJoinNext(result?)
        } {
            Select::TransportJoinNext(()) => unreachable!(),
            Select::Invoke(None) => break Ok(latencies),
            Select::Invoke(Some(txn)) => {
                let version = scratches
                    .last_key_value()
                    .map(|(&version, _)| version)
                    .unwrap_or(0);
                scratches.insert(
                    version + 1,
                    Scratch {
                        start: Instant::now(),
                        hashes: Default::default(),
                    },
                );
                Transport::write(txn, replica_egresses.values()).await?
            }
            Select::Message(None) => anyhow::bail!("message receive channel close"),
            Select::Message(Some(reply)) => {
                let Some(scratch) = scratches.get_mut(&reply.version) else {
                    continue;
                };
                scratch.hashes.insert(reply.replica_index, reply.hash);
                if scratch
                    .hashes
                    .values()
                    .filter(|&&hash| hash == reply.hash)
                    .count()
                    == spec.num_fault + 1
                {
                    let end = Instant::now();
                    if end.duration_since(start) >= WARMUP_DURATION {
                        latencies += end.duration_since(scratch.start).as_micros() as u64;
                    }
                    scratches.remove(&reply.version);
                    finish_sender.send(()).await?;
                }
            }
        }
    }
}

pub async fn close_loop_client_task(spec: Spec, config: TaskConfig) -> anyhow::Result<Latencies> {
    let (invoke_sender, invoke_receiver) = mpsc::channel(100);
    let (finish_sender, mut finish_receiver) = mpsc::channel(100);
    let mut client_task = pin!(client_task(
        spec,
        config.service,
        invoke_receiver,
        finish_sender
    ));
    for _ in 0..config.num_concurrent {
        invoke_sender.send(Txn(vec![])).await? // TODO
    }
    let deadline = Instant::now() + config.client_duration;
    enum Select {
        Finish(Option<()>),
        Join,
    }
    while let Ok(select) = {
        let task = async {
            anyhow::Ok(tokio::select! {
                commit = finish_receiver.recv() => Select::Finish(commit),
                result = &mut client_task => { result?; Select::Join },
            })
        };
        timeout_at(deadline, task).await
    } {
        match select? {
            Select::Join => unreachable!(),
            Select::Finish(None) => {
                client_task.await?;
                unreachable!()
            }
            Select::Finish(Some(())) => {
                invoke_sender
                    .send(Txn(vec![])) // TODO
                    .await?
            }
        };
    }
    drop(invoke_sender);
    client_task.await
}

pub async fn server_task(
    mut replica: Replica,
    config: TaskConfig,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let replica_id = replica.id();
    let external_endpoint = Endpoint::server(
        // server_config(),
        {
            let mut transport = TransportConfig::default();
            transport.max_idle_timeout(None);
            let mut config = server_config();
            config.transport_config(transport.into());
            config
        },
        config.service.server_external_addresses[&replica_id],
    )?;

    let (message_sender, mut message_receiver) = mpsc::channel(100);
    let (mut transport, write_senders) =
        boot_replica(replica_id, config.replica, message_sender).await?;
    tracing::info!("replica ready");

    // replica.core.init(initial_state)
    'client: loop {
        let Some(client_incoming) = external_endpoint.accept().await else {
            unreachable!("endpoint closed")
        };
        let client_connection = client_incoming.await?;
        let mut client_transport = Transport::new();
        let (execute_sender, mut execute_receiver) = mpsc::channel(100);
        let (reply_sender, reply_receiver) = mpsc::channel(100);
        client_transport.add_connection(client_connection, execute_sender, reply_receiver);

        let mut actions = Vec::new();
        loop {
            enum Select {
                Execute(Option<Txn>),
                Message(Option<super::replica::Message>),
                TransportJoinNext(()),
                ClientTransportJoinNext(anyhow::Result<()>),
                Cancel,
            }
            match tokio::select! {
                () = cancel.cancelled() => Select::Cancel,
                invoke = execute_receiver.recv() => Select::Execute(invoke),
                message = message_receiver.recv() => Select::Message(message),
                result = client_transport.join_next() => Select::ClientTransportJoinNext(result),
                result = transport.join_next() => Select::TransportJoinNext(result?),
            } {
                Select::TransportJoinNext(()) => unreachable!(),
                Select::Cancel => break 'client,
                Select::Execute(None) | Select::Message(None) => todo!(),
                Select::Execute(Some(txn)) => replica.execute(txn, &mut actions),
                Select::Message(Some(message)) => replica.receive(message, &mut actions),
                Select::ClientTransportJoinNext(result) => {
                    if let Err(err) = result {
                        tracing::info!(%err);
                        continue 'client;
                    }
                }
            }

            for action in actions.drain(..) {
                match action {
                    crate::big_bft::replica::ReplicaAction::SendToAll(message) => {
                        Transport::write(message, write_senders.values()).await?
                    }
                    crate::big_bft::replica::ReplicaAction::Executed(version, hash) => {
                        let reply = super::message::Reply {
                            version,
                            hash,
                            replica_index: replica_id as _,
                        };
                        Transport::write(reply, [&reply_sender]).await?
                    }
                }
            }
        }
    }
    Ok(())
}
