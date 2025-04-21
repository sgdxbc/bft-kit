use std::{collections::HashMap, net::SocketAddr};

use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
};

use crate::{
    common::{ClientId, ReplicaId},
    pbft::{Command, Replica, Spec, message},
    transport::{
        ClientConfig, ReplicaTask, ServiceConfig, Transport,
        tcp::{boot_client, boot_replica},
    },
    workload::{ConcurrentClients, Invoke, Latencies},
};

use super::{Finalized, TaskConfig};

pub async fn client_task(
    spec: Spec,
    config: ClientConfig,
    service_config: ServiceConfig,
    id: ClientId,
    invoke_receiver: Receiver<Invoke>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Latencies> {
    let (message_sender, message_receiver) = mpsc::channel(64);
    let (transport, replica_egresses) = boot_client(id, service_config, message_sender).await?;
    super::client_task_internal(
        spec,
        config,
        id,
        invoke_receiver,
        commit_sender,
        message_receiver,
        transport,
        replica_egresses,
    )
    .await
}

pub async fn clients_task(spec: Spec, config: TaskConfig) -> anyhow::Result<Vec<Latencies>> {
    let mut concurrent_clients = ConcurrentClients::new();
    for _ in 0..config.num_client {
        concurrent_clients.spawn(|id, invoke_receiver, commit_sender| {
            client_task(
                spec.clone(),
                config.client.clone(),
                config.service.clone(),
                id,
                invoke_receiver,
                commit_sender,
            )
        })
    }
    match config.client {
        ClientConfig::CloseLoop => concurrent_clients.close_loop(config.client_duration).await,
        ClientConfig::OpenLoop(client_config) => {
            concurrent_clients
                .open_loop(config.client_duration, client_config.sending_rate)
                .await
        }
    }
}

pub async fn server_task(replica: Replica, config: TaskConfig) -> anyhow::Result<()> {
    let (request_sender, request_receiver) = mpsc::channel(100);
    let (finalize_sender, finalize_receiver) = mpsc::channel(100);

    let replica_id = replica.core.config.id;
    let service_task = service_task(
        replica_id,
        config.service.clone(),
        request_sender,
        finalize_receiver,
    );
    let replica_task = replica_task(replica, config, request_receiver, finalize_sender);
    tokio::try_join!(service_task, replica_task)?;
    unreachable!()
}

async fn replica_task(
    replica: Replica,
    config: TaskConfig,
    request_receiver: Receiver<Command>,
    finalized_sender: Sender<Finalized>,
) -> anyhow::Result<()> {
    let replica_id = replica.core.config.id;
    let mut task = ReplicaTask::new(replica, finalized_sender);
    let transport;
    let (message_sender, message_receiver) = mpsc::channel(100);
    (transport, task.write_senders) =
        boot_replica(replica_id, config.replica, message_sender).await?;
    tracing::info!("replica ready");

    task.run_internal(
        config.tick_interval,
        request_receiver,
        message_receiver,
        transport,
    )
    .await
}

async fn service_task(
    replica_id: ReplicaId,
    config: ServiceConfig,
    request_sender: Sender<Command>,
    mut finalize_receiver: Receiver<Finalized>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_listener =
        TcpListener::bind(config.server_external_addresses[&replica_id]).await?;
    let (message_sender, mut message_receiver) = mpsc::channel(4096);
    let mut transport = Transport::new();
    let mut write_senders = HashMap::new();
    loop {
        enum Select {
            Accept((TcpStream, SocketAddr)),
            Message(Option<Command>),
            Finalize(Option<Finalized>),
            TransportJoinNext(anyhow::Result<()>),
        }
        use Select::{Accept, Message, TransportJoinNext};
        match tokio::select! {
            accept = external_listener.accept() => Accept(accept?),
            message = message_receiver.recv() => Message(message),
            finalize = finalize_receiver.recv() => Select::Finalize(finalize),
            result = transport.join_next() => TransportJoinNext(result),
        } {
            TransportJoinNext(Ok(())) | Message(None) => unreachable!(),
            TransportJoinNext(Err(err)) => {
                // sometimes it's connection reset, suppress it
                tracing::warn!(%err, "client read task returned")
            }
            Accept((mut connection, _)) => {
                let mut client_id = [0; size_of::<ClientId>()];
                connection.read_exact(&mut client_id).await?;
                let client_id = ClientId::from_le_bytes(client_id);
                tracing::debug!(%client_id, "accept client connection");
                let (write_sender, write_receiver) = mpsc::channel(100);
                transport.add_tcp_connection(connection, message_sender.clone(), write_receiver);
                let replaced = write_senders.insert(client_id, write_sender);
                anyhow::ensure!(replaced.is_none());
            }
            Message(Some(command)) => match replies.get(&command.client_id) {
                Some(reply) if reply.seq > command.seq => {}
                Some(reply) if reply.seq == command.seq => {
                    let egress = write_senders.get(&command.client_id);
                    anyhow::ensure!(
                        egress.is_some(),
                        "send to unexpected client id {}",
                        command.client_id
                    );
                    Transport::write(reply, egress).await?
                }
                _ => request_sender.send(command).await?,
            },
            Select::Finalize(finalize) => 'finalize: {
                let Some(finalize) = finalize else {
                    tracing::warn!("finalize channel closed");
                    break 'finalize;
                };
                for command in finalize.commands {
                    let reply = message::Reply {
                        seq: command.seq,
                        view_num: finalize.view_num,
                        // a 0/0 service, extend to support arbitrary state machine later
                        result: Default::default(),
                        replica_id,
                    };
                    let replaced = replies.insert(command.client_id, reply.clone());
                    assert!(replaced.map(|reply| reply.seq) < Some(reply.seq));
                    let egress = write_senders.get(&command.client_id);
                    anyhow::ensure!(
                        egress.is_some(),
                        "send to unexpected client id {}",
                        command.client_id
                    );
                    Transport::write(reply, egress).await?
                }
            }
        }
    }
}
