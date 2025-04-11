use std::{collections::HashMap, net::SocketAddr};

use bincode::{Decode, Encode, error::DecodeError};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::sleep,
    try_join,
};

use crate::common::{ClientId, ReplicaId, transport::BootServerConfig};

use crate::pbft::{Client, ClientAction, ToReplica, message};

use super::{AbstractEgress, AbstractServer, Finalize, TaskConfig};

async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    mut ingress: impl AsyncRead + Unpin,
    read_sender: Sender<M>,
    remote_close: bool,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    let mut offset = 0;
    loop {
        let len = ingress.read(&mut decode_bytes[offset..]).await?;
        if len == 0 {
            anyhow::ensure!(remote_close);
            break Ok(());
        }
        offset += len;
        match bincode::decode_from_slice(&decode_bytes[..offset], bincode::config::standard()) {
            Ok((message, num_bytes)) => {
                read_sender.send(message).await?;
                if num_bytes != offset {
                    decode_bytes.copy_within(num_bytes..offset, 0)
                }
                offset -= num_bytes
            }
            // expect to be rare and performance does not matter
            Err(DecodeError::UnexpectedEnd { additional }) => {
                tracing::warn!(offset, additional, "read partial message")
            }
            Err(err) => anyhow::bail!(err),
        }
    }
}

async fn write_message<W: AsyncWrite + Unpin>(
    message: impl Encode,
    egresses: impl IntoIterator<Item = W>,
    encode_bytes: &mut [u8],
) -> anyhow::Result<()> {
    let len = bincode::encode_into_slice(message, encode_bytes, bincode::config::standard())?;
    for mut egress in egresses {
        egress.write_all(&encode_bytes[..len]).await?
    }
    Ok(())
}

pub struct ClientTask {
    client: Client,
    config: TaskConfig,
    replica_ingress: Receiver<message::Reply>,
    read_tasks: JoinSet<anyhow::Result<()>>,
    replica_egresses: Vec<OwnedWriteHalf>,
    encode_bytes: Vec<u8>,
}

impl ClientTask {
    pub async fn init(client: Client, config: TaskConfig) -> anyhow::Result<Self> {
        let mut replica_egresses = Vec::new();
        let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
        let (read_sender, read_receiver) = mpsc::channel(64);
        for &addr in &config.replica_external_addresses {
            let (read_half, write_half) = TcpStream::connect(addr).await?.into_split();
            read_tasks.spawn(read_task(read_half, read_sender.clone(), false));
            replica_egresses.push(write_half);
        }
        for egress in &mut replica_egresses {
            egress.write_all(&client.config.id.to_le_bytes()).await?
        }

        Ok(Self {
            client,
            config,
            replica_ingress: read_receiver,
            read_tasks,
            replica_egresses,
            encode_bytes: vec![0; 1 << 16],
        })
    }

    pub async fn invoke(&mut self, op: Vec<u8>) -> anyhow::Result<Vec<u8>> {
        let mut action = self.client.invoke(op);
        loop {
            match action {
                ClientAction::Nop => {}
                ClientAction::SendToReplica(replica_id, message) => {
                    write_message(
                        message,
                        [&mut self.replica_egresses[replica_id as usize]],
                        &mut self.encode_bytes,
                    )
                    .await?
                }
                ClientAction::SendToAllReplicas(message) => {
                    write_message(message, &mut self.replica_egresses, &mut self.encode_bytes)
                        .await?
                }
                ClientAction::Return(result) => break Ok(result),
            }

            enum Select {
                Sleep,
                Read(Option<message::Reply>),
                Join(()),
            }
            use Select::*;
            action = match tokio::select! {
                () = sleep(self.config.client_tick_interval) => Sleep,
                reply = self.replica_ingress.recv() => Read(reply),
                Some(result) = self.read_tasks.join_next() => Join(result??),
            } {
                Sleep => self.client.tick(),
                Read(reply) => self
                    .client
                    .receive(reply.ok_or(anyhow::format_err!("unexpect read channel close"))?),
                Join(()) => unreachable!(),
            }
        }
    }
}

impl super::AbstractClientTask for ClientTask {
    fn init(client: Client, config: TaskConfig) -> impl Future<Output = anyhow::Result<Self>> {
        Self::init(client, config)
    }

    fn invoke(&mut self, op: Vec<u8>) -> impl Future<Output = anyhow::Result<Vec<u8>>> + Send {
        Self::invoke(self, op)
    }
}

// duplicating common::transport::boot_server
// currently no plan to extract this into common as well since no plan to have
// more protocols to support tcp
async fn boot_server(
    replica_id: ReplicaId,
    config: BootServerConfig,
    read_sender: Sender<ToReplica>,
) -> anyhow::Result<(JoinSet<anyhow::Result<()>>, HashMap<u8, OwnedWriteHalf>)> {
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    let new_socket = || {
        let socket = TcpSocket::new_v4()?;
        socket.set_reuseport(true)?;
        socket.bind(config.server_internal_addresses[replica_id as usize])?;
        anyhow::Ok(socket)
    };
    let active_task = async {
        sleep(config.server_interconnect_delay).await;
        let mut connections = HashMap::new();
        for (i, &addr) in config
            .server_internal_addresses
            .iter()
            .enumerate()
            .skip(replica_id as usize + 1)
        {
            let mut connection = new_socket()?.connect(addr).await?;
            connection.write_all(&replica_id.to_le_bytes()).await?;
            connections.insert(i as ReplicaId, connection);
        }
        anyhow::Ok(connections)
    };
    let internal_listener = new_socket()?.listen(100)?;
    let passive_task = async {
        let mut connections = HashMap::new();
        for _ in 0..replica_id {
            // the peer address here usually can be used for lookup, but in proxied
            // environment like AWS VPC it fails
            let (mut connection, _) = internal_listener.accept().await?;
            let mut replica_id = [0; size_of::<ReplicaId>()];
            connection.read_exact(&mut replica_id).await?;
            connections.insert(ReplicaId::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (mut connections, other_connections) = try_join!(active_task, passive_task)?;
    connections.extend(other_connections);
    anyhow::ensure!(connections.len() == config.server_internal_addresses.len() - 1);
    let mut replica_egresses = HashMap::new();
    for (replica_id, connection) in connections {
        let (read_half, write_half) = connection.into_split();
        read_tasks.spawn(read_task(
            read_half,
            read_sender.clone(),
            // this probably causes some `read_task` errors, as there has to be a replica
            // that is the one exits earliest and "remote close" other replicas' read tasks
            // will try some neat polling to prevent the error being _revealed_
            false,
        ));
        replica_egresses.insert(replica_id, write_half);
    }
    Ok((read_tasks, replica_egresses))
}

async fn service_task(
    replica_id: ReplicaId,
    config: TaskConfig,
    submit_sender: Sender<ToReplica>,
    mut finalize_receiver: Receiver<Finalize>,
) -> anyhow::Result<()> {
    let mut replies = HashMap::<ClientId, message::Reply>::new();
    let external_listener =
        TcpListener::bind(config.replica_external_addresses[replica_id as usize]).await?;
    let mut read_tasks = JoinSet::new();
    let mut client_egresses = HashMap::new();
    let mut encode_bytes = vec![0; 1 << 16];
    let (client_read_sender, mut client_read_receiver) = mpsc::channel(4096);
    loop {
        enum Select {
            Accept((TcpStream, SocketAddr)),
            Read(Option<ToReplica>),
            Finalize(Option<Finalize>),
            Join(()),
        }
        use Select::{Accept, Join, Read};
        match tokio::select! {
            accept = external_listener.accept() => Accept(accept?),
            message = client_read_receiver.recv() => Read(message),
            finalize = finalize_receiver.recv() => Select::Finalize(finalize),
            Some(result) = read_tasks.join_next() => Join(result??),
        } {
            Accept((mut connection, _)) => {
                let mut client_id = [0; size_of::<ClientId>()];
                connection.read_exact(&mut client_id).await?;
                let client_id = ClientId::from_le_bytes(client_id);
                tracing::debug!(%client_id, "accept client connection");
                let (read_half, write_half) = connection.into_split();
                read_tasks.spawn(read_task(read_half, client_read_sender.clone(), true));
                let replaced = client_egresses.insert(client_id, write_half);
                anyhow::ensure!(replaced.is_none());
            }
            Read(message) => {
                let Some(ToReplica::Request(command)) = message else {
                    unimplemented!()
                };
                match replies.get(&command.client_id) {
                    Some(reply) if reply.seq > command.seq => {}
                    Some(reply) if reply.seq == command.seq => {
                        let egress = client_egresses.get_mut(&command.client_id).ok_or(
                            anyhow::format_err!(
                                "send to unexpected client id {}",
                                command.client_id
                            ),
                        )?;
                        write_message(reply.clone(), [egress], &mut encode_bytes).await?
                    }
                    _ => submit_sender.send(ToReplica::Request(command)).await?,
                }
            }
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
                    let egress =
                        client_egresses
                            .get_mut(&command.client_id)
                            .ok_or(anyhow::format_err!(
                                "send to unexpected client {}",
                                command.client_id
                            ))?;
                    if let Err(err) = write_message(reply, [egress], &mut encode_bytes).await {
                        tracing::info!(%err, "egress to client failed")
                        // not removing from egress table to prevent the following
                        // (failed) writing errors
                        // may cause repeatedly logging but the pattern should be rare
                    }
                }
            }
            Join(()) => {}
        }
    }
}

pub struct Server;
impl AbstractServer for Server {
    type Egress = OwnedWriteHalf;

    fn boot_server(
        replica_id: ReplicaId,
        config: TaskConfig,
        read_sender: Sender<ToReplica>,
    ) -> impl Future<
        Output = anyhow::Result<(
            JoinSet<anyhow::Result<()>>,
            HashMap<ReplicaId, Self::Egress>,
        )>,
    > {
        boot_server(replica_id, config.boot_server, read_sender)
    }

    fn service_task(
        replica_id: ReplicaId,
        config: TaskConfig,
        submit_sender: Sender<ToReplica>,
        finalize_receiver: Receiver<Finalize>,
    ) -> impl Future<Output = anyhow::Result<()>> {
        service_task(replica_id, config, submit_sender, finalize_receiver)
    }

    fn into_egress(egress: &mut Self::Egress) -> impl AbstractEgress {
        egress
    }
}

impl AbstractEgress for &'_ mut OwnedWriteHalf {
    async fn write_bytes(self, encode_bytes: &[u8]) -> anyhow::Result<()> {
        self.write_all(encode_bytes).await?;
        Ok(())
    }
}

// cspell:enableCompoundWords
