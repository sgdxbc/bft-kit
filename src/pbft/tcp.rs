use std::{net::SocketAddr, time::Duration};

use bincode::error::DecodeError;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, tcp::OwnedWriteHalf},
    sync::mpsc::{self, Receiver},
    task::JoinSet,
    time::sleep,
};

use super::{Client, ClientAction};

pub struct TaskConfig {
    pub replica_addresses: Vec<SocketAddr>,
    pub tick_interval: Duration,
}

pub struct ClientTask {
    client: Client,
    config: TaskConfig,
    replica_ingress: Receiver<super::message::Reply>,
    read_tasks: JoinSet<anyhow::Result<()>>,
    replica_egresses: Vec<OwnedWriteHalf>,
    encode_bytes: Vec<u8>,
}

impl ClientTask {
    pub async fn init(client: Client, config: TaskConfig) -> anyhow::Result<Self> {
        let mut replica_egresses = Vec::new();
        let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
        let (read_sender, read_receiver) = mpsc::channel(64);
        for &addr in &config.replica_addresses {
            let (mut read_half, write_half) = TcpStream::connect(addr).await?.into_split();
            replica_egresses.push(write_half);
            let read_sender = read_sender.clone();
            read_tasks.spawn(async move {
                let mut decode_bytes = vec![0; 1 << 16];
                let mut offset = 0;
                loop {
                    offset += read_half.read(&mut decode_bytes[offset..]).await?;
                    match bincode::decode_from_slice(
                        &decode_bytes[..offset],
                        bincode::config::standard(),
                    ) {
                        Ok((message, num_bytes)) => {
                            read_sender.send(message).await?;
                            if num_bytes != offset {
                                decode_bytes.copy_within(num_bytes..offset, 0)
                            }
                            offset -= num_bytes
                        }
                        // expect to be rare and performance does not matter
                        Err(DecodeError::UnexpectedEnd { .. }) => {}
                        Err(err) => anyhow::bail!(err),
                    }
                }
            });
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
            if let Some(result) = self.perform_action(action).await? {
                break Ok(result);
            }
            enum Select {
                Sleep,
                Read(Option<super::message::Reply>),
                Join(()),
            }
            use Select::*;
            action = match tokio::select! {
                () = sleep(self.config.tick_interval) => Sleep,
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

    async fn perform_action(
        &mut self,
        action: ClientAction,
    ) -> Result<Option<Vec<u8>>, anyhow::Error> {
        match action {
            ClientAction::Nop => {}
            ClientAction::SendToReplica(replica_id, message) => {
                let len = bincode::encode_into_slice(
                    message,
                    &mut self.encode_bytes,
                    bincode::config::standard(),
                )?;
                self.replica_egresses[replica_id as usize]
                    .write_all(&self.encode_bytes[..len])
                    .await?
            }
            ClientAction::SendToAllReplicas(message) => {
                let len = bincode::encode_into_slice(
                    message,
                    &mut self.encode_bytes,
                    bincode::config::standard(),
                )?;
                for connection in &mut self.replica_egresses {
                    connection.write_all(&self.encode_bytes[..len]).await?
                }
            }
            ClientAction::Return(result) => return Ok(Some(result)),
        }
        Ok(None)
    }
}
