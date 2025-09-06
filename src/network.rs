use std::{collections::HashMap, net::SocketAddr};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint};
use rand::Rng;
use tokio::{
    select, spawn,
    sync::mpsc::{Receiver, Sender, channel},
    task::JoinHandle,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

pub struct Network<IM, OM> {
    endpoint: Endpoint,
    connections: HashMap<u32, Connection>,

    cancel: CancellationToken,
    tx_incoming_messages: Sender<IM>,
    rx_outgoing_messages: Receiver<(Dest, OM)>,

    tx_close: Sender<u32>,
    rx_close: Receiver<u32>,
}

pub enum Dest {
    Ids(Vec<u32>),
    All,
}

impl<IM: Decode<()> + Send + 'static, OM: Encode + Send + 'static> Network<IM, OM> {
    pub fn new(
        endpoint: Endpoint,
        cancel: CancellationToken,
        tx_incoming_messages: Sender<IM>,
        rx_outgoing_messages: Receiver<(Dest, OM)>,
    ) -> Self {
        let (tx_close, rx_close) = channel(100);
        Self {
            endpoint,
            connections: HashMap::new(),
            cancel: cancel.clone(),
            tx_incoming_messages,
            rx_outgoing_messages,
            tx_close,
            rx_close,
        }
    }

    pub fn spawn_client(mut self, replicas: Vec<(SocketAddr, u32)>) -> JoinHandle<()> {
        let id = rand::rng().random_range(1 << 10..u32::MAX); // preserve lower ids for replicas
        let cancel = self.cancel.clone();
        let client = async move {
            for (addr, remote_id) in replicas {
                self.connect(addr, remote_id, id).await?;
            }
            self.run().await
        };
        spawn(async move {
            if let Err(err) = client.await {
                tracing::error!(%err);
                cancel.cancel()
            }
        })
    }

    pub fn spawn_replica(
        mut self,
        replicas: Vec<(SocketAddr, u32)>,
        self_index: usize,
    ) -> JoinHandle<()> {
        let cancel = self.cancel.clone();
        let replica = async move {
            // every replica actively connect to the other replicas with lower indexes to
            // form a single-connected full mesh
            for (addr, remote_id) in replicas.into_iter().take(self_index) {
                self.connect(addr, remote_id, self_index as _).await?
            }
            self.run().await
        };
        spawn(async move {
            if let Err(err) = replica.await {
                tracing::error!(%err);
                cancel.cancel()
            }
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<A, O> {
                Cancel,
                Accept(A),
                Close(u32),
                Outgoing(O),
            }
            match select! {
                () = self.cancel.cancelled() => Event::Cancel,
                Some(incoming) = self.endpoint.accept() => Event::Accept(incoming),
                Some(id) = self.rx_close.recv() => Event::Close(id),
                Some(outgoing) = self.rx_outgoing_messages.recv() => Event::Outgoing(outgoing),
            } {
                Event::Cancel => break,
                Event::Accept(incoming) => {
                    let connection = incoming.await?;
                    let mut remote_id = [0; 4];
                    connection
                        .accept_uni()
                        .await?
                        .read_exact(&mut remote_id)
                        .await?;
                    let remote_id = u32::from_le_bytes(remote_id);

                    self.spawn_read_loop(connection.clone(), remote_id, self.tx_close.clone());
                    self.connections.insert(remote_id, connection);
                }
                Event::Close(id) => {
                    self.connections.remove(&id);
                }
                Event::Outgoing((dest, msg)) => {
                    let bytes =
                        Bytes::from(bincode::encode_to_vec(msg, bincode::config::standard())?);
                    match dest {
                        Dest::Ids(ids) => {
                            for id in ids {
                                let Some(connection) = self.connections.get(&id) else {
                                    anyhow::bail!("No connection for id {id}")
                                };
                                self.spawn_write_bytes(connection.clone(), bytes.clone());
                            }
                        }
                        Dest::All => {
                            for connection in self.connections.values() {
                                self.spawn_write_bytes(connection.clone(), bytes.clone());
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn connect(
        &mut self,
        remote_addr: SocketAddr,
        remote_id: u32,
        self_id: u32,
    ) -> anyhow::Result<()> {
        let connection = self
            .endpoint
            .connect(remote_addr, "server.example")?
            .await?;
        connection
            .open_uni()
            .await?
            .write_all(&self_id.to_le_bytes())
            .await?;
        self.spawn_read_loop(connection.clone(), remote_id, self.tx_close.clone());
        self.connections.insert(remote_id, connection);
        Ok(())
    }

    fn spawn_read_loop(
        &self,
        connection: Connection,
        remote_id: u32,
        tx_close: Sender<u32>,
    ) -> JoinHandle<()> {
        let read_loop = Self::read_loop(
            connection,
            self.tx_incoming_messages.clone(),
            self.cancel.clone(),
        );
        let cancel = self.cancel.clone();
        spawn(async move {
            if let Err(err) = read_loop.await {
                tracing::error!(%err);
                cancel.cancel()
            }
            let _ = tx_close.send(remote_id).await;
        })
    }

    async fn read_loop(
        connection: Connection,
        tx_incoming_messages: Sender<IM>,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        loop {
            let mut stream = match connection.accept_uni().await {
                Ok(stream) => stream,
                Err(ConnectionError::ApplicationClosed(_) | ConnectionError::LocallyClosed) => {
                    break;
                }
                Err(err) => Err(anyhow::Error::new(err))?,
            };

            let tx_incoming_messages = tx_incoming_messages.clone();
            let read = async move {
                let bytes = stream.read_to_end(64 << 20).await?;
                let (message, len) =
                    bincode::decode_from_slice(&bytes, bincode::config::standard())?;
                anyhow::ensure!(len == bytes.len(), "Invalid message length");
                let _ = tx_incoming_messages.send(message).await;
                anyhow::Ok(())
            };

            let cancel = cancel.clone();
            spawn(async move {
                if let Err(err) = read.await {
                    tracing::error!(%err);
                    cancel.cancel()
                }
            });
        }
        Ok(())
    }

    async fn write_bytes(connection: Connection, bytes: Bytes) -> anyhow::Result<()> {
        connection.open_uni().await?.write_all(&bytes).await?;
        Ok(())
    }

    fn spawn_write_bytes(&self, connection: Connection, bytes: Bytes) -> JoinHandle<()> {
        let cancel = self.cancel.clone();
        spawn(async move {
            if let Err(err) = Self::write_bytes(connection, bytes).await {
                tracing::error!(%err);
                cancel.cancel()
            }
        })
    }
}
