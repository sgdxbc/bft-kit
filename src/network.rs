use std::{collections::HashMap, net::SocketAddr};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint};
use rand::{Rng, rng};
use tokio::{
    select, spawn,
    sync::mpsc::{Receiver, Sender, channel},
    task::JoinHandle,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    crypto::cert::quinn::{client_config, server_config},
    replica::ReplicaIndex,
    task::TaskGroup,
};

pub struct Network<IM, OM> {
    group: TaskGroup,
    endpoint: Endpoint,
    connections: HashMap<u32, Connection>,

    static_connections: Option<StaticConnections>,
    tx_incoming_messages: Sender<IM>,
    rx_outgoing_messages: Receiver<(Dest, OM)>,

    tx_close: Sender<u32>,
    rx_close: Receiver<u32>,
}

pub struct StaticConnections {
    pub num_connection: usize,
    pub connected: CancellationToken,
}

pub enum Dest {
    Ids(Vec<u32>),
    All,
}

impl<IM: Decode<()> + Send + 'static, OM: Encode + Send + 'static> Network<IM, OM> {
    pub fn spawn_client(
        group: TaskGroup,
        tx_incoming_messages: Sender<IM>,
        rx_outgoing_messages: Receiver<(Dest, OM)>,
        replica_addrs: Vec<SocketAddr>,
        connected: CancellationToken,
    ) -> JoinHandle<()> {
        spawn(group.clone().wrap_fallible(Self::client(
            group,
            tx_incoming_messages,
            rx_outgoing_messages,
            replica_addrs,
            connected,
        )))
    }

    pub fn spawn_replica(
        group: TaskGroup,
        tx_incoming_messages: Sender<IM>,
        rx_outgoing_messages: Receiver<(Dest, OM)>,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        connected: CancellationToken,
    ) -> JoinHandle<()> {
        spawn(group.clone().wrap_fallible(Self::replica(
            group,
            tx_incoming_messages,
            rx_outgoing_messages,
            replica_addrs,
            replica_index,
            connected,
        )))
    }

    fn new(
        group: TaskGroup,
        endpoint: Endpoint,
        static_connections: Option<StaticConnections>,
        tx_incoming_messages: Sender<IM>,
        rx_outgoing_messages: Receiver<(Dest, OM)>,
    ) -> Self {
        let (tx_close, rx_close) = channel(1);
        Self {
            group,
            endpoint,
            static_connections,
            connections: HashMap::new(),
            tx_incoming_messages,
            rx_outgoing_messages,
            tx_close,
            rx_close,
        }
    }

    async fn client(
        group: TaskGroup,
        tx_incoming_messages: Sender<IM>,
        rx_outgoing_messages: Receiver<(Dest, OM)>,
        replica_addrs: Vec<SocketAddr>,
        connected: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
        endpoint.set_default_client_config(client_config());
        let static_connections = StaticConnections {
            num_connection: replica_addrs.len(),
            connected,
        };
        let mut network = Self::new(
            group,
            endpoint,
            Some(static_connections),
            tx_incoming_messages,
            rx_outgoing_messages,
        );
        let id = rng().random_range(1 << 10..u32::MAX);
        network
            .start(
                replica_addrs
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (addr, i as _)),
                id,
            )
            .await?;
        network.run().await
    }

    async fn replica(
        group: TaskGroup,
        tx_incoming_messages: Sender<IM>,
        rx_outgoing_messages: Receiver<(Dest, OM)>,
        replica_addrs: Vec<SocketAddr>,
        replica_index: ReplicaIndex,
        connected: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut endpoint =
            Endpoint::server(server_config(), replica_addrs[replica_index as usize])?;
        endpoint.set_default_client_config(client_config());
        let static_connections = StaticConnections {
            num_connection: replica_addrs.len() - 1,
            connected,
        };
        let mut network = Self::new(
            group,
            endpoint,
            Some(static_connections),
            tx_incoming_messages,
            rx_outgoing_messages,
        );
        network
            .start(
                replica_addrs
                    .into_iter()
                    .enumerate()
                    .filter(move |&(i, _)| i != replica_index as usize)
                    .map(|(i, addr)| (addr, i as _))
                    .take(replica_index as _),
                replica_index as _,
            )
            .await?;
        network.run().await
    }

    async fn start(
        &mut self,
        replicas: impl Iterator<Item = (SocketAddr, u32)> + Send + 'static,
        id: u32,
    ) -> anyhow::Result<()> {
        for (addr, remote_id) in replicas {
            self.connect(addr, remote_id, id).await?;
        }
        Ok(())
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        loop {
            enum Event<A, O> {
                Accept(A),
                Close(u32),
                Outgoing(O),
            }
            match select! {
                Some(incoming) = self.endpoint.accept() => Event::Accept(incoming),
                Some(id) = self.rx_close.recv() => Event::Close(id),
                Some(outgoing) = self.rx_outgoing_messages.recv() => Event::Outgoing(outgoing),
            } {
                Event::Accept(incoming) => {
                    let connection = incoming.await?;
                    let mut remote_id = [0; 4];
                    connection
                        .accept_uni()
                        .await?
                        .read_exact(&mut remote_id)
                        .await?;
                    let remote_id = u32::from_le_bytes(remote_id);
                    tracing::debug!("accepted connection from {remote_id}");

                    self.spawn_read_loop(connection.clone(), remote_id, self.tx_close.clone());
                    self.connections.insert(remote_id, connection);
                    if let Some(static_connections) = &self.static_connections {
                        if self.connections.len() == static_connections.num_connection {
                            static_connections.connected.cancel()
                        }
                    }
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
        if let Some(static_connections) = &self.static_connections {
            if self.connections.len() == static_connections.num_connection {
                static_connections.connected.cancel()
            }
        }
        Ok(())
    }

    fn spawn_read_loop(
        &self,
        connection: Connection,
        remote_id: u32,
        tx_close: Sender<u32>,
    ) -> JoinHandle<()> {
        let read_loop = self.group.clone().wrap_fallible(Self::read_loop(
            connection,
            self.tx_incoming_messages.clone(),
            self.group.clone(),
        ));
        spawn(async move {
            read_loop.await;
            let _ = tx_close.send(remote_id).await;
        })
    }

    async fn read_loop(
        connection: Connection,
        tx_incoming_messages: Sender<IM>,
        group: TaskGroup,
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
            spawn(group.clone().wrap_fallible(async move {
                let bytes = stream.read_to_end(64 << 20).await?;
                let (message, len) =
                    bincode::decode_from_slice(&bytes, bincode::config::standard())?;
                anyhow::ensure!(len == bytes.len(), "Invalid message length");
                let _ = tx_incoming_messages.send(message).await;
                anyhow::Ok(())
            }));
        }
        Ok(())
    }

    async fn write_bytes(connection: Connection, bytes: Bytes) -> anyhow::Result<()> {
        connection.open_uni().await?.write_all(&bytes).await?;
        Ok(())
    }

    fn spawn_write_bytes(&self, connection: Connection, bytes: Bytes) -> JoinHandle<()> {
        spawn(
            self.group
                .clone()
                .wrap_fallible(Self::write_bytes(connection, bytes)),
        )
    }
}
