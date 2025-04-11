use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint};
use tokio::{sync::mpsc::Sender, task::JoinSet, time::sleep, try_join};

use crate::crypto::cert::quinn::{client_config, server_config};

use super::ReplicaId;

pub async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    ingress: Connection,
    read_sender: Sender<M>,
    remote_close: bool,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    loop {
        let mut stream = match ingress.accept_uni().await {
            Ok(stream) => stream,
            Err(ConnectionError::ApplicationClosed(_)) => {
                anyhow::ensure!(remote_close);
                tracing::debug!("remote closed");
                return Ok(());
            }
            Err(err) => anyhow::bail!(err),
        };
        let mut offset = 0;
        while let Some(len) = stream.read(&mut decode_bytes).await? {
            offset += len
        }
        let (message, len) =
            bincode::decode_from_slice(&decode_bytes[..offset], bincode::config::standard())?;
        anyhow::ensure!(len == offset); //
        read_sender.send(message).await?;
    }
}

pub struct WriteMessage {
    encode_bytes: Vec<u8>,
}

pub trait AbstractEgress {
    fn write_bytes(self, encode_bytes: &[u8]) -> impl Future<Output = anyhow::Result<()>> + Send;
}

impl WriteMessage {
    pub fn new() -> Self {
        Self {
            encode_bytes: vec![0; 1 << 16],
        }
    }

    pub async fn run<C: AbstractEgress>(
        &mut self,
        message: impl Encode,
        egresses: impl IntoIterator<Item = C>,
    ) -> anyhow::Result<()> {
        let len = bincode::encode_into_slice(
            message,
            &mut self.encode_bytes,
            bincode::config::standard(),
        )?;
        for egress in egresses {
            egress.write_bytes(&self.encode_bytes[..len]).await?
        }
        Ok(())
    }
}

impl Default for WriteMessage {
    fn default() -> Self {
        Self::new()
    }
}

impl AbstractEgress for &'_ Connection {
    async fn write_bytes(self, encode_bytes: &[u8]) -> anyhow::Result<()> {
        self.open_uni().await?.write_all(encode_bytes).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct BootServerConfig {
    pub server_internal_addresses: Vec<SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. set longer in higher latency environments (or human
    // action is involved)
    pub server_interconnect_delay: Duration,
}

pub async fn boot_server<M: Decode<()> + Send + Sync + 'static>(
    replica_id: ReplicaId,
    config: BootServerConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<(JoinSet<anyhow::Result<()>>, HashMap<ReplicaId, Connection>)> {
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(None);
    let transport = Arc::new(transport);
    let mut internal_endpoint = Endpoint::server(
        // server_config(),
        {
            let mut config = server_config();
            config.transport_config(transport.clone());
            config
        },
        config.server_internal_addresses[replica_id as usize],
    )?;
    internal_endpoint.set_default_client_config({
        let mut config = client_config();
        config.transport_config(transport);
        config
    });
    let active_task = async {
        sleep(config.server_interconnect_delay).await;
        let mut connections = HashMap::new();
        for (i, &addr) in config
            .server_internal_addresses
            .iter()
            .enumerate()
            .skip(replica_id as usize + 1)
        {
            let connection = internal_endpoint.connect(addr, "server.example")?.await?;
            connection
                .open_uni()
                .await?
                // to need to `to_le_bytes()` for current u8 based ReplicaId, just future proof
                .write_all(&replica_id.to_le_bytes())
                .await?;
            connections.insert(i as ReplicaId, connection);
        }
        anyhow::Ok(connections)
    };
    let passive_task = async {
        let mut connections = HashMap::new();
        for _ in 0..replica_id {
            let connection = internal_endpoint
                .accept()
                .await
                .expect("endpoint not closed")
                .await?;
            let mut replica_id = [0; size_of::<ReplicaId>()];
            connection
                .accept_uni()
                .await?
                .read_exact(&mut replica_id)
                .await?;
            connections.insert(ReplicaId::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (mut connections, other_connections) = try_join!(active_task, passive_task)?;
    connections.extend(other_connections);
    anyhow::ensure!(connections.len() == config.server_internal_addresses.len() - 1);
    let replica_egresses = connections;
    let mut read_tasks = JoinSet::<anyhow::Result<()>>::new();
    for connection in replica_egresses.values() {
        read_tasks.spawn(read_task(connection.clone(), message_sender.clone(), false));
    }
    Ok((read_tasks, replica_egresses))
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub server_external_addresses: Vec<SocketAddr>,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub num_max_concurrent: usize,
}
