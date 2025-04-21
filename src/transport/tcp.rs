use std::collections::HashMap;

use bincode::{Decode, error::DecodeError};
use tokio::{
    io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc::{self, Receiver, Sender},
    time::sleep,
};
use tokio_util::bytes::Bytes;

use crate::common::{ClientId, ReplicaId};

use super::{ReplicaConfig, ServiceConfig, Transport, TransportAndSenders};

async fn read_task<M: Decode<()> + Send + Sync + 'static>(
    mut ingress: impl AsyncRead + Unpin,
    read_sender: Sender<M>,
) -> anyhow::Result<()> {
    let mut decode_bytes = vec![0; 1 << 16];
    let mut bytes_len = 0;
    loop {
        let len = ingress.read(&mut decode_bytes[bytes_len..]).await?;
        anyhow::ensure!(len != 0);
        bytes_len += len;
        let mut offset = 0;
        let mut range;
        while {
            range = offset..bytes_len;
            !range.is_empty()
        } {
            match bincode::decode_from_slice(
                &decode_bytes[range.clone()],
                bincode::config::standard(),
            ) {
                Ok((message, num_bytes)) => {
                    read_sender.send(message).await?;
                    offset += num_bytes // assert offset not > bytes_len?
                }
                // expect to be rare and performance does not matter
                Err(DecodeError::UnexpectedEnd { additional }) => {
                    tracing::warn!(?range, additional, "read partial message");
                    break;
                }
                Err(err) => anyhow::bail!(err),
            }
        }
        bytes_len = range.len();
        decode_bytes.copy_within(range, 0)
    }
}

async fn write_task(
    mut egress: impl AsyncWrite + Unpin,
    mut write_receiver: Receiver<Bytes>,
) -> anyhow::Result<()> {
    while let Some(bytes) = write_receiver.recv().await {
        egress.write_all(&bytes).await?
    }
    Ok(())
}

impl Transport {
    pub fn add_tcp_connection<RM: Decode<()> + Send + Sync + 'static>(
        &mut self,
        connection: TcpStream,
        read_sender: Sender<RM>,
        write_receiver: Receiver<Bytes>,
    ) {
        let (read_half, write_half) = connection.into_split();
        self.read_tasks.spawn(read_task(read_half, read_sender));
        self.write_tasks
            .spawn(write_task(write_half, write_receiver));
    }

    pub fn add_tcp_read_connection<RM: Decode<()> + Send + Sync + 'static>(
        &mut self,
        connection: TcpStream,
        read_sender: Sender<RM>,
    ) {
        self.read_tasks.spawn(read_task(connection, read_sender));
    }

    pub fn add_tcp_write_connection(
        &mut self,
        connection: TcpStream,
        write_receiver: Receiver<Bytes>,
    ) {
        self.write_tasks
            .spawn(write_task(connection, write_receiver));
    }
}

pub async fn boot_client<M: Decode<()> + Send + Sync + 'static>(
    id: ClientId,
    service_config: ServiceConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<TransportAndSenders> {
    let mut transport = Transport::new();
    let mut write_senders = HashMap::new();
    for (&replica_id, &addr) in &service_config.server_external_addresses {
        let mut connection = TcpStream::connect(addr).await?;
        connection.write_all(&id.to_le_bytes()).await?;
        let (write_sender, write_receiver) = mpsc::channel(100);
        transport.add_tcp_connection(connection, message_sender.clone(), write_receiver);
        write_senders.insert(replica_id, write_sender);
    }
    Ok((transport, write_senders))
}

// unlike QUIC, TCP transport use dual socket style interconnect. interconnect
// streams are unidirectional, sending from ephemeral addresses to server
// internal addresses. not sure about the exact implications of this
// the rationale is to avoid TIME_WAIT connections from previous round of
// benchmark to interfere with the following round during a rapid
// debugging/tuning develop cycle
// takeaway: TCP is not the best transport solution to work with when developing
// research prototypes. will not try to extensively tune it in this codebase and
// primarily (if not exclusively) use QUIC
pub async fn boot_replica<M: Decode<()> + Send + Sync + 'static>(
    replica_id: ReplicaId,
    config: ReplicaConfig,
    message_sender: Sender<M>,
) -> anyhow::Result<TransportAndSenders> {
    let active_task = async {
        tracing::info!(
            "start server interconnect after {:?}",
            config.server_interconnect_delay
        );
        sleep(config.server_interconnect_delay).await;
        let mut connections = HashMap::new();
        for (&i, &addr) in &config.server_internal_addresses {
            if i == replica_id {
                continue;
            }
            let mut connection = TcpStream::connect(addr).await?;
            connection.write_all(&replica_id.to_le_bytes()).await?;
            connections.insert(i as ReplicaId, connection);
        }
        anyhow::Ok(connections)
    };
    let internal_listener =
        TcpListener::bind(config.server_internal_addresses[&replica_id]).await?;
    tracing::info!(addr = ?internal_listener.local_addr(), "start listening");
    let passive_task = async {
        let mut connections = HashMap::new();
        for _ in 0..config.server_internal_addresses.len() - 1 {
            // the peer address here usually can be used for lookup, but in proxied
            // environment like AWS VPC it fails
            let (mut connection, _) = internal_listener.accept().await?;
            let mut replica_id = [0; size_of::<ReplicaId>()];
            connection.read_exact(&mut replica_id).await?;
            connections.insert(ReplicaId::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (egress_connections, ingress_connections) = tokio::try_join!(active_task, passive_task)?;
    let mut transport = Transport::new();
    for connection in ingress_connections.into_values() {
        transport.add_tcp_read_connection(connection, message_sender.clone())
    }
    let mut write_senders = HashMap::new();
    for (replica_id, connection) in egress_connections {
        let (write_sender, write_receiver) = mpsc::channel(100);
        transport.add_tcp_write_connection(connection, write_receiver);
        write_senders.insert(replica_id, write_sender);
    }
    Ok((transport, write_senders))
}
