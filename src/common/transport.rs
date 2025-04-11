use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError};
use tokio::sync::mpsc::Sender;

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
    fn write_bytes(self, encode_bytes: &[u8]) -> impl Future<Output = anyhow::Result<()>>;
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
