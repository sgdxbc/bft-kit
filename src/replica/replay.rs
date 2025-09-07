use bincode::{Decode, Encode};
use tokio::{
    spawn,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};

use crate::{app::AppProtocolTypeConfig, task::TaskGroup};

use super::{Reply, Request};

pub struct Replay<C: AppProtocolTypeConfig> {
    client_seq: u64,

    rx_workload: Receiver<(C::Op, oneshot::Sender<C::Res>)>,
    tx_request: Sender<(Request, oneshot::Sender<Reply>)>,
}

impl<C: AppProtocolTypeConfig + 'static> Replay<C>
where
    C::Op: Encode + Send + 'static,
    C::Res: Decode<()> + Send + 'static,
{
    pub fn spawn(
        group: TaskGroup,
        rx_workload: Receiver<(C::Op, oneshot::Sender<C::Res>)>,
        tx_request: Sender<(Request, oneshot::Sender<Reply>)>,
    ) -> JoinHandle<()> {
        let mut replay = Self {
            client_seq: 0,
            rx_workload,
            tx_request,
        };
        spawn(group.wrap_fallible(async move { replay.run().await }))
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some((op, tx_res)) = self.rx_workload.recv().await {
            self.client_seq += 1;
            let request = Request {
                client_id: 0,
                client_seq: self.client_seq,
                op: bincode::encode_to_vec(&op, bincode::config::standard())?,
            };
            let (tx_reply, rx_reply) = oneshot::channel();
            let _ = self.tx_request.send((request, tx_reply)).await;
            let Ok(reply) = rx_reply.await else { break };
            let (res, len) = bincode::decode_from_slice(&reply.res, bincode::config::standard())?;
            anyhow::ensure!(len == reply.res.len(), "Invalid result length");
            let _ = tx_res.send(res);
        }
        Ok(())
    }
}
