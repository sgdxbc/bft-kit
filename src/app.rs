use std::{collections::HashMap, mem::take};

use bincode::{Decode, Encode};
use tokio::{
    select, spawn,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::JoinHandle,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    crypto::{DigestHash, UpdateHash},
    replica::{Reply, Request},
    storage::{Bump, StorageKey, StorageOp, StorageRes},
};

pub enum StateOp<K, V> {
    Put(K, V),
    Get(K, oneshot::Sender<Option<V>>),
    Delete(K),
}

pub trait AppProtocolTypeConfig {
    type Op;
    type Res;
}

pub trait AppTypeConfig: AppProtocolTypeConfig {
    type Key;
    type Value;
}

pub struct AppRunner<C: AppTypeConfig> {
    forward_count: usize,
    inserts: HashMap<StorageKey, Bytes>,
    deletes: Vec<StorageKey>,

    rx_execute_request: Receiver<(Request, oneshot::Sender<Reply>)>,
    tx_execute_op: Sender<(C::Op, oneshot::Sender<C::Res>)>,
    rx_state_op: Receiver<StateOp<C::Key, C::Value>>,
    tx_storage_op: Sender<StorageOp>,
}

impl<C: AppTypeConfig + 'static> AppRunner<C>
where
    C::Op: Send + 'static + Decode<()>,
    C::Res: Send + 'static + Encode,
    C::Key: Send + 'static + UpdateHash,
    C::Value: Send + 'static + Encode + Decode<()>,
{
    pub fn spawn(
        cancel: CancellationToken,
        rx_execute_request: Receiver<(Request, oneshot::Sender<Reply>)>,
        tx_execute_op: Sender<(C::Op, oneshot::Sender<C::Res>)>,
        rx_state_op: Receiver<StateOp<C::Key, C::Value>>,
        tx_storage_op: Sender<StorageOp>,
    ) -> JoinHandle<()> {
        let mut app = Self {
            forward_count: 0,
            inserts: Default::default(),
            deletes: Default::default(),

            rx_execute_request,
            tx_execute_op,
            rx_state_op,
            tx_storage_op,
        };
        spawn(async move {
            if let Some(Err(err)) = cancel.run_until_cancelled(app.run()).await {
                tracing::error!(%err);
                cancel.cancel()
            }
        })
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some((request, tx_reply)) = self.rx_execute_request.recv().await {
            self.handle_request(request, tx_reply).await?;
        }
        Ok(())
    }

    async fn handle_request(
        &mut self,
        request: Request,
        tx_reply: oneshot::Sender<Reply>,
    ) -> anyhow::Result<()> {
        if self.forward_count > 0 {
            self.forward_count -= 1;
            return Ok(());
        }

        let (op, len) = bincode::decode_from_slice(&request.op, bincode::config::standard())?;
        anyhow::ensure!(len == request.op.len(), "Invalid operation length");
        let (tx_res, mut rx_res) = oneshot::channel();
        let _ = self.tx_execute_op.send((op, tx_res)).await;

        let res = loop {
            enum Event<R, O> {
                AppRes(R),
                AppStorageOp(O),
            }
            match select! {
                Ok(res) = &mut rx_res => Event::AppRes(res),
                Some(op) = self.rx_state_op.recv() => Event::AppStorageOp(op),
            } {
                Event::AppRes(res) => break res,
                Event::AppStorageOp(op) => {
                    self.handle_state_op(op).await?;
                    if self.forward_count > 0 {
                        self.forward_count -= 1;
                        return Ok(());
                    }
                }
            }
        };

        let reply = Reply {
            client_seq: request.client_seq,
            res: bincode::encode_to_vec(res, bincode::config::standard())?,
        };
        let _ = tx_reply.send(reply);

        let bump = Bump {
            inserts: take(&mut self.inserts),
            deletes: take(&mut self.deletes),
        };
        let (tx_ok, rx_ok) = oneshot::channel();
        let _ = self.tx_storage_op.send(StorageOp::Bump(bump, tx_ok)).await;
        match rx_ok.await {
            Ok(StorageRes::Ok(())) => {}
            Ok(StorageRes::Forward(count)) => self.forward_count = count,
            Err(_) => {}
        }
        Ok(())
    }

    async fn handle_state_op(
        &mut self,
        op: StateOp<C::Key, C::Value>,
    ) -> Result<(), anyhow::Error> {
        match op {
            StateOp::Put(key, value) => {
                self.inserts.insert(
                    key.digest().0.into(),
                    bincode::encode_to_vec(value, bincode::config::standard())?.into(),
                );
            }
            StateOp::Delete(key) => self.deletes.push(key.digest().0.into()),
            // TODO concurrent get
            StateOp::Get(key, tx_value) => {
                let (tx_bytes, rx_bytes) = oneshot::channel();
                let _ = self
                    .tx_storage_op
                    .send(StorageOp::Fetch(key.digest().0.into(), tx_bytes))
                    .await;
                match rx_bytes.await {
                    Ok(StorageRes::Ok(Some(bytes))) => {
                        let (value, len) =
                            bincode::decode_from_slice(&bytes, bincode::config::standard())?;
                        anyhow::ensure!(len == bytes.len(), "Invalid value length");
                        let _ = tx_value.send(Some(value));
                    }
                    Ok(StorageRes::Ok(None)) => {
                        let _ = tx_value.send(None);
                    }
                    Ok(StorageRes::Forward(count)) => self.forward_count = count,
                    Err(_) => {}
                }
            }
        }
        Ok(())
    }
}
