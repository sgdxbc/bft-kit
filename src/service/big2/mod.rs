use std::collections::HashMap;

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    task::JoinSet,
};

use crate::{
    app::{AppProtocol, DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState},
    crypto::{DigestHash, UpdateHash},
    replication::Replicated,
    service::{ClientId, Reply, Request},
    transport::BINCODE_CONFIG,
};

use self::storage::StorageKey;

use super::ClientSeq;

pub mod storage;

pub async fn big_loop<A: DataShardingApp + 'static, RD: Clone>(
    endpoint: Endpoint,
    app: A,
    submit_sender: Sender<Request<A::Op>>,
    mut replicated_receiver: Receiver<Replicated<Request<A::Op>, RD>>,
    execute_request_sender: Sender<ExecuteRequest<A, RD>>,
) -> anyhow::Result<()>
where
    Request<A::Op>: Send + Decode<()> + 'static,
    Reply<A::Res, RD>: Send + Encode + Clone + 'static,
{
    enum Event<A, C, R> {
        Accept(Option<Box<A>>),
        Close(C),
        Replicated(R),
    }
    let mut client_loops = JoinSet::new();
    let mut client_seqs = HashMap::new();
    let mut client_reply_senders = HashMap::new();
    loop {
        match select! {
            connecting = endpoint.accept() => Event::Accept(connecting.map(Into::into)),
            replicated = replicated_receiver.recv() => Event::Replicated(replicated),
            // strict rule: fail to handle any client gracefully is considered as a fatal
            // error of the whole service
            // good for prototyping and reveal unnoticeable issues but probably not suitable
            // for production
            Some(client_id) = client_loops.join_next() => Event::Close(client_id??),
        } {
            Event::Accept(None) => {
                tracing::info!("endpoint closed; service shutting down");
                break;
            }
            Event::Accept(Some(connecting)) => {
                let connection = (*connecting).await?;
                let mut client_id = [0; size_of::<ClientId>()];
                connection
                    .accept_uni()
                    .await?
                    .read_exact(&mut client_id)
                    .await?;
                let client_id = ClientId::from_le_bytes(client_id);

                let (reply_sender, reply_receiver) = channel(100);
                let client_loop =
                    client_loop::<A, _>(connection, submit_sender.clone(), reply_receiver);
                client_loops.spawn(async move {
                    client_loop.await?;
                    anyhow::Ok(client_id)
                });
                client_reply_senders.insert(client_id, reply_sender);
            }
            Event::Close(client_id) => {
                client_reply_senders.remove(&client_id);
            }
            Event::Replicated(None) => {
                tracing::error!("replicated channel closed");
                break;
            }
            Event::Replicated(Some(replicated)) => {
                for request in replicated.logs {
                    if let Some(&seq) = client_seqs.get(&request.client_id)
                        && seq >= request.client_seq
                    {
                        tracing::warn!("ignoring out of order request");
                        continue;
                    }
                    client_seqs.insert(request.client_id, request.client_seq);
                    let execute_request = ExecuteRequest {
                        state: app.new_execute(request.op),
                        client_seq: request.client_seq,
                        metadata: replicated.metadata.clone(),
                        reply_sender: client_reply_senders.get(&request.client_id).cloned(),
                    };
                    if execute_request_sender.capacity() == 0 {
                        tracing::warn!("execute request channel congested")
                    }
                    if execute_request_sender.send(execute_request).await.is_err() {
                        tracing::error!("execute request channel closed");
                        break;
                    }
                }
            }
        }
    }
    if !client_loops.is_empty() {
        tracing::warn!(
            "service shutdown with {} active client loops",
            client_loops.len()
        );
        drop(client_reply_senders);
        while let Some(client_id) = client_loops.join_next().await {
            client_id??;
        }
    }
    Ok(())
}

async fn client_loop<A: AppProtocol, RD>(
    connection: Connection,
    submit_sender: Sender<Request<A::Op>>,
    mut reply_receiver: Receiver<Reply<A::Res, RD>>,
) -> anyhow::Result<()>
where
    Request<A::Op>: Decode<()>,
    Reply<A::Res, RD>: Encode + Clone,
{
    let mut last_reply = Option::<Reply<A::Res, RD>>::None;
    loop {
        enum Event<A, R> {
            Accept(A),
            Reply(R),
        }
        match select! {
            stream = connection.accept_uni() => Event::Accept(stream),
            reply = reply_receiver.recv() => Event::Reply(reply),
        } {
            Event::Accept(Ok(mut stream)) => {
                let bytes = stream.read_to_end(4 << 10).await?;
                let (request, _len) =
                    bincode::decode_from_slice::<Request<A::Op>, _>(&bytes, BINCODE_CONFIG)?;
                // anyhow::ensure!(len == bytes.len());
                if let Some(reply) = &last_reply {
                    if reply.client_seq > request.client_seq {
                        tracing::warn!("ignoring out of order request");
                        continue;
                    } else if reply.client_seq == request.client_seq {
                        tracing::warn!("resending reply for duplicated request");
                        let bytes = bincode::encode_to_vec(reply, BINCODE_CONFIG)?;
                        connection.open_uni().await?.write_all(&bytes).await?;
                        continue;
                    }
                }
                if submit_sender.capacity() == 0 {
                    tracing::warn!("submit channel congested")
                }
                if submit_sender.send(request).await.is_err() {
                    tracing::error!("submit channel closed");
                    break;
                }
            }
            Event::Accept(Err(err)) => match err {
                ConnectionError::ApplicationClosed(_) => break,
                ConnectionError::LocallyClosed => {
                    // probably should not happen; not drop or close the connection anywhere
                    // currently
                    tracing::warn!("client connection closed locally");
                    break;
                }
                err => anyhow::bail!(err),
            },
            Event::Reply(None) => break,
            Event::Reply(Some(reply)) => {
                let bytes = bincode::encode_to_vec(&reply, BINCODE_CONFIG)?;
                connection.open_uni().await?.write_all(&bytes).await?;
                last_reply = Some(reply.clone())
            }
        }
    }
    Ok(())
}

pub struct StorageHandle(pub Sender<storage::Invoke>);

impl StorageHandle {
    async fn put(&self, updates: Vec<(StorageKey, Vec<u8>)>) -> anyhow::Result<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.0
            .send(storage::Invoke::Put(updates, res_sender))
            .await?;
        res_receiver.await?;
        Ok(())
    }

    async fn get(&self, keys: Vec<StorageKey>) -> anyhow::Result<Vec<Option<Vec<u8>>>> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.0.send(storage::Invoke::Get(keys, res_sender)).await?;
        Ok(res_receiver.await?)
    }
}

pub struct ExecuteRequest<A: DataShardingApp, RD> {
    state: A::ExecuteState,
    client_seq: ClientSeq,
    metadata: RD,
    reply_sender: Option<Sender<Reply<A::Res, RD>>>,
}

pub async fn execute_loop<A: DataShardingApp, RD>(
    mut execute_request_receiver: Receiver<ExecuteRequest<A, RD>>,
    storage_handle: StorageHandle,
) -> anyhow::Result<()>
where
    A::Key: UpdateHash,
    A::Value: Encode + Decode<()>,
{
    while let Some(mut execute_request) = execute_request_receiver.recv().await {
        loop {
            match execute_request.state.proceed() {
                DataShardingExecuteOutput::Pending(keys) => {
                    let storage_keys = keys
                        .iter()
                        .map(|key| StorageKey::from(key.digest().0))
                        .collect();
                    let values = storage_handle.get(storage_keys).await?;
                    for (key, value) in keys.into_iter().zip(values) {
                        let value = value.map(|bytes| {
                            let (value, len) =
                                bincode::decode_from_slice(&bytes, BINCODE_CONFIG).unwrap();
                            assert_eq!(len, bytes.len());
                            value
                        });
                        execute_request.state.install(key, value)
                    }
                }
                DataShardingExecuteOutput::Complete(res, updates) => {
                    let reply = Reply {
                        client_seq: execute_request.client_seq,
                        res,
                        metadata: execute_request.metadata,
                    };
                    if let Some(reply_sender) = execute_request.reply_sender {
                        let result = reply_sender.send(reply).await;
                        if result.is_err() {
                            tracing::warn!("reply channel closed")
                        }
                    }
                    let updates = updates
                        .iter()
                        .map(|(k, v)| {
                            let bytes = bincode::encode_to_vec(v, BINCODE_CONFIG).unwrap();
                            (StorageKey::from(k.digest().0), bytes)
                        })
                        .collect();
                    storage_handle.put(updates).await?;
                    break;
                }
            }
        }
    }
    Ok(())
}
