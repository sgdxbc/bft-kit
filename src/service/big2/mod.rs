use std::{collections::HashMap, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint};
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender, channel},
        oneshot,
    },
    time::Instant,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use crate::{
    app::{AppProtocol, DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState},
    crypto::{DigestHash, UpdateHash},
    replication::Replicated,
    service::{ClientId, Reply, Request},
    transport::BINCODE_CONFIG,
    workload::NanoLatencies,
};

use self::storage::{FetchResult, StorageKey};

use super::ClientSeq;

pub mod storage;

#[derive(Debug, Clone)]
pub struct StorageHandle(pub Sender<storage::Invoke>);

pub enum BigServiceLog<Op, M> {
    Request(Request<Op>),
    StorageOrder(M),
}

impl<Op, M> From<M> for BigServiceLog<Op, M> {
    fn from(message: M) -> Self {
        BigServiceLog::StorageOrder(message)
    }
}

pub async fn big_loop<
    A: DataShardingApp + 'static,
    SM: Send + 'static,
    RD: Clone + Send + 'static,
>(
    endpoint: Endpoint,
    app: A,
    submit_sender: Sender<BigServiceLog<A::Op, SM>>,
    mut replicated_receiver: Receiver<Replicated<BigServiceLog<A::Op, SM>, RD>>,
    storage_handle: StorageHandle,
    storage_ordered_message_sender: Sender<SM>,
) -> anyhow::Result<()>
where
    Request<A::Op>: Send + Decode<()> + 'static,
    Reply<A::Res, RD>: Send + Encode + Clone + 'static,
    A::Key: UpdateHash + Send,
    A::Value: Encode + Decode<()> + Send,
    A::ExecuteState: Send + 'static,
    A::Res: Send + 'static,
{
    let tracker = TaskTracker::new();
    let cancel = CancellationToken::new();

    let (execute_request_sender, execute_request_receiver) = channel(100);
    tracker.spawn({
        let cancel = cancel.clone();
        async move {
            if let Err(err) = execute_loop::<A, _>(execute_request_receiver, storage_handle).await {
                tracing::error!(%err);
                cancel.cancel()
            }
        }
    });

    let mut client_seqs = HashMap::new();
    let mut client_reply_senders = HashMap::new();

    let (close_sender, mut close_receiver) = channel(100);

    'outer: loop {
        enum Event<A, C, R> {
            Accept(Option<Box<A>>),
            Close(C),
            Replicated(R),
            Cancel,
        }
        match select! {
            connecting = endpoint.accept() => Event::Accept(connecting.map(Into::into)),
            Some(client_id) = close_receiver.recv() => Event::Close(client_id),
            replicated = replicated_receiver.recv() => Event::Replicated(replicated),
            () = cancel.cancelled() => Event::Cancel
        } {
            Event::Accept(None) | Event::Cancel => break,
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
                    client_loop::<A, _, _>(connection, submit_sender.clone(), reply_receiver);
                client_reply_senders.insert(client_id, reply_sender);

                let cancel = cancel.clone();
                let close_sender = close_sender.clone();
                tracker.spawn(async move {
                    if let Err(err) = client_loop.await {
                        tracing::error!(%err);
                        cancel.cancel()
                    }
                    let _ = close_sender.send(client_id).await;
                });
            }
            Event::Close(client_id) => {
                client_reply_senders.remove(&client_id);
            }
            Event::Replicated(None) => {
                tracing::error!("replicated channel closed");
                break;
            }
            Event::Replicated(Some(replicated)) => {
                for log in replicated.logs {
                    match log {
                        BigServiceLog::StorageOrder(message) => {
                            if storage_ordered_message_sender.send(message).await.is_err() {
                                tracing::error!("storage ordered receive channel closed");
                                break;
                            }
                        }
                        BigServiceLog::Request(request) => {
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
                            // if execute_request_sender.capacity() == 0 {
                            //     tracing::warn!("execute request channel congested")
                            // }
                            if execute_request_sender.send(execute_request).await.is_err() {
                                tracing::error!("execute request channel closed");
                                break 'outer;
                            }
                        }
                    }
                }
            }
        }
    }
    tracker.close();
    drop(execute_request_sender);
    if !client_reply_senders.is_empty() {
        tracing::warn!(
            "service shutdown with {} active client loops",
            client_reply_senders.len()
        );
        drop(client_reply_senders)
    }
    tracker.wait().await;
    Ok(())
}

async fn client_loop<A: AppProtocol, SM, RD>(
    connection: Connection,
    submit_sender: Sender<BigServiceLog<A::Op, SM>>,
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
                if submit_sender
                    .send(BigServiceLog::Request(request))
                    .await
                    .is_err()
                {
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

struct ExecuteRequest<A: DataShardingApp, RD> {
    state: A::ExecuteState,
    client_seq: ClientSeq,
    metadata: RD,
    reply_sender: Option<Sender<Reply<A::Res, RD>>>,
}

impl StorageHandle {
    async fn bump(&self, updates: Vec<(StorageKey, Vec<u8>)>) -> anyhow::Result<()> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.0
            .send(storage::Invoke::Bump(updates, res_sender))
            .await
            .map_err(|_| anyhow::format_err!("invoke channel closed"))?;
        res_receiver.await?;
        Ok(())
    }

    async fn fetch(&self, keys: Vec<StorageKey>) -> anyhow::Result<FetchResult> {
        let (res_sender, res_receiver) = oneshot::channel();
        self.0
            .send(storage::Invoke::Fetch(keys, res_sender))
            .await
            .map_err(|_| anyhow::format_err!("invoke channel closed"))?;
        Ok(res_receiver.await?)
    }
}

async fn execute_loop<A: DataShardingApp, RD>(
    mut execute_request_receiver: Receiver<ExecuteRequest<A, RD>>,
    storage_handle: StorageHandle,
) -> anyhow::Result<()>
where
    A::Key: UpdateHash,
    A::Value: Encode + Decode<()>,
{
    let start = Instant::now();
    let mut num_skip_version = 0;
    let mut execute_latencies = NanoLatencies::new(3).unwrap();
    'outer: while let Some(mut execute_request) = execute_request_receiver.recv().await {
        if num_skip_version > 0 {
            num_skip_version -= 1;
            continue;
        }

        let start = Instant::now();
        loop {
            match execute_request.state.proceed() {
                DataShardingExecuteOutput::Pending(keys) => {
                    let storage_keys = keys
                        .iter()
                        .map(|key| StorageKey::from(key.digest().0))
                        .collect();
                    match storage_handle.fetch(storage_keys).await? {
                        FetchResult::Values(values) => {
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
                        FetchResult::Skip(num_version) => {
                            num_skip_version = num_version - 1; // minus the current one
                            continue 'outer;
                        }
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
                    storage_handle.bump(updates).await?;
                    break;
                }
            }
        }
        execute_latencies += start.elapsed().as_nanos() as u64
    }

    tracing::info!(
        "execute tput: {} ops/sec, mean latency: {:?}",
        execute_latencies.len() as f32 / start.elapsed().as_secs_f32(),
        Duration::from_nanos(execute_latencies.mean() as _),
    );
    Ok(())
}
