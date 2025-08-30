use std::{collections::HashMap, convert::identity};

use bincode::{Decode, Encode};
use quinn::{Connection, Endpoint, Incoming, RecvStream};
use tokio::{
    select,
    sync::mpsc::{Receiver, Sender, channel},
    task::JoinSet,
};

use crate::{
    app::{AppProtocol, AppState},
    replication::Replicated,
    service::{ClientId, Reply, Request},
    transport::BINCODE_CONFIG,
};

pub async fn unsharded_loop<A: AppState + 'static, RD>(
    endpoint: Endpoint,
    mut app: A,
    submit_sender: Sender<Request<A::Op>>,
    mut replicated_receiver: Receiver<Replicated<Request<A::Op>, RD>>,
) -> anyhow::Result<()>
where
    Request<A::Op>: Send + Decode<()> + 'static,
    Reply<A::Res, RD>: Send + Encode + Clone + 'static,
    RD: Clone,
{
    enum Event<R> {
        Accept(Option<Box<Incoming>>),
        Close(ClientId),
        Replicated(R),
    }
    let mut client_loops = JoinSet::new();
    let mut client_seqs = HashMap::new();
    let mut client_reply_senders = HashMap::new();
    loop {
        match select! {
            connecting = endpoint.accept() => Event::Accept(connecting.map(Into::into)),
            replicated = replicated_receiver.recv() => Event::Replicated(replicated),
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
                    let reply = Reply {
                        client_seq: request.client_seq,
                        res: app.execute(request.op),
                        metadata: replicated.metadata.clone(),
                    };
                    let Some(reply_sender) = client_reply_senders.get(&request.client_id) else {
                        tracing::warn!("no reply sender for client");
                        continue;
                    };
                    if reply_sender.capacity() == 0 {
                        tracing::warn!("reply channel congested")
                    }
                    if reply_sender.send(reply).await.is_err() {
                        tracing::warn!("failed to send reply")
                    }
                }
            }
        }
    }
    drop(client_reply_senders);
    while let Some(client_id) = client_loops.join_next().await {
        if let Err(err) = client_id.map_err(Into::into).and_then(identity) {
            tracing::warn!("service client loop error: {err}")
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
        enum Event<R> {
            Accept(RecvStream),
            Reply(R),
        }
        match select! {
            stream = connection.accept_uni() => Event::Accept(stream?),
            reply = reply_receiver.recv() => Event::Reply(reply),
        } {
            Event::Accept(mut stream) => {
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
            Event::Reply(None) => break,
            Event::Reply(Some(reply)) => {
                let bytes = bincode::encode_to_vec(&reply, BINCODE_CONFIG)?;
                connection.open_uni().await?.write_all(&bytes).await?;
                last_reply = Some(reply.clone());
            }
        }
    }
    Ok(())
}
