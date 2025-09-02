use std::{collections::HashMap, mem::take, time::Duration};

use bincode::{Decode, Encode};
use quinn::Connection;
use tokio::{
    select,
    sync::{
        mpsc::{Receiver, Sender},
        oneshot,
    },
    task::JoinSet,
    time::sleep,
};

use crate::{
    app::AppProtocol,
    service::{ClientId, Reply, Request},
    transport::BINCODE_CONFIG,
};

use super::Replicated;

pub async fn client_loop<A: AppProtocol>(
    id: ClientId,
    timeout: Duration,
    connection: Connection,
    mut invoke_receiver: Receiver<(A::Op, oneshot::Sender<A::Res>)>,
) -> anyhow::Result<()>
where
    Request<A::Op>: Encode,
    Reply<A::Res, ()>: Decode<()>,
{
    let mut seq = 0;
    let mut senders = HashMap::new();
    let mut timeouts = JoinSet::new(); // instead of TaskTracker to abort on exit
    let mut timeout_handles = HashMap::new();
    loop {
        enum Event<I, A, T> {
            Invoke(I),
            Accept(A),
            Timeout(T),
        }
        match select! {
            invoke = invoke_receiver.recv() => Event::Invoke(invoke),
            stream = connection.accept_uni() => Event::Accept(stream?),
            Some(timeout) = timeouts.join_next() => Event::Timeout(timeout),
        } {
            Event::Invoke(None) => break,
            Event::Invoke(Some((op, res_sender))) => {
                seq += 1;
                let request = Request {
                    client_id: id,
                    client_seq: seq,
                    op,
                };
                let bytes = bincode::encode_to_vec(request, BINCODE_CONFIG)?;
                connection.open_uni().await?.write_all(&bytes).await?;
                senders.insert(seq, res_sender);
                let handle = timeouts.spawn(async move {
                    sleep(timeout).await;
                    seq
                });
                timeout_handles.insert(seq, handle);
            }
            Event::Accept(mut stream) => {
                let bytes = stream.read_to_end(4 << 10).await?;
                let (reply, _) =
                    bincode::decode_from_slice::<Reply<A::Res, ()>, _>(&bytes, BINCODE_CONFIG)?;
                let Some(sender) = senders.remove(&reply.client_seq) else {
                    anyhow::bail!("invalid seq in Reply")
                };
                if sender.send(reply.res).is_err() {
                    tracing::error!("reply sender closed");
                    break;
                }
                timeout_handles.remove(&reply.client_seq).unwrap().abort()
            }
            Event::Timeout(Err(err)) => assert!(err.is_cancelled()), // propagate if panicked
            Event::Timeout(Ok(seq)) => {
                tracing::warn!("request timed out: seq {seq}");
                senders.remove(&seq); // implicitly close the result channel
                timeout_handles.remove(&seq);
            }
        }
    }
    Ok(())
}

pub async fn unreplicated_loop<L>(
    batch_size: usize,
    mut submit_receiver: Receiver<L>,
    replicated_sender: Sender<Replicated<L, ()>>,
) -> anyhow::Result<()> {
    let mut logs = Vec::new();
    while submit_receiver.recv_many(&mut logs, batch_size).await != 0 {
        let replicated = Replicated {
            logs: take(&mut logs),
            metadata: (),
        };
        if replicated_sender.capacity() == 0 {
            tracing::warn!("replicated channel congested")
        }
        if replicated_sender.send(replicated).await.is_err() {
            tracing::error!("replicated channel closed");
            break;
        }
    }
    Ok(())
}
