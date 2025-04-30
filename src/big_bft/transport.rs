#![allow(unused)]
use std::net::SocketAddr;

use hdrhistogram::Histogram;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    time::Instant,
};

use crate::{
    common::ClientId,
    transport::{ServiceConfig, Transport, boot_client},
    workload::Latencies,
};

use super::{Spec, Txn, message};

pub async fn client_task(
    spec: Spec,
    service_config: ServiceConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<Txn>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<Latencies> {
    let (message_sender, mut message_receiver) = mpsc::channel(1000);
    let (mut transport, replica_egresses) = boot_client(id, service_config, message_sender).await?;

    let mut latencies = Histogram::new(3)?;
    let start = Instant::now();
    loop {
        enum Select {
            Invoke(Option<Txn>),
            Message(Option<message::Reply>),
            TransportJoinNext(()),
        }
        match tokio::select! {
            invoke = invoke_receiver.recv() => Select::Invoke(invoke),
            message = message_receiver.recv() => Select::Message(message),
            result = transport.join_next() => Select::TransportJoinNext(result?)
        } {
            Select::TransportJoinNext(()) => unreachable!(),
            Select::Invoke(None) => break Ok(latencies),
            Select::Invoke(Some(txn)) => Transport::write(txn, replica_egresses.values()).await?,
            Select::Message(None) => anyhow::bail!("message receive channel close"),
            Select::Message(Some(reply)) => {
                //
            }
        }
    }
}
