#![allow(unused)]
use std::net::SocketAddr;

use tokio::sync::mpsc::{self, Receiver, Sender};

use super::{Spec, Txn};

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub server_external_addresses: Vec<SocketAddr>,
}

type ClientId = u32;

pub async fn client_task(
    spec: Spec,
    service_config: ServiceConfig,
    id: ClientId,
    mut invoke_receiver: Receiver<Txn>,
    commit_sender: Sender<ClientId>,
) -> anyhow::Result<()> {
    Ok(())
}
