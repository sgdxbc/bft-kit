use std::{net::SocketAddr, time::Duration};

use crate::{
    service::{ReplicationState, ServiceMessage, ServiceSend},
    state::{AppState, Never, State},
};

pub async fn run_replicated_service<
    R: ReplicationState<A::Op>,
    A: AppState,
    S: State<Send = ServiceSend<R, A>, Output = Never, Message = ServiceMessage<R, A>>,
>(
    state: S,
    addr: SocketAddr,
    replica_addrs: Vec<SocketAddr>,
    min_tick_interval: Duration,
) -> anyhow::Result<()> {
    Ok(())
}
