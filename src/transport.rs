use std::{collections::HashMap, net::SocketAddr, pin::pin, time::Duration};

use bincode::{Decode, Encode};
use quinn::{Connection, ConnectionError, Endpoint, Incoming, ReadError, ReadToEndError};
use tokio::{
    select, spawn,
    sync::mpsc::{self, Receiver, Sender},
    task::{JoinError, JoinSet},
    time::{Instant, sleep},
    try_join,
};
use tokio_util::{bytes::Bytes, sync::CancellationToken};

use crate::{
    Command,
    command::{Execute, ReceiveAction, Service},
    crypto::cert::quinn::{client_config, server_config},
    replica::{ReplicaProtocol, ReplyProtocol},
};

// pub mod tcp;

type Id = u64;

pub async fn run_read<M: Decode<()> + Send + Sync + 'static>(
    connection: Connection,
    sender: Sender<M>,
) -> anyhow::Result<()> {
    while let Some(bytes) = read_connection(&connection).await? {
        let (message, len) = bincode::decode_from_slice(&bytes, bincode::config::standard())?;
        anyhow::ensure!(len == bytes.len());
        if sender.capacity() == 0 {
            tracing::warn!("read channel full")
        }
        if sender.send(message).await.is_err() {
            tracing::warn!("read channel closed, exiting");
            break;
        }
    }
    Ok(())
}

async fn read_connection(connection: &Connection) -> anyhow::Result<Option<Vec<u8>>> {
    let mut stream = match connection.accept_uni().await {
        Ok(stream) => stream,
        Err(ConnectionError::LocallyClosed | ConnectionError::ApplicationClosed(_)) => {
            return Ok(None);
        }
        Err(err) => anyhow::bail!(err),
    };
    match stream.read_to_end(1 << 16).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ReadToEndError::Read(ReadError::ConnectionLost(
            ConnectionError::LocallyClosed | ConnectionError::ApplicationClosed(_),
        ))) => {
            tracing::warn!("connection closed while reading");
            Ok(None)
        }
        Err(err) => Err(err)?,
    }
}

pub async fn run_write(
    connection: Connection,
    mut receiver: Receiver<Bytes>,
) -> anyhow::Result<()> {
    while let Some(message) = receiver.recv().await {
        let mut stream = connection.open_uni().await?;
        // TODO figure a way to concurrently write into a connection (via multiple
        // streams) if that matters to performance
        stream.write_all(&message).await?
    }
    // the safe reasoning of read side could be a bit counterintuitive
    // the assumption is that the write sender is kept by same owner of the read
    // receiver, and it would only close the write channel (signaling the connection
    // should be closed) after it has read everything (it wants)
    // so, if this LocallyClosed interrupts read task when some messages are yet to
    // be delivered, those are messages _unexpected_ by the owner, i.e., owner will
    // not read them anyway
    connection.close(Default::default(), Default::default());
    Ok(())
}

pub async fn write(
    message: impl Encode,
    senders: impl IntoIterator<Item = &'_ Sender<Bytes>>,
) -> anyhow::Result<()> {
    let bytes = Bytes::from(bincode::encode_to_vec(
        message,
        bincode::config::standard(),
    )?);
    for sender in senders {
        if sender.capacity() == 0 {
            tracing::warn!("write channel full")
        }
        if sender.send(bytes.clone()).await.is_err() {
            tracing::warn!("write channel closed")
        }
    }
    Ok(())
}

pub async fn run_transport<M: Decode<()> + Send + Sync + 'static>(
    connection: Connection,
    read_sender: Sender<M>,
    write_receiver: Receiver<Bytes>,
) -> anyhow::Result<()> {
    let read_task = spawn(run_read(connection.clone(), read_sender));
    if let Err(err) = run_write(connection, write_receiver).await {
        read_task.abort();
        anyhow::bail!(err)
    }
    // if write task exits successfully, it must have closed the connection so read
    // task will exit (soon)
    read_task.await.unwrap()?; // nowhere cancel the task and propagate panic
    Ok(())
}

pub type WriteSenders = HashMap<Id, Sender<Bytes>>;

// TODO extract client skeleton
// not sure whether that is possible or not since (simple) clients are inline
// implemented in transport
// nonetheless, bootstrapping part is extracted below

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    external_addresses: HashMap<Id, SocketAddr>,
}

pub async fn start_client<M: Decode<()> + Send + Sync + 'static>(
    id: Id,
    service_config: &ServiceConfig,
    message_sender: Sender<M>,
    tasks: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<WriteSenders> {
    let mut endpoint = Endpoint::client(([0, 0, 0, 0], 0).into())?;
    endpoint.set_default_client_config(client_config());
    let mut write_senders = HashMap::new();
    for (&replica_id, &addr) in &service_config.external_addresses {
        let connection = endpoint.connect(addr, "server.example")?.await?;
        connection
            .open_uni()
            .await?
            .write_all(&id.to_le_bytes())
            .await?;
        let (write_sender, write_receiver) = mpsc::channel(1000);
        tasks.spawn(run_transport(
            connection,
            message_sender.clone(),
            write_receiver,
        ));
        write_senders.insert(replica_id, write_sender);
    }
    Ok(write_senders)
}

pub async fn run_service<E: Execute, P: ReplyProtocol>(
    mut service: Service<E>,
    external_address: SocketAddr,
    submit_sender: Sender<Command>,
    mut finalized_receiver: Receiver<(Vec<Command>, P::FinalizeMetadata)>,
    cancel: CancellationToken,
) -> anyhow::Result<()>
where
    P::Reply: Encode,
    P::FinalizeMetadata: Send + Sync + 'static,
{
    let endpoint = Endpoint::server(server_config(), external_address)?;
    let (request_sender, mut request_receiver) = mpsc::channel(10_000);
    let mut last_finalize_metadata = None;
    let mut write_senders = HashMap::new();
    let mut transport_tasks = JoinSet::new();

    enum Event<M> {
        Accept(Option<Box<Incoming>>),
        Request(Option<Command>),
        Finalized((Vec<Command>, M)),
        Transport(Result<(Id, anyhow::Result<()>), JoinError>),
        Cancel,
    }
    use Event::*;

    loop {
        match select! {
            accept = endpoint.accept() => Accept(accept.map(Into::into)),
            request = request_receiver.recv() => Request(request),
            Some(finalized) = finalized_receiver.recv() => Finalized(finalized),
            Some(transport) = transport_tasks.join_next() => Transport(transport),
            () = cancel.cancelled() => Cancel,
        } {
            Accept(None) => unreachable!("locally-kept endpoint is not closed anywhere"),
            Accept(Some(incoming)) => {
                let connection = (*incoming).await?;
                let mut client_id = [0; size_of::<Id>()];
                connection
                    .accept_uni()
                    .await?
                    .read_exact(&mut client_id)
                    .await?;
                let client_id = Id::from_le_bytes(client_id);
                let (write_sender, write_receiver) = mpsc::channel(100);
                let request_sender = request_sender.clone();
                transport_tasks.spawn(async move {
                    (
                        client_id,
                        run_transport(connection, request_sender, write_receiver).await,
                    )
                });
                write_senders.insert(client_id, write_sender);
            }
            Request(None) => unreachable!("the original request sender is never dropped"),
            Request(Some(command)) => match service.receive(&command) {
                ReceiveAction::Ignore => {}
                ReceiveAction::Submit => {
                    if submit_sender.capacity() == 0 {
                        tracing::warn!("submit channel full");
                    }
                    submit_sender.send(command).await?
                }
                ReceiveAction::Reply(result) => {
                    let Some(metadata) = &last_finalize_metadata else {
                        anyhow::bail!("missing finalized metadata")
                    };
                    let sender = write_senders.get(&(command.client_id.0 as Id));
                    if sender.is_none() {
                        // assume the client has closed the connection during this request buffering
                        // at service ingress and don't care about the result anymore
                        tracing::info!(%command.client_id, "send to closed connection");
                        continue;
                    }
                    write(P::new_reply(command.seq, result, metadata), sender).await?
                }
            },
            Finalized((commands, metadata)) => {
                for (client_id, seq, result) in service.execute(&commands) {
                    let sender = write_senders.get(&(client_id.0 as Id));
                    if sender.is_none() {
                        // assume the client has closed the connection during service processing
                        // this command and don't care about the result anymore
                        tracing::info!(%client_id, "send to closed connection");
                        continue;
                    }
                    write(P::new_reply(seq, result, &metadata), sender).await?
                }
                last_finalize_metadata = Some(metadata)
            }
            Transport(result) => {
                let (client_id, result) = result?;
                write_senders.remove(&client_id);
                if let Err(err) = result {
                    if !matches!(
                        err.downcast_ref(),
                        Some(ConnectionError::ApplicationClosed(_))
                    ) {
                        tracing::warn!(%client_id, %err)
                    }
                }
            }
            Cancel => break,
        }
    }

    // dump log here, if useful
    Ok(())
}

#[derive(Debug, Clone)]
pub struct ReplicaConfig {
    internal_addresses: HashMap<Id, SocketAddr>,
    // how long should replicas wait before attempting to connect each other's
    // internal addresses. not necessary for QUIC because it allows connect before
    // accept. set longer in higher latency environments (or human action is
    // involved)
    interconnect_delay: Duration,
    tick_interval: Duration,
}

pub async fn start_replica<M: Decode<()> + Send + Sync + 'static>(
    id: Id,
    config: &ReplicaConfig,
    message_sender: Sender<M>,
    tasks: &mut JoinSet<anyhow::Result<()>>,
) -> anyhow::Result<WriteSenders> {
    let mut internal_endpoint = Endpoint::server(server_config(), config.internal_addresses[&id])?;
    internal_endpoint.set_default_client_config(client_config());
    let active_task = async {
        let mut connections = HashMap::new();
        for (&i, &addr) in &config.internal_addresses {
            if i <= id {
                continue;
            }
            let connection = internal_endpoint.connect(addr, "server.example")?.await?;
            tracing::debug!(remote = ?connection.remote_address(), "connect");
            connection
                .open_uni()
                .await?
                // to need to `to_le_bytes()` for current u8 based ReplicaId, just future proof
                .write_all(&id.to_le_bytes())
                .await?;
            connections.insert(i, connection);
        }
        anyhow::Ok(connections)
    };
    let passive_task = async {
        tracing::info!(addr = ?internal_endpoint.local_addr(), "start listening");
        let mut connections = HashMap::new();
        for _ in 0..id {
            let connection = internal_endpoint
                .accept()
                .await
                .expect("endpoint not closed")
                .await?;
            tracing::debug!(remote = ?connection.remote_address(), "accept");
            let mut replica_id = [0; size_of::<Id>()];
            connection
                .accept_uni()
                .await?
                .read_exact(&mut replica_id)
                .await?;
            connections.insert(Id::from_le_bytes(replica_id), connection);
        }
        Ok(connections)
    };
    let (mut connections, other_connections) = try_join!(active_task, passive_task)?;
    connections.extend(other_connections);
    anyhow::ensure!(connections.len() == config.internal_addresses.len() - 1);

    let mut write_senders = HashMap::new();
    for (id, connection) in connections {
        let (write_sender, write_receiver) = mpsc::channel(100);
        tasks.spawn(run_transport(
            connection,
            message_sender.clone(),
            write_receiver,
        ));
        write_senders.insert(id, write_sender);
    }
    Ok(write_senders)
}

pub struct ReplicaContext<M> {
    send_buffer: Vec<M>,
    finalize_buffer: Vec<Vec<Command>>,
}

pub async fn run_replica<R: ReplicaProtocol<ReplicaContext<M>, Message = M>, M>(
    mut replica: R,
    id: Id,
    config: ReplicaConfig,
    mut submit_receiver: Receiver<Command>,
    finalized_sender: Sender<(Vec<Command>, R::FinalizeMetadata)>,
) -> anyhow::Result<()>
where
    R::Message: Encode + Decode<()> + Send + Sync + 'static,
    R::FinalizeMetadata: Send + Sync + 'static,
{
    let (message_sender, mut message_receiver) = mpsc::channel(100);
    let mut transport_tasks = JoinSet::new();
    let write_senders = start_replica(id, &config, message_sender, &mut transport_tasks).await?;
    let mut context = ReplicaContext {
        send_buffer: Default::default(),
        finalize_buffer: Default::default(),
    };

    replica.init(&mut context);

    let mut next_tick = pin!(sleep(config.tick_interval));
    let mut ticked_at = Instant::now();

    enum Event<M> {
        Submit(Option<Command>),
        Message(M),
        Tick,
        Transport(Result<anyhow::Result<()>, JoinError>),
    }
    use Event::*;
    loop {
        match select! {
            submit = submit_receiver.recv() => Submit(submit),
            Some(message) = message_receiver.recv() => Message(message),
            () = &mut next_tick => Tick,
            Some(result) = transport_tasks.join_next() => Transport(result),
        } {
            Submit(None) => break Ok(()),
            Submit(Some(command)) => replica.submit(command, &mut context),
            Message(message) => replica.receive(message, &mut context),
            Tick => {
                replica.tick(ticked_at.elapsed(), &mut context);
                ticked_at = Instant::now();
                next_tick.as_mut().reset(ticked_at + config.tick_interval);
            }
            Transport(result) => {
                let result = result?;
                tracing::warn!(?result, "unexpected transport task exited")
            }
        }
        for message in context.send_buffer.drain(..) {
            write(message, write_senders.values()).await?
        }
        for commands in context.finalize_buffer.drain(..) {
            if finalized_sender.capacity() == 0 {
                tracing::warn!("finalized channel full")
            }
            finalized_sender
                .send((commands, replica.finalize_metadata()))
                .await?
        }
    }
}

mod parse {
    use std::time::Duration;

    use crate::parse::Settings;

    impl TryFrom<Settings> for crate::transport::ServiceConfig {
        type Error = anyhow::Error;

        fn try_from(settings: Settings) -> Result<Self, Self::Error> {
            Ok(Self {
                // if necessary, allow nonconsecutive replica id
                external_addresses: settings
                    .get_values("server_external_address")?
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (i as _, addr))
                    .collect(),
            })
        }
    }

    impl TryFrom<Settings> for super::ReplicaConfig {
        type Error = anyhow::Error;

        fn try_from(settings: Settings) -> Result<Self, Self::Error> {
            Ok(Self {
                // if necessary, allow nonconsecutive replica id
                internal_addresses: settings
                    .get_values("server_internal_address")?
                    .into_iter()
                    .enumerate()
                    .map(|(i, addr)| (i as _, addr))
                    .collect(),
                interconnect_delay: Duration::from_secs_f32(
                    settings
                        .get_option("server_interconnect_delay")?
                        .unwrap_or(0.),
                ),
                tick_interval: Duration::from_secs_f32(settings.get("server_tick_interval")?),
            })
        }
    }
}
