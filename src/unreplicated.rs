pub mod message {
    use bincode::{Decode, Encode};

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Reply {
        pub seq: u32,
        pub result: Vec<u8>,
    }
}

pub type ToClient = message::Reply;

pub mod transport {
    use std::{collections::BTreeMap, time::Duration};

    use tokio::{
        sync::mpsc::{self, Receiver, Sender},
        time::Instant,
    };
    use tokio_util::sync::CancellationToken;

    use crate::{
        transport::{
            AbstractService, ClientConfig, ServiceConfig, ServiceTask, Transport, boot_client,
            checked_send,
        },
        workload::{ConcurrentClients, Invoke, Latencies},
        {ClientId, ClientSeq, Command},
    };

    use super::{ToClient, message};

    #[derive(Debug, Clone)]
    pub struct TaskConfig {
        pub client: ClientConfig,
        pub service: ServiceConfig,
        pub num_client: usize,
        pub client_duration: Duration,
    }

    pub const WARMUP_DURATION: Duration = Duration::from_secs(1);

    pub async fn client_task(
        config: ClientConfig,
        service_config: ServiceConfig,
        id: ClientId,
        mut invoke_receiver: Receiver<Invoke>,
        commit_sender: Sender<ClientId>,
    ) -> anyhow::Result<Latencies> {
        let (message_sender, mut message_receiver) = mpsc::channel(64);
        let (mut transport, replica_egresses) =
            boot_client(id, service_config, message_sender).await?;

        let mut seq = 0;
        let mut latencies = Latencies::new(3)?;
        let start = Instant::now();
        struct SeqScratch {
            expected_result: Option<Vec<u8>>,
            start: Instant,
        }
        let mut seq_scratch = BTreeMap::new();
        loop {
            enum Select {
                Invoke(Option<Invoke>),
                Message(Option<ToClient>),
                TransportJoinNext(()),
            }
            match tokio::select! {
                invoke = invoke_receiver.recv() => Select::Invoke(invoke),
                message = message_receiver.recv() => Select::Message(message),
                result = transport.join_next() => Select::TransportJoinNext(result?)
            } {
                Select::TransportJoinNext(()) | Select::Message(None) => unreachable!(),
                Select::Invoke(None) => break Ok(latencies),
                Select::Invoke(Some((op, result))) => {
                    seq += 1;
                    let command = Command {
                        client_id: id,
                        seq,
                        op,
                    };
                    let egress = replica_egresses.get(&0);
                    anyhow::ensure!(egress.is_some());
                    Transport::write(command, egress).await?;
                    match &config {
                        // resend for close loop?
                        ClientConfig::CloseLoop => anyhow::ensure!(seq_scratch.is_empty()),
                        ClientConfig::OpenLoop(config) => {
                            if seq_scratch.len() == config.num_max_concurrent {
                                seq_scratch.pop_first();
                            }
                        }
                    }
                    seq_scratch.insert(
                        seq,
                        SeqScratch {
                            expected_result: result,
                            start: Instant::now(),
                        },
                    );
                }
                Select::Message(Some(reply)) => {
                    let Some(scratch) = seq_scratch.remove(&reply.seq) else {
                        continue;
                    };
                    if let Some(result) = scratch.expected_result {
                        anyhow::ensure!(reply.result == result)
                    }
                    let end = Instant::now();
                    if end.duration_since(start) >= WARMUP_DURATION {
                        latencies += end.duration_since(scratch.start).as_micros() as u64;
                    }
                    commit_sender.send(id).await?
                }
            }
        }
    }

    pub async fn clients_task(config: TaskConfig) -> anyhow::Result<Vec<Latencies>> {
        let mut concurrent_clients = ConcurrentClients::new();
        for _ in 0..config.num_client {
            concurrent_clients.spawn(|id, invoke_receiver, commit_sender| {
                client_task(
                    config.client.clone(),
                    config.service.clone(),
                    id,
                    invoke_receiver,
                    commit_sender,
                )
            })
        }
        match config.client {
            ClientConfig::CloseLoop => concurrent_clients.close_loop(config.client_duration).await,
            ClientConfig::OpenLoop(client_config) => {
                concurrent_clients
                    .open_loop(config.client_duration, client_config.sending_rate)
                    .await
            }
        }
    }

    pub struct ServiceKit;
    impl AbstractService for ServiceTask<ServiceKit> {
        type Reply = message::Reply;
        type Finalized = Command;

        fn reply_seq(reply: &Self::Reply) -> ClientSeq {
            reply.seq
        }

        fn on_finalized(
            &mut self,
            command: Self::Finalized,
        ) -> impl Iterator<Item = (ClientId, Self::Reply)> {
            if matches!(self.replies.get(&command.client_id), Some(reply) if reply.seq >= command.seq)
            {
                tracing::warn!(?command, "duplicated finalized");
                return None.into_iter();
            }
            let reply = message::Reply {
                seq: command.seq,
                // a 0/0 service, extend to support arbitrary state machine later
                result: Default::default(),
            };
            self.replies.insert(command.client_id, reply.clone());
            Some((command.client_id, reply)).into_iter()
        }
    }

    pub async fn server_task(config: TaskConfig, cancel: CancellationToken) -> anyhow::Result<()> {
        let (request_sender, mut request_receiver) = mpsc::channel(1000);
        let (finalized_sender, finalized_receiver) = mpsc::channel(1000);

        let service_task = ServiceTask::<ServiceKit>::new(0, request_sender).run(
            config.service,
            finalized_receiver,
            cancel.clone(),
        );
        let replica_task = async {
            while let Some(command) = cancel
                .run_until_cancelled(request_receiver.recv())
                .await
                .flatten()
            {
                // finalized_sender.send(command).await?
                if !checked_send(&finalized_sender, command).await? {
                    tracing::warn!("finalized channel full");
                }
            }
            Ok(())
        };
        tokio::try_join!(service_task, replica_task)?;
        Ok(())
    }
}

mod parse {
    use std::time::Duration;

    use crate::parse::Options;

    impl TryFrom<Options> for super::transport::TaskConfig {
        type Error = anyhow::Error; // TODO

        fn try_from(options: Options) -> Result<Self, Self::Error> {
            Ok(Self {
                client: options.clone().try_into()?,
                service: options.clone().try_into()?,
                num_client: options.get("num_client")?,
                client_duration: Duration::from_secs_f32(options.get("client_duration")?),
            })
        }
    }
}
