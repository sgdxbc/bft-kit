pub mod message {
    use bincode::{Decode, Encode};

    use crate::common::ReplicaId;

    #[derive(Debug, Clone, Encode, Decode)]
    pub struct Reply {
        pub seq: u32,
        pub result: Vec<u8>,
        pub replica_id: ReplicaId,
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
        common::{ClientId, ClientSeq, Command},
        transport::{
            AbstractService, ClientConfig, ReplicaConfig, ServiceConfig, ServiceTask, WriteMessage,
            boot_client,
        },
        workload::{ConcurrentClients, Invoke, Latencies},
    };

    use super::{ToClient, message};

    #[derive(Debug, Clone)]
    pub struct TaskConfig {
        pub client: ClientConfig,
        pub replica: ReplicaConfig,
        pub service: ServiceConfig,
        pub num_client: usize,
        pub client_duration: Duration,
        pub tick_interval: Duration,
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
        let (mut read_tasks, replica_egresses) =
            boot_client(id, service_config, message_sender).await?;

        let mut seq = 0;
        let mut write_message = WriteMessage::new();
        let mut latencies = Latencies::new(3)?;
        struct SeqScratch {
            expected_result: Option<Vec<u8>>,
            start: Instant,
        }
        let mut seq_scratch = BTreeMap::new();
        loop {
            enum Select {
                Invoke(Option<Invoke>),
                Message(Option<ToClient>),
                JoinNext(()),
            }
            match tokio::select! {
                invoke = invoke_receiver.recv() => Select::Invoke(invoke),
                message = message_receiver.recv() => Select::Message(message),
                Some(result) = read_tasks.join_next() => Select::JoinNext(result??)
            } {
                Select::JoinNext(()) => unreachable!(),
                Select::Invoke(None) => break Ok(latencies),
                Select::Invoke(Some((op, result))) => {
                    seq += 1;
                    let command = Command {
                        client_id: id,
                        seq,
                        op,
                    };
                    write_message.run(command, &replica_egresses).await?;
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
                Select::Message(reply) => {
                    let Some(reply) = reply else {
                        anyhow::bail!("message receive channel close")
                    };
                    let Some(scratch) = seq_scratch.remove(&reply.seq) else {
                        continue;
                    };
                    if let Some(result) = scratch.expected_result {
                        anyhow::ensure!(reply.result == result)
                    }
                    latencies += scratch.start.elapsed().as_micros() as u64;
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
                replica_id: self.replica_id,
            };
            self.replies.insert(command.client_id, reply.clone());
            Some((command.client_id, reply)).into_iter()
        }
    }

    pub async fn server_task(config: TaskConfig, cancel: CancellationToken) -> anyhow::Result<()> {
        let (request_sender, mut request_receiver) = mpsc::channel(100);
        let (finalized_sender, finalized_receiver) = mpsc::channel(100);

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
                finalized_sender.send(command).await?
            }
            Ok(())
        };
        tokio::try_join!(service_task, replica_task)?;
        Ok(())
    }
}
