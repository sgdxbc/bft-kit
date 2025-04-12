use std::{collections::HashMap, time::Duration};

use rand::random;
use tokio::{
    sync::mpsc::{self, Receiver, Sender},
    task::JoinSet,
    time::{Instant, timeout_at},
};

use super::ClientId;

pub type Invoke = (Vec<u8>, Option<Vec<u8>>);

pub struct ConcurrentClients {
    pub tasks: JoinSet<anyhow::Result<Latencies>>,
    pub invoke_senders: HashMap<ClientId, Sender<Invoke>>,
    pub commit_sender: Sender<ClientId>,
    pub commit_receiver: Receiver<ClientId>,
}

pub type Latencies = hdrhistogram::Histogram<u32>;

impl ConcurrentClients {
    pub fn new() -> Self {
        let (commit_sender, commit_receiver) = mpsc::channel(100);
        Self {
            tasks: JoinSet::new(),
            invoke_senders: Default::default(),
            commit_sender,
            commit_receiver,
        }
    }

    pub fn spawn<F: Future<Output = anyhow::Result<Latencies>> + Send + 'static>(
        &mut self,
        task: impl FnOnce(ClientId, Receiver<Invoke>, Sender<ClientId>) -> F,
    ) {
        let id = ClientId(random());
        let (invoke_sender, invoke_receiver) = mpsc::channel(100);
        let replaced = self.invoke_senders.insert(id, invoke_sender);
        assert!(replaced.is_none());
        self.tasks
            .spawn(task(id, invoke_receiver, self.commit_sender.clone()));
    }

    pub async fn close_loop(mut self, duration: Duration) -> anyhow::Result<Vec<Latencies>> {
        for sender in self.invoke_senders.values() {
            sender
                .send((Default::default(), Some(Default::default())))
                .await?
        }

        let deadline = Instant::now() + duration;
        while let Ok(client_id) = timeout_at(deadline, self.commit_receiver.recv()).await {
            let Some(client_id) = client_id else {
                anyhow::bail!("commit receive channel closed")
            };
            self.invoke_senders[&client_id]
                .send((Default::default(), Some(Default::default())))
                .await?
        }

        drop(self.invoke_senders);
        let mut latencies = Vec::new();
        while let Some(client_latencies) = self.tasks.join_next().await {
            latencies.push(client_latencies??)
        }
        Ok(latencies)
    }
}
