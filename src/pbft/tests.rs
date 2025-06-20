use std::collections::VecDeque;

use test_log::test;

use super::*;

struct System {
    replicas: Vec<Replica>,
    messages: VecDeque<(ReplicaId, Message)>,
    log: Vec<Event>,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Finalize(ReplicaId, Requests, ViewNum),
}

impl System {
    fn new(num_replica: ReplicaId, num_faulty_replica: ReplicaId) -> Self {
        let replicas = (0..num_replica)
            .map(|id| {
                let core_config = ReplicaCoreConfig {
                    id,
                    num_replica,
                    num_faulty_replica,
                    num_inflight_block: 1,
                    max_block_size: 100,
                };
                let config = ReplicaConfig {
                    crypto: PeerConfig::new(id as _, num_replica as _),
                };
                Replica::new(core_config, config)
            })
            .collect();
        Self {
            replicas,
            messages: Default::default(),
            log: Default::default(),
        }
    }
}

#[derive(Default)]
struct Context(Vec<ReplicaCommand>);

enum ReplicaCommand {
    Send(Message),
    Finalize(Requests, ViewNum),
}

impl super::Context for Context {
    fn send_message(&mut self, message: Message) {
        self.0.push(ReplicaCommand::Send(message));
    }

    fn finalize(&mut self, requests: Requests, view_num: ViewNum) {
        self.0.push(ReplicaCommand::Finalize(requests, view_num));
    }
}

impl System {
    fn submit(&mut self, replica_id: ReplicaId, request: Request) {
        let mut context = Context::default();
        self.replicas[replica_id as usize].submit(request, &mut context);
        self.execute(replica_id, context);
    }

    fn deliver(&mut self) -> bool {
        let Some((replica_id, message)) = self.messages.pop_front() else {
            return false;
        };
        tracing::info!(%replica_id, ?message, "deliver");
        let mut context = Context::default();
        self.replicas[replica_id as usize].receive(message, &mut context);
        self.execute(replica_id, context);
        true
    }

    fn execute(&mut self, replica_id: ReplicaId, context: Context) {
        for command in context.0 {
            match command {
                ReplicaCommand::Send(message) => {
                    for other_replica_id in 0..self.replicas.len() as ReplicaId {
                        if other_replica_id != replica_id {
                            self.messages.push_back((other_replica_id, message.clone()));
                        }
                    }
                }
                ReplicaCommand::Finalize(requests, view_num) => {
                    self.log
                        .push(Event::Finalize(replica_id, requests, view_num));
                }
            }
        }
    }

    // helpers
    fn deliver_all(&mut self, max_num_message: usize) {
        for _ in 0..max_num_message {
            if !self.deliver() {
                return;
            }
        }
        unreachable!()
    }

    fn has_finalized(&self, replica_id: ReplicaId, requests: Requests, view_num: ViewNum) -> bool {
        self.log
            .contains(&Event::Finalize(replica_id, requests, view_num))
    }

    fn has_finalized_all(&self, requests: Requests, view_num: ViewNum) -> bool {
        (0..self.replicas.len())
            .all(|replica_id| self.has_finalized(replica_id as _, requests.clone(), view_num))
    }
}

fn request(client_id: u32, seq_num: u64) -> Request {
    Request {
        client_id,
        seq_num,
        op: format!("request@{client_id}#{seq_num}").into_bytes(),
    }
}

#[test]
fn basic() {
    let mut system = System::new(4, 1);
    system.submit(0, request(0, 1));
    system.deliver_all(100);
    tracing::debug!("log: {:?}", system.log);
    assert!(system.has_finalized_all(vec![request(0, 1)], 0));
}
