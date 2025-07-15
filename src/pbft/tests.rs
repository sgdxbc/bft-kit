use std::collections::VecDeque;

use test_log::test;

use crate::command::ClientId;

use super::*;

struct System {
    replicas: Vec<Replica>,
    messages: VecDeque<(ReplicaId, Message)>,
    log: Vec<Event>,
}

#[derive(Debug, PartialEq, Eq)]
enum Event {
    Finalize(ReplicaId, Commands, ViewNum),
}

impl System {
    fn new(num_replica: ReplicaId, num_faulty_replica: ReplicaId) -> Self {
        Self::with_batch_size(num_replica, num_faulty_replica, 1)
    }

    fn with_batch_size(
        num_replica: ReplicaId,
        num_faulty_replica: ReplicaId,
        batch_size: usize,
    ) -> Self {
        let replicas = (0..num_replica)
            .map(|id| {
                let core_config = ReplicaCoreConfig {
                    id,
                    num_replica,
                    num_faulty_replica,
                    num_inflight_block: 1,
                    max_batch_size: batch_size,
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
struct Context(Vec<ReplicaAction>);

enum ReplicaAction {
    Send(Message),
    Finalize(Commands, ViewNum),
}

impl super::Context for Context {
    fn send_message(&mut self, message: Message) {
        self.0.push(ReplicaAction::Send(message));
    }

    fn finalize(&mut self, commands: Commands, view_num: ViewNum) {
        self.0.push(ReplicaAction::Finalize(commands, view_num));
    }
}

impl System {
    fn submit(&mut self, replica_id: ReplicaId, command: Command) {
        let mut context = Context::default();
        self.replicas[replica_id as usize].submit(command, &mut context);
        self.perform(replica_id, context);
    }

    fn deliver(&mut self) -> bool {
        let Some((replica_id, message)) = self.messages.pop_front() else {
            return false;
        };
        tracing::info!(%replica_id, ?message, "deliver");
        let mut context = Context::default();
        self.replicas[replica_id as usize].receive(message, &mut context);
        self.perform(replica_id, context);
        true
    }

    fn perform(&mut self, replica_id: ReplicaId, context: Context) {
        for action in context.0 {
            match action {
                ReplicaAction::Send(message) => {
                    for other_replica_id in 0..self.replicas.len() as ReplicaId {
                        if other_replica_id != replica_id {
                            self.messages.push_back((other_replica_id, message.clone()));
                        }
                    }
                }
                ReplicaAction::Finalize(commands, view_num) => {
                    self.log
                        .push(Event::Finalize(replica_id, commands, view_num));
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

    fn finalized_of(&self, replica_id: ReplicaId, view_num: ViewNum) -> Vec<Commands> {
        self.log
            .iter()
            .filter_map(|event| match event {
                Event::Finalize(other_replica_id, commands, other_view_num)
                    if *other_replica_id == replica_id && *other_view_num == view_num =>
                {
                    Some(commands.clone())
                }
                _ => None,
            })
            .collect()
    }

    fn has_finalized_all(&self, view_num: ViewNum, expected: &[Command]) -> bool {
        let replica0_log = self.finalized_of(0, view_num);
        let other_replica_logs = (1..self.replicas.len())
            .map(|replica_id| self.finalized_of(replica_id as _, view_num))
            .collect::<Vec<_>>();
        other_replica_logs.iter().all(|log| log == &replica0_log)
            && expected.iter().all(|command| {
                replica0_log
                    .iter()
                    .any(|commands| commands.contains(command))
            })
    }
}

fn command(client_id: u32, seq: u64) -> Command {
    Command {
        client_id: ClientId(client_id),
        seq,
        op: format!("command@{client_id}#{seq}").into_bytes(),
    }
}

#[test]
fn basic() {
    let mut system = System::new(4, 1);
    system.submit(0, command(0, 1));
    system.deliver_all(100);
    tracing::debug!("log: {:?}", system.log);
    assert!(system.has_finalized_all(0, &[command(0, 1)]))
}

#[test]
fn concurrent_submit() {
    let mut system = System::new(4, 1);
    for client_id in 0..4 {
        system.submit(0, command(client_id, 1));
    }
    system.deliver_all(100);
    assert!(
        system.has_finalized_all(
            0,
            &(0..4)
                .map(|client_id| command(client_id, 1))
                .collect::<Vec<_>>(),
        )
    )
}

#[test]
fn concurrent_submit_batched() {
    let mut system = System::with_batch_size(4, 1, 10);
    for client_id in 0..4 {
        system.submit(0, command(client_id, 1));
    }
    system.deliver_all(100);
    assert!(
        system.has_finalized_all(
            0,
            &(0..4)
                .map(|client_id| command(client_id, 1))
                .collect::<Vec<_>>(),
        )
    );
    assert!(system.replicas[0].core.ops.len() < 4)
}
