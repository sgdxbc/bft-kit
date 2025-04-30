use std::collections::VecDeque;

use crate::big_bft::Op;

use super::*;

struct System {
    replicas: Vec<Replica>,
    messages: VecDeque<(usize, Message)>,
}

impl System {
    fn new() -> Self {
        let spec = Spec {
            num_shard: 16,
            num_replica: 4,
            num_fault: 1,
            num_stripe_shard: 8,
            num_fast_replica: 1,
        };
        Self {
            replicas: (0..spec.num_replica)
                .map(|index| {
                    Replica::new(ReplicaCore::new(ReplicaCoreConfig {
                        spec: spec.clone(),
                        index,
                    }))
                })
                .collect(),
            messages: Default::default(),
        }
    }

    fn init(&mut self, num_key: usize) {
        for replica in &mut self.replicas {
            replica.core.init((0..num_key).map(|k| {
                let mut key = DigestHash::default();
                key[..size_of::<usize>()].copy_from_slice(&k.to_be_bytes());
                (key, format!("value{k}"))
            }))
        }
    }
}

enum StepAction {
    Executed(usize, Version, DigestHash),
}
type StepActions = Vec<StepAction>;

impl System {
    fn execute(&mut self, txn: Txn, actions: &mut StepActions) {
        for index in 0..self.replicas.len() {
            let mut replica_actions = Vec::new();
            self.replicas[index].execute(txn.clone(), &mut replica_actions);
            self.effect_replica_actions(index, replica_actions, actions)
        }
    }

    fn step(&mut self, actions: &mut StepActions) -> bool {
        let Some((index, message)) = self.messages.pop_front() else {
            return false;
        };
        let mut replica_actions = Vec::new();
        self.replicas[index].receive(message, &mut replica_actions);
        self.effect_replica_actions(index, replica_actions, actions);
        true
    }

    fn effect_replica_actions(
        &mut self,
        index: usize,
        replica_actions: ReplicaActions,
        actions: &mut StepActions,
    ) {
        for replica_action in replica_actions {
            match replica_action {
                ReplicaAction::Executed(version, commitment) => {
                    actions.push(StepAction::Executed(index, version, commitment))
                }
                ReplicaAction::SendToAll(message) => {
                    for other_index in 0..self.replicas.len() {
                        if other_index != index {
                            self.messages.push_back((other_index, message.clone()))
                        }
                    }
                }
            }
        }
    }
}

fn read(k: usize) -> Op {
    let mut key = DigestHash::default();
    key[..size_of::<usize>()].copy_from_slice(&k.to_be_bytes());
    Op::Read(key)
}

#[test]
fn normal_1() {
    let mut system = System::new();
    system.init(1);
    let mut actions = Vec::new();

    let txn = Txn(vec![read(0)]);
    system.execute(txn, &mut actions);

    let mut step_count = 0;
    while system.step(&mut actions) {
        assert!(step_count < 100);
        step_count += 1
    }
    assert_eq!(actions.len(), 4);
    let mut indexes = Vec::new();
    let mut last_commitment = None;
    for action in actions {
        let StepAction::Executed(index, version, commitment) = action;
        assert_eq!(version, 1);
        assert!(indexes.iter().all(|&other_index| other_index != index));
        indexes.push(index);
        if let Some(last_commitment) = last_commitment.replace(commitment) {
            assert_eq!(last_commitment, commitment)
        }
    }
}
