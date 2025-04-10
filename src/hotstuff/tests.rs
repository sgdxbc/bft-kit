use test_log::test;

use crate::{
    common::testing::{AbstractReplica, Action, Actions, Effect, Event},
    crypto::threshold::givre_replica_key_shares,
};

use super::*;

type System = crate::common::testing::System<Replica, ToReplica>;

impl System {
    fn new(spec: Spec) -> Self {
        let key_shares = givre_replica_key_shares(spec.num_replica, spec.num_faulty);
        let replicas = (0..spec.num_replica)
            .map(|i| {
                Replica::new(
                    ReplicaCoreConfig {
                        spec: spec.clone(),
                        id: i,
                        max_batch_size: 1,
                    },
                    CryptoConfig {
                        key_share: key_shares[i as usize].clone(),
                        num_supply_commit: 10,
                        num_max_refill: 10,
                    },
                )
            })
            .collect();
        Self {
            replicas,
            events: Default::default(),
        }
    }
}

impl AbstractReplica for Replica {
    type Action = ReplicaAction;
    type Message = ToReplica;

    fn init(&mut self, actions: &mut Vec<Self::Action>) {
        Replica::init(self, actions)
    }

    fn request(&mut self, command: Command, actions: &mut Vec<Self::Action>) {
        self.receive(ToReplica::Request(command), actions)
    }

    fn receive(&mut self, message: Self::Message, actions: &mut Vec<Self::Action>) {
        Self::receive(self, message, actions)
    }
}

impl Effect<System> for ReplicaAction {
    fn effect(self, replica_id: ReplicaId, system: &mut System, actions: &mut Actions) {
        match self {
            Self::SendToReplica(replica_id, message) => system
                .events
                .push_back(Event::SendToReplica(replica_id, message)),
            Self::SendToAllReplicas(message) => {
                for id in 0..system.num_replica() {
                    if id != replica_id {
                        system
                            .events
                            .push_back(Event::SendToReplica(id, message.clone()))
                    }
                }
            }
            Self::Finalize(commands) => actions.push(Action::Finalize(replica_id, commands)),
        }
    }
}

#[test]
fn normal_1() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    System::normal_1(&mut system)
}

#[test]
fn close_loop() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    System::close_loop(&mut system, 10, 1 + 1);
    let num_vote = 10 * (3 + 1);
    assert!(
        system
            .replicas
            .iter()
            .all(|replica| (num_vote - 1..=num_vote).contains(&replica.core.vote_height))
    )
}

#[test]
fn concurrent_clients() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    System::concurrent_clients(&mut system, 10, 1 + 1);
    let num_vote = 10 + 3;
    assert!(
        system
            .replicas
            .iter()
            .all(|replica| (num_vote - 1..=num_vote).contains(&replica.core.vote_height))
    )
}

#[test]
fn batched() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    for replica in &mut system.replicas {
        replica.core.config.max_batch_size = 100
    }
    System::concurrent_clients(&mut system, 10, 1 + 1);
    let num_vote = 2 + 3;
    assert!(
        system
            .replicas
            .iter()
            .all(|replica| (num_vote - 1..=num_vote).contains(&replica.core.vote_height))
    )
}
