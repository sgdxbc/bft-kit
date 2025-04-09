#![allow(unused)]
use std::collections::VecDeque;

use crate::crypto::threshold::givre_replica_key_shares;

use super::*;

struct System {
    replicas: Vec<Replica>,
    events: VecDeque<Event>,
}

#[derive(Debug)]
enum Event {
    SendToReplica(ReplicaId, ToReplica),
}

impl System {
    fn new(spec: Spec) -> Self {
        let key_shares = givre_replica_key_shares(spec.num_replica, spec.num_faulty);
        let replicas = (0..spec.num_replica)
            .map(|i| {
                Replica::new(
                    ReplicaCoreConfig {
                        spec: spec.clone(),
                        id: i,
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

enum Action {
    Finalize(ReplicaId, Vec<Command>),
}
type Actions = Vec<Action>;

impl System {
    fn step(&mut self, actions: &mut Actions) -> bool {
        let Some(event) = self.events.pop_front() else {
            return false;
        };
        let Event::SendToReplica(replica_id, message) = event;
        let mut replica_actions = Vec::new();
        self.replicas[replica_id as usize].receive(message, &mut replica_actions);
        self.effect(replica_id, replica_actions, actions);
        true
    }

    fn request(&mut self, replica_id: ReplicaId, command: Command) {
        let mut replica_actions = Vec::new();
        self.replicas[replica_id as usize]
            .receive(ToReplica::Request(command), &mut replica_actions);
        let mut actions = Vec::new();
        self.effect(replica_id, replica_actions, &mut actions);
        assert!(actions.is_empty())
    }

    fn effect(
        &mut self,
        replica_id: ReplicaId,
        mut replica_actions: ReplicaActions,
        actions: &mut Actions,
    ) {
        while !replica_actions.is_empty() {
            for action in take(&mut replica_actions) {
                match action {
                    ReplicaAction::SendToReplica(replica_id, message) => self
                        .events
                        .push_back(Event::SendToReplica(replica_id, message)),
                    ReplicaAction::SendToAllReplicas(message) => {
                        for id in 0..self.replicas.len() as ReplicaId {
                            if id != replica_id {
                                self.events
                                    .push_back(Event::SendToReplica(id, message.clone()))
                            }
                        }
                    }
                    ReplicaAction::Finalize(commands) => {
                        actions.push(Action::Finalize(replica_id, commands))
                    }
                }
            }
        }
    }
}
