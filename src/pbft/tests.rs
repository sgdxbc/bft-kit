use std::iter::{once, repeat};

use test_log::test;

use crate::testing::{Action, Actions, Effect, Event, is_finalized};

use super::*;

type System = crate::testing::System<Replica, ToReplica>;

impl System {
    fn new(spec: Spec) -> Self {
        let replicas = (0..spec.num_replica)
            .map(|i| {
                let config = ReplicaCoreConfig::new_basic(spec.clone(), i);
                Replica::new(config)
            })
            .collect();
        Self {
            replicas,
            events: Default::default(),
        }
    }
}

impl Effect<System> for ReplicaAction {
    fn effect(self, replica_id: ReplicaId, system: &mut System, actions: &mut Actions) {
        tracing::debug!(?self);
        match self {
            Self::SendToReplica(id, message) => {
                assert_ne!(id, replica_id);
                system.events.push_back(Event::SendToReplica(id, message))
            }
            ReplicaAction::SendToAllReplicas(message) => {
                for id in 0..system.num_replica() {
                    if id != replica_id {
                        system
                            .events
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
    System::close_loop(&mut system, 10, 1 + 1)
}

#[test]
fn concurrent_clients() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    System::concurrent_clients(&mut system, 10, 1 + 1)
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
    assert!(
        system
            .replicas
            .iter()
            .all(|replica| replica.core.finalize_num <= 2)
    )
}

fn concurrent_proposals(max_num_inflight: BlockNum, max_batch_size: usize) {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    for replica in &mut system.replicas {
        replica.core.config.max_num_inflight = max_num_inflight;
        replica.core.config.max_batch_size = max_batch_size
    }
    System::concurrent_clients_with_step_thresholds(
        &mut system,
        10,
        1 + 1,
        once(100 * max_num_inflight).chain(repeat(100)),
    );
    assert!(
        system
            .replicas
            .iter()
            .all(|replica| replica.core.finalize_num
                <= max_num_inflight
                    + ((10 - max_num_inflight) / max_batch_size as BlockNum).max(1))
    )
}

#[test]
fn concurrent_proposals_2() {
    concurrent_proposals(2, 1)
}

#[test]
fn concurrent_proposals_8() {
    concurrent_proposals(8, 1)
}

#[test]
fn concurrent_batched_proposals() {
    concurrent_proposals(4, 100)
}

fn drop_1(
    skip: impl Fn(&Event<ToReplica>) -> bool,
    resend_request: bool,
    tick_replica0: bool,
) -> System {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    let mut actions = Vec::new();
    system.request(0, Command::new(0, 1));
    system.filter_exhaust(skip, 100, &mut actions);
    if resend_request {
        for id in 0..system.num_replica() {
            system.request(id, Command::new(0, 1))
        }
    }
    if tick_replica0 {
        let mut replica_actions = Vec::new();
        system.replicas[0].tick(&mut replica_actions);
        system.replicas[0].tick(&mut replica_actions);
        system.effect(0, replica_actions, &mut actions)
    }
    for i in 0.. {
        assert!(i < 100);
        let progress = system.step(&mut actions);
        if !resend_request {
            assert!(progress)
        }
        if is_finalized(&actions, Command::new(0, 1), system.num_replica(), 1 + 1) {
            break;
        }
    }
    system.exhaust(100, &mut actions);
    system
}

#[test]
fn drop_request() {
    drop_1(
        |event| matches!(event, Event::SendToReplica(_, ToReplica::Request(_))),
        true,
        false,
    );
}

#[test]
fn drop_reply() {
    let system = drop_1(|_| false, true, false);
    assert!(
        system
            .replicas
            .iter()
            .all(|replica| replica.core.blocks.len() <= 1)
    );
}

#[test]
fn drop_pre_prepare() {
    drop_1(
        |event| matches!(event, Event::SendToReplica(_, ToReplica::PrePrepare(..))),
        false,
        true,
    );
}

// temporarily disabled
// the updated protocol implementation does not commit on majority, only on
// primary, so a client commit is not guaranteed (immediately)
#[test]
#[ignore]
fn drop_prepare() {
    drop_1(
        |event| matches!(event, Event::SendToReplica(_, ToReplica::Prepare(_))),
        false,
        true,
    );
}

#[test]
#[ignore]
fn drop_commit() {
    drop_1(
        |event| matches!(event, Event::SendToReplica(_, ToReplica::Commit(_))),
        false,
        true,
    );
}

#[test]
fn partition_replica3() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec);
    system.request(0, Command::new(0, 1));
    let mut actions = Vec::new();
    for i in 0.. {
        assert!(i < 100);
        if matches!(system.events.front(), Some(&Event::SendToReplica(id, _)) if id == 3) {
            system.events.pop_front();
            continue;
        }
        let progress = system.step(&mut actions);
        assert!(progress);
        if is_finalized(&actions, Command::new(0, 1), system.num_replica(), 1 + 1) {
            break;
        }
    }
}
