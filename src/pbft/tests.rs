use std::collections::VecDeque;

use test_log::test;

use super::*;

struct System {
    clients: Vec<Client>,
    servers: Vec<(Replica, Service)>,
    events: VecDeque<Event>,
}

type ClientId = u32;
const _: () = assert!(size_of::<ClientId>() == size_of::<crate::common::ClientId>());

struct Service {
    replies: HashMap<crate::common::ClientId, message::Reply>,
}

enum ServiceAction {
    Nop,
    Submit(message::Request),
    SendToClient(ClientId, message::Reply),
}

impl Service {
    fn receive(&self, request: message::Request) -> ServiceAction {
        match self.replies.get(&request.client_id) {
            Some(reply) if reply.seq > request.seq => ServiceAction::Nop,
            Some(reply) if reply.seq == request.seq => {
                ServiceAction::SendToClient(request.client_id.0, reply.clone())
            }
            _ => ServiceAction::Submit(request),
        }
    }

    fn commit(&mut self, request: message::Request, replica: &Replica) -> ServiceAction {
        let result = request.op; // echo back
        let reply = message::Reply {
            seq: request.seq,
            view_num: replica.view_num,
            result,
            replica_id: replica.config.id,
        };
        let replaced = self.replies.insert(request.client_id, reply.clone());
        assert!(replaced.map(|reply| reply.seq) < Some(request.seq)); // None < Some(..)
        ServiceAction::SendToClient(request.client_id.0, reply)
    }
}

#[derive(Debug)]
enum Event {
    SendToClient(ClientId, message::Reply),
    SendToReplica(ReplicaId, ToReplica),
}

#[derive(Debug)]
enum StepResult {
    ServerProgress,
    ClientProgress,
    ClientReturn(ClientId, Vec<u8>),
}

impl System {
    fn new(spec: Spec, num_client: u32) -> Self {
        let clients = (0..num_client)
            .map(|i| {
                let config = ClientConfig {
                    spec: spec.clone(),
                    id: ClientId(i),
                };
                Client::new(config)
            })
            .collect();
        let servers = (0..spec.num_replica)
            .map(|i| {
                let config = ReplicaConfig::new_basic(spec.clone(), i);
                (
                    Replica::new(config),
                    Service {
                        replies: Default::default(),
                    },
                )
            })
            .collect();
        Self {
            clients,
            servers,
            events: Default::default(),
        }
    }

    fn step(&mut self) -> Option<StepResult> {
        let result = 'result: {
            let event = self.events.pop_front();
            tracing::debug!(?event);
            match event? {
                Event::SendToClient(client_id, reply) => {
                    let action = self.clients[client_id as usize].receive(reply);
                    self.handle_client_action(client_id, action)
                }
                Event::SendToReplica(replica_id, mut message) => {
                    let (replica, service) = &mut self.servers[replica_id as usize];
                    if let ToReplica::Request(request) = message {
                        match service.receive(request) {
                            ServiceAction::Nop => break 'result StepResult::ServerProgress,
                            ServiceAction::SendToClient(client_id, reply) => {
                                self.events.push_back(Event::SendToClient(client_id, reply));
                                break 'result StepResult::ServerProgress;
                            }
                            ServiceAction::Submit(request) => message = ToReplica::Request(request),
                        }
                    }
                    let mut actions = Vec::new();
                    replica.receive(message, &mut actions);
                    for action in actions {
                        self.handle_replica_action(replica_id, action)
                    }
                    StepResult::ServerProgress
                }
            }
        };
        Some(result)
    }

    fn handle_client_action(
        &mut self,
        client_id: ClientId,
        client_action: ClientAction,
    ) -> StepResult {
        match client_action {
            ClientAction::Nop => {}
            ClientAction::Return(result) => return StepResult::ClientReturn(client_id, result),
            ClientAction::SendToReplica(id, message) => self.send_to_replica(id, message),
            ClientAction::SendToAllReplicas(message) => self.send_to_all_replica(message, None),
        }
        StepResult::ClientProgress
    }

    fn handle_replica_action(&mut self, replica_id: ReplicaId, replica_action: ReplicaAction) {
        tracing::debug!(?replica_action);
        match replica_action {
            ReplicaAction::SendToReplica(id, message) => self.send_to_replica(id, message),
            ReplicaAction::SendToAllReplicas(message) => {
                self.send_to_all_replica(message, Some(replica_id))
            }
            ReplicaAction::Finalize(requests) => {
                let (replica, service) = &mut self.servers[replica_id as usize];
                for request in requests {
                    match service.commit(request, replica) {
                        ServiceAction::Nop => {}
                        ServiceAction::SendToClient(client_id, reply) => {
                            self.events.push_back(Event::SendToClient(client_id, reply))
                        }
                        // probably unimplemented! when converting to a general construction
                        ServiceAction::Submit(_) => unreachable!(),
                    }
                }
            }
        }
    }

    fn send_to_replica(&mut self, replica_id: u8, message: ToReplica) {
        self.events
            .push_back(Event::SendToReplica(replica_id, message));
    }

    fn send_to_all_replica(&mut self, message: ToReplica, loopback: Option<ReplicaId>) {
        for replica_id in 0..self.servers.len() as ReplicaId {
            if loopback != Some(replica_id) {
                self.send_to_replica(replica_id, message.clone());
            }
        }
    }

    fn invoke(&mut self, client_id: ClientId, op: Vec<u8>) -> StepResult {
        let action = self.clients[client_id as usize].invoke(op);
        self.handle_client_action(client_id, action)
    }

    fn exhaust(&mut self, max_num_step: u32) {
        for _ in 0..max_num_step {
            if self.step().is_none() {
                return;
            }
        }
        unreachable!()
    }
}

#[test]
fn normal_1() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 1);
    system.invoke(0, b"hello".into());
    for i in 0.. {
        assert!(i < 100);
        if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
            assert_eq!(client_id, 0);
            assert_eq!(&result, b"hello");
            break;
        }
    }
    system.exhaust(100);
    let num_committed = system
        .servers
        .iter()
        .filter(|(replica, _)| replica.is_committed(0))
        .count();
    assert!(num_committed >= 3)
}

#[test]
fn close_loop() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 1);
    for round in 0..10 {
        system.invoke(0, format!("hello#{round}").into());
        for i in 0.. {
            assert!(i < 100);
            if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
                assert_eq!(client_id, 0);
                assert_eq!(&result, format!("hello#{round}").as_bytes());
                break;
            }
        }
    }
    system.exhaust(100);
    let num_committed = system
        .servers
        .iter()
        .filter(|(replica, _)| replica.is_committed(10))
        .count();
    assert!(num_committed >= 3)
}

#[test]
fn concurrent_clients() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 10);
    for i in 0..10 {
        system.invoke(i, format!("hello@{i}").into());
    }
    for _ in 0..10 {
        for i in 0.. {
            assert!(i < 100);
            if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
                assert_eq!(&result, format!("hello@{client_id}").as_bytes());
                break;
            }
        }
    }
    system.exhaust(100);
    let num_committed = system
        .servers
        .iter()
        .filter(|(replica, _)| replica.is_committed(10))
        .count();
    assert!(num_committed >= 3)
}

#[test]
fn batched() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 10);
    for (replica, _) in &mut system.servers {
        replica.config.max_batch_size = 100
    }
    for i in 0..10 {
        system.invoke(i, format!("hello@{i}").into());
    }
    for _ in 0..10 {
        for i in 0.. {
            assert!(i < 100);
            if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
                assert_eq!(&result, format!("hello@{client_id}").as_bytes());
                break;
            }
        }
    }
    system.exhaust(100);
    let batch_proposal = system.servers.iter().all(|(replica, _)| {
        replica
            .blocks
            .values()
            .any(|block| block.requests.len() > 1)
    });
    assert!(batch_proposal)
}

fn concurrent_proposals(max_num_inflight: BlockNum, max_batch_size: usize) {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 10);
    for (replica, _) in &mut system.servers {
        replica.config.max_num_inflight = max_num_inflight;
        replica.config.max_batch_size = max_batch_size
    }
    for i in 0..10 {
        system.invoke(i, format!("hello@{i}").into());
    }
    for num_replied in 0..10 {
        for i in 0.. {
            let threshold = if num_replied > 1 {
                100
            } else {
                100 * max_num_inflight
            };
            assert!(i < threshold);
            if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
                assert_eq!(&result, format!("hello@{client_id}").as_bytes());
                break;
            }
        }
    }
    system.exhaust(100);
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

fn drop_1(skip: impl Fn(&Event) -> bool, tick_client: bool, tick_replica0: bool) -> System {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 1);
    system.invoke(0, b"hello".into());
    while !system.events.is_empty() {
        if skip(system.events.front().unwrap()) {
            let event = system.events.pop_front();
            tracing::debug!(?event, "dropping");
            continue;
        }
        system.step();
    }
    if tick_client {
        let action = system.clients[0].tick();
        system.handle_client_action(0, action);
        let action = system.clients[0].tick();
        system.handle_client_action(0, action);
    }
    let mut actions = Vec::new();
    if tick_replica0 {
        system.servers[0].0.tick(&mut actions);
        system.servers[0].0.tick(&mut actions);
        for action in actions {
            system.handle_replica_action(0, action)
        }
    }
    for i in 0.. {
        assert!(i < 100);
        if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
            assert_eq!(client_id, 0);
            assert_eq!(&result, b"hello");
            break;
        }
    }
    system.exhaust(100);
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
    let system = drop_1(
        |event| matches!(event, Event::SendToClient(_, _)),
        true,
        false,
    );
    assert!(
        system
            .servers
            .iter()
            .all(|(replica, _)| replica.blocks.len() <= 1)
    );
}

#[test]
fn drop_pre_prepare() {
    drop_1(
        |event| matches!(event, Event::SendToReplica(_, ToReplica::PrePrepare(_))),
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
fn drop_replica3() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 1);
    system.invoke(0, b"hello".into());
    for i in 0.. {
        assert!(i < 100);
        if matches!(system.events.front(), Some(&Event::SendToReplica(id, _)) if id == 3) {
            system.events.pop_front();
            continue;
        }
        if let StepResult::ClientReturn(client_id, result) = system.step().unwrap() {
            assert_eq!(client_id, 0);
            assert_eq!(&result, b"hello");
            break;
        }
    }
}
