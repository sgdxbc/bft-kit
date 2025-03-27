use std::collections::VecDeque;

use super::*;

struct System {
    clients: Vec<Client>,
    servers: Vec<(Replica, Service)>,
    messages: VecDeque<Message>,
}

#[derive(Default)]
struct Service {
    replies: HashMap<ClientId, message::Reply>,
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
                ServiceAction::SendToClient(request.client_id, reply.clone())
            }
            _ => ServiceAction::Submit(request),
        }
    }

    fn commit(&mut self, request: message::Request, replica: &Replica) -> ServiceAction {
        let result = request.op; // echo back
        let reply = replica.reply(request.seq, result);
        let replaced = self.replies.insert(request.client_id, reply.clone());
        assert!(replaced.map(|reply| reply.seq) < Some(request.seq)); // None < Some(..)
        ServiceAction::SendToClient(request.client_id, reply)
    }
}

#[derive(Debug)]
enum Message {
    ToClient(ClientId, message::Reply),
    ToReplica(ReplicaId, ToReplica),
}

#[derive(Debug)]
enum StepResult {
    SendMessage,
    ServerNop,
    ClientNop,
    ClientReturn(ClientId, Vec<u8>),
}

impl System {
    fn new(spec: Spec, num_client: usize) -> Self {
        let clients = (0..num_client)
            .map(|i| {
                let config = ClientConfig {
                    spec: spec.clone(),
                    id: i as _,
                };
                Client::new(config)
            })
            .collect();
        let servers = (0..spec.num_replica)
            .map(|i| {
                let config = ReplicaConfig {
                    spec: spec.clone(),
                    id: i as _,
                };
                (Replica::new(config), Service::default())
            })
            .collect();
        Self {
            clients,
            servers,
            messages: Default::default(),
        }
    }

    fn step(&mut self) -> Option<StepResult> {
        let result = 'result: {
            match dbg!(self.messages.pop_front()) {
                None => return None,
                Some(Message::ToClient(client_id, reply)) => match self.clients[client_id as usize]
                    .receive(reply)
                {
                    ClientAction::Nop => StepResult::ClientNop,
                    ClientAction::Return(result) => StepResult::ClientReturn(client_id, result),
                    ClientAction::SendToReplica(id, message) => self.send_to_replica(id, message),
                    ClientAction::SendToAllReplicas(message) => {
                        self.send_to_all_replica(message, None)
                    }
                },
                Some(Message::ToReplica(replica_id, mut message)) => {
                    let (replica, service) = &mut self.servers[replica_id as usize];
                    if let ToReplica::Request(request) = message {
                        match service.receive(request) {
                            ServiceAction::Nop => break 'result StepResult::ServerNop,
                            ServiceAction::SendToClient(client_id, reply) => {
                                self.messages.push_back(Message::ToClient(client_id, reply));
                                break 'result StepResult::SendMessage;
                            }
                            ServiceAction::Submit(request) => message = ToReplica::Request(request),
                        }
                    }
                    let action = replica.receive(message);
                    self.handle_replica_action(replica_id, action)
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
            ClientAction::Nop => StepResult::ClientNop,
            ClientAction::Return(result) => StepResult::ClientReturn(client_id, result),
            ClientAction::SendToReplica(id, message) => self.send_to_replica(id, message),
            ClientAction::SendToAllReplicas(message) => self.send_to_all_replica(message, None),
        }
    }

    fn handle_replica_action(
        &mut self,
        replica_id: ReplicaId,
        replica_action: ReplicaAction,
    ) -> StepResult {
        match dbg!(replica_action) {
            ReplicaAction::Nop => StepResult::ServerNop,
            ReplicaAction::Finalize(requests) => {
                let (replica, service) = &mut self.servers[replica_id as usize];
                for request in requests {
                    match service.commit(request, replica) {
                        ServiceAction::Nop => {}
                        ServiceAction::SendToClient(client_id, reply) => {
                            self.messages.push_back(Message::ToClient(client_id, reply))
                        }
                        // probably unimplemented! when converting to a general construction
                        ServiceAction::Submit(_) => unreachable!(),
                    }
                }
                StepResult::SendMessage // should be ServerNop when no message sent to client(s)
            }
            ReplicaAction::SendToReplica(id, message) => self.send_to_replica(id, message),
            ReplicaAction::SendToAllReplicas(message) => {
                self.send_to_all_replica(message, Some(replica_id))
            }
            ReplicaAction::Prepare(vote) => {
                self.send_to_all_replica(ToReplica::Prepare(vote.clone()), Some(replica_id));
                let action = self.servers[replica_id as usize].0.insert_prepare(vote);
                self.handle_replica_action(replica_id, action)
            }
            ReplicaAction::Commit(vote) => {
                self.send_to_all_replica(ToReplica::Commit(vote.clone()), Some(replica_id));
                let action = self.servers[replica_id as usize].0.insert_commit(vote);
                self.handle_replica_action(replica_id, action)
            }
        }
    }

    fn send_to_replica(&mut self, replica_id: u8, message: ToReplica) -> StepResult {
        self.messages
            .push_back(Message::ToReplica(replica_id, message));
        StepResult::SendMessage
    }

    fn send_to_all_replica(
        &mut self,
        message: ToReplica,
        loopback: Option<ReplicaId>,
    ) -> StepResult {
        for replica_id in 0..self.servers.len() as ReplicaId {
            if loopback != Some(replica_id) {
                self.send_to_replica(replica_id, message.clone());
            }
        }
        StepResult::SendMessage
    }
}

#[test]
fn normal_1() {
    let spec = Spec {
        num_faulty: 1,
        num_replica: 4,
    };
    let mut system = System::new(spec, 1);
    let action = system.clients[0].invoke(b"hello".into());
    system.handle_client_action(0, action);
    for i in 0.. {
        assert!(i < 100);
        if let StepResult::ClientReturn(client_id, result) = dbg!(system.step().unwrap()) {
            assert_eq!(client_id, 0);
            assert_eq!(&result, b"hello");
            break;
        }
    }
    let num_committed = system
        .servers
        .iter()
        .filter(|(replica, _)| replica.is_committed(0))
        .count();
    assert!(num_committed >= 3)
}
