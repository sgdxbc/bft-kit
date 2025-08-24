use test_log::test;

use crate::{
    app::kv::{Kv, KvOp, KvRes},
    replication::{ReplicaIndex, unreplicated::UnreplicatedReplica},
};

use super::{storage::*, *};

#[test]
fn print_placement() {
    let config = ShardedStorageConfig {
        num_node: 10,
        num_active_copy: 7,
    };
    println!("{:?}", config.node_indices_of(Key::from_low_u64_le(0)));
    println!("{:?}", config.node_indices_of(Key::from_low_u64_le(1)));
    println!("{:?}", config.node_indices_of(Key::from_low_u64_le(2)));
    println!("{:?}", config.node_indices_of(Key::from_low_u64_le(3)));

    let mut node_overheads = [0; 10];
    for key in 0..1000 {
        for node_index in config.node_indices_of(Key::from_low_u64_le(key)) {
            node_overheads[node_index as usize] += 1;
        }
    }
    for (node_index, num_key) in node_overheads.into_iter().enumerate() {
        println!("Node {node_index} has {num_key} keys")
    }
}

type A = Kv;
type R = UnreplicatedReplica<Request<KvOp>>;
type S = ShardedStorage;
type ServiceMessage = super::ServiceMessage<<R as State>::Message, <S as State>::Message>;

struct SystemState {
    services: Vec<(BigService<A, R>, HashMap<String, Bytes>)>,
    service_network: VecDeque<(ReplicaIndex, ServiceMessage)>,
    replies: Vec<(ClientId, Reply<KvRes, ()>)>,
}

#[test]
fn idle_pending() {
    let app = Kv;
    let storage_config = ShardedStorageConfig {
        num_node: 1,
        num_active_copy: 1,
    };
    let storage = ShardedStorage::new(storage_config, 0, [0].into());
    let service = BigService::new(
        app,
        UnreplicatedReplica::new(),
        storage,
        ServiceConfig {
            num_cached_value: 0,
            num_max_will_fetch: 0,
        },
    );
    let mut state = SystemState {
        services: vec![(service, Default::default())],
        service_network: Default::default(),
        replies: Default::default(),
    };
    let (service, _) = &mut state.services[0];
    let proceed = service.proceed(Duration::ZERO);
    assert!(matches!(proceed, Proceed::Pending(None)));
}

impl SystemState {
    fn deliver(&mut self, index: ReplicaIndex, message: ServiceMessage) {
        self.services[index as usize]
            .0
            .receive(Message::Intermediate(message))
    }

    fn proceed_service(&mut self, index: ReplicaIndex, since_start: Duration) -> Option<Duration> {
        loop {
            match self.services[index as usize].0.proceed(since_start) {
                Proceed::Pending(tick_after) => break tick_after,
                Proceed::Send(Send::Reply(client_id, reply)) => {
                    self.replies.push((client_id, reply))
                }
                Proceed::Send(Send::Intermediate(ServiceSend::Storage((
                    Dest::One(index),
                    message,
                )))) => self
                    .service_network
                    .push_back((index, ServiceMessage::Storage(message))),
                Proceed::Send(Send::Intermediate(ServiceSend::Storage((
                    Dest::Multi(indices),
                    message,
                )))) => {
                    for index in indices {
                        self.service_network
                            .push_back((index, ServiceMessage::Storage(message.clone())))
                    }
                }
                Proceed::Send(Send::Intermediate(ServiceSend::Storage((Dest::All, message)))) => {
                    for index in 0..self.services.len() {
                        self.service_network
                            .push_back((index as _, ServiceMessage::Storage(message.clone())))
                    }
                }
                Proceed::Output(Output::Read(key)) => {
                    let (service, storage) = &mut self.services[index as usize];
                    let value = storage[&key].clone();
                    service.read_ok(key, value)
                }
                Proceed::Output(Output::Write(key, value)) => {
                    let (service, storage) = &mut self.services[index as usize];
                    storage.insert(key.clone(), value);
                    service.write_ok(key)
                }
            }
        }
    }

    fn deliver_proceed(
        &mut self,
        index: ReplicaIndex,
        message: ServiceMessage,
        since_start: Duration,
    ) -> Option<Duration> {
        self.deliver(index, message);
        self.proceed_service(index, since_start)
    }

    fn run(&mut self, since_start: Duration) -> Option<Duration> {
        let mut tick_after = None;
        while let Some((index, message)) = self.service_network.pop_front() {
            tracing::trace!(%index, ?message);
            tick_after = earliest([
                self.deliver_proceed(index, message, since_start),
                tick_after,
            ])
        }
        tick_after
    }
}

fn request(seq: ClientSeq, op: KvOp) -> Request<KvOp> {
    Request {
        client_id: 0,
        client_seq: seq,
        op,
    }
}

#[test]
fn one_service() {
    let app = Kv;
    let storage_config = ShardedStorageConfig {
        num_node: 1,
        num_active_copy: 1,
    };
    let storage = ShardedStorage::new(storage_config, 0, [0].into());
    let service = BigService::new(
        app,
        UnreplicatedReplica::new(),
        storage,
        ServiceConfig {
            num_cached_value: 0,
            num_max_will_fetch: 0,
        },
    );
    let mut state = SystemState {
        services: vec![(service, Default::default())],
        service_network: Default::default(),
        replies: Default::default(),
    };

    state.services[0].0.receive(Message::Request(request(
        1,
        KvOp::Put("k".into(), "v".into()),
    )));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, KvRes::Put);
    state.services[0]
        .0
        .receive(Message::Request(request(2, KvOp::Get("k".into()))));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, KvRes::Get(Some("v".into())));
}

impl SystemState {
    fn new(num_service: ReplicaIndex) -> Self {
        Self {
            services: (0..num_service)
                .map(|index| {
                    let app = Kv;
                    let storage_config = ShardedStorageConfig {
                        num_node: num_service,
                        num_active_copy: 1,
                    };
                    let storage = ShardedStorage::new(storage_config, index, [index].into());
                    (
                        BigService::new(
                            app,
                            UnreplicatedReplica::new(),
                            storage,
                            ServiceConfig {
                                num_cached_value: 0,
                                num_max_will_fetch: 0,
                            },
                        ),
                        Default::default(),
                    )
                })
                .collect(),
            service_network: Default::default(),
            replies: Default::default(),
        }
    }

    fn receive(&mut self, request: Request<KvOp>, since_start: Duration) -> Option<Duration> {
        earliest((0..self.services.len()).map(|index| {
            self.services[index]
                .0
                .receive(Message::Request(request.clone()));
            self.proceed_service(index as _, since_start)
        }))
    }
}

#[test]
fn multiple_services() {
    let mut state = SystemState::new(2);
    state.receive(
        request(1, KvOp::Put("k".into(), "v".into())),
        Duration::ZERO,
    );
    state.run(Duration::ZERO);
    assert_eq!(state.replies.len(), 2);
}
