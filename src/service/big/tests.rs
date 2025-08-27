use test_log::test;

use crate::{
    app::kv::{Kv, KvOp, KvRes},
    replication::{ReplicaIndex, unreplicated::UnreplicatedReplica},
};

use super::{storage::*, *};

#[test]
fn print_active_placement() {
    let config = ShardedStorageConfig {
        num_node: 10,
        num_faulty_node: 3,
        num_active_copy: 7,
        num_stripe: 1000,
        bypass_vote: true,
    };
    println!("{:?}", config.nodes_of_group(0).collect::<Vec<_>>());
    println!("{:?}", config.nodes_of_group(1).collect::<Vec<_>>());
    println!("{:?}", config.nodes_of_group(2).collect::<Vec<_>>());
    println!("{:?}", config.nodes_of_group(3).collect::<Vec<_>>());

    let mut node_overheads = [0; 10];
    for index in 0..1000 {
        for node_index in config.nodes_of_group(index) {
            node_overheads[node_index as usize] += 1
        }
    }
    for (node_index, num_key) in node_overheads.into_iter().enumerate() {
        println!("Node {node_index} has {num_key} shards")
    }
}

type A = Kv;
type R = UnreplicatedReplica<BigServiceLog<A, S>>;
type S = ShardedStorage;
type ServiceMessage = super::ServiceMessage<<R as State>::Message, <S as State>::Message>;

struct SystemState {
    hosts: Vec<SystemHost>,
    service_network: VecDeque<(ReplicaIndex, ServiceMessage)>,
    replies: Vec<(ClientId, Reply<KvRes, ()>)>,
}

type SystemHost = (BigService<A, R, S>, HashMap<String, Bytes>);

#[test]
fn idle_pending() {
    let app = Kv;
    let storage_config = ShardedStorageConfig {
        num_node: 1,
        num_faulty_node: 0,
        num_active_copy: 1,
        num_stripe: 1,
        bypass_vote: false,
    };
    let storage = ShardedStorage::new(storage_config, [0].into());
    let service = BigService::new(
        app,
        UnreplicatedReplica::new(),
        storage,
        ServiceConfig {
            num_cached_value: 0,
            executing_buffer_size: 0,
        },
    );
    let mut state = SystemState {
        hosts: vec![(service, Default::default())],
        service_network: Default::default(),
        replies: Default::default(),
    };
    let (service, _) = &mut state.hosts[0];
    let proceed = service.proceed(Duration::ZERO);
    assert!(matches!(proceed, Action::Pending(None)));
}

impl SystemState {
    fn deliver(&mut self, index: ReplicaIndex, message: ServiceMessage) {
        self.hosts[index as usize]
            .0
            .receive(Message::Intermediate(message))
    }

    fn proceed_service(&mut self, index: ReplicaIndex, since_start: Duration) -> Option<Duration> {
        loop {
            match self.hosts[index as usize].0.proceed(since_start) {
                Action::Pending(tick_after) => break tick_after,
                Action::Perform(Effect::Reply(client_id, reply)) => {
                    self.replies.push((client_id, reply))
                }

                // remark: current implementation only works for 1-1 mapping of services and
                // storage nodes
                Action::Perform(Effect::Intermediate(ServiceEffect::StorageSend((
                    Dest::One(index),
                    message,
                )))) => self
                    .service_network
                    .push_back((index, ServiceMessage::Storage(message))),
                Action::Perform(Effect::Intermediate(ServiceEffect::StorageSend((
                    Dest::Multi(indices),
                    message,
                )))) => {
                    for index in indices {
                        self.service_network
                            .push_back((index, ServiceMessage::Storage(message.clone())))
                    }
                }
                Action::Perform(Effect::Intermediate(ServiceEffect::StorageSend((
                    Dest::All,
                    message,
                )))) => {
                    for index in 0..self.hosts.len() {
                        self.service_network
                            .push_back((index as _, ServiceMessage::Storage(message.clone())))
                    }
                }

                Action::Perform(Effect::Intermediate(ServiceEffect::Store(Store::Get(key)))) => {
                    let (service, storage) = &mut self.hosts[index as usize];
                    let value = storage[&key].clone();
                    service.put_complete(key, value)
                }
                Action::Perform(Effect::Intermediate(ServiceEffect::Store(Store::Put(
                    key,
                    value,
                )))) => {
                    let (service, storage) = &mut self.hosts[index as usize];
                    storage.insert(key.clone(), value);
                    service.get_complete(key)
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
        num_faulty_node: 0,
        num_active_copy: 1,
        num_stripe: 1,
        bypass_vote: false,
    };
    let storage = ShardedStorage::new(storage_config, [0].into());
    let service = BigService::new(
        app,
        UnreplicatedReplica::new(),
        storage,
        ServiceConfig {
            num_cached_value: 0,
            executing_buffer_size: 0,
        },
    );
    let mut state = SystemState {
        hosts: vec![(service, Default::default())],
        service_network: Default::default(),
        replies: Default::default(),
    };

    state.hosts[0].0.receive(Message::Request(request(
        1,
        KvOp::Put("k".into(), "v".into()),
    )));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, KvRes::Put);
    state.hosts[0]
        .0
        .receive(Message::Request(request(2, KvOp::Get("k".into()))));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, KvRes::Get(Some("v".into())));
}

impl SystemState {
    fn new(num_service: ReplicaIndex, num_faulty: ReplicaIndex) -> Self {
        Self {
            hosts: (0..num_service)
                .map(|index| {
                    let app = Kv;
                    let storage_config = ShardedStorageConfig {
                        num_node: num_service,
                        num_faulty_node: num_faulty,
                        num_active_copy: 1,
                        num_stripe: 1,
                        bypass_vote: true,
                    };
                    let storage = ShardedStorage::new(storage_config, [index].into());
                    (
                        BigService::new(
                            app,
                            UnreplicatedReplica::new(),
                            storage,
                            ServiceConfig {
                                num_cached_value: 0,
                                executing_buffer_size: 0,
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
        earliest((0..self.hosts.len()).map(|index| {
            self.hosts[index]
                .0
                .receive(Message::Request(request.clone()));
            self.proceed_service(index as _, since_start)
        }))
    }
}

#[test]
fn multiple_services() {
    let mut state = SystemState::new(2, 0);
    state.receive(
        request(1, KvOp::Put("k".into(), "v".into())),
        Duration::ZERO,
    );
    state.run(Duration::ZERO);
    assert_eq!(state.replies.len(), 2);
}
