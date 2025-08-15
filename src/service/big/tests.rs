use std::collections::BTreeMap;

use test_log::test;

use crate::replication::unreplicated::Replica;

use super::{
    app::{DataShardingSchema, Kv, KvOp, KvRes},
    *,
};

#[test]
fn print_placement() {
    let config = ShardedStorageConfig {
        num_node: 10,
        num_shard: 1000,
        num_active_copy: 7,
    };
    println!("{:?}", config.node_indices_of(0));
    println!("{:?}", config.node_indices_of(1));
    println!("{:?}", config.node_indices_of(2));
    println!("{:?}", config.node_indices_of(3));

    let mut node_overheads = BTreeMap::new();
    for shard_index in 0..config.num_shard {
        for node_index in config.node_indices_of(shard_index) {
            *node_overheads.entry(node_index).or_insert(0) += 1
        }
    }
    for (node_index, num_shard) in node_overheads {
        println!("Node {node_index} has {num_shard} shards")
    }
}

type A = DataShardingSchema<Kv>;
type R = Replica<Request<Vec<KvOp>>>;
type S = ShardedStorage<<A as DataShardingApp>::Shard>;

struct SystemState {
    services: Vec<Service<A, R>>,
    service_network: VecDeque<(ServiceIndex, ServiceMessage<R, S>)>,
    replies: Vec<(ClientId, Reply<Vec<KvRes>, ()>)>,
}

#[test]
fn idle_pending() {
    let app = DataShardingSchema::new(1);
    let storage_config = ShardedStorageConfig {
        num_node: 1,
        num_shard: 1,
        num_active_copy: 1,
    };
    let storage = ShardedStorage::new(storage_config, 0, [0].into(), &app);
    let service = Service::new(app, Replica::new(), storage);
    let mut state = SystemState {
        services: vec![service],
        service_network: Default::default(),
        replies: Default::default(),
    };
    let proceed = state.services[0].proceed(Duration::ZERO);
    assert!(matches!(proceed, Proceed::Pending(None)));
}

impl SystemState {
    fn deliver(&mut self, index: ServiceIndex, message: ServiceMessage<R, S>) {
        self.services[index as usize].receive(Message::Intermediate(message))
    }

    fn proceed_service(&mut self, index: ServiceIndex, since_start: Duration) -> Option<Duration> {
        loop {
            match self.services[index as usize].proceed(since_start) {
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
                Proceed::Send(Send::Intermediate(ServiceSend::Storage((
                    Dest::All,
                    message,
                )))) => {
                    for index in 0..self.services.len() {
                        self.service_network
                            .push_back((index as _, ServiceMessage::Storage(message.clone())))
                    }
                }
            }
        }
    }

    fn deliver_proceed(
        &mut self,
        index: ServiceIndex,
        message: ServiceMessage<R, S>,
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

fn request(seq: ClientSeq, op: KvOp) -> Request<Vec<KvOp>> {
    Request {
        client_id: 0,
        client_seq: seq,
        op: vec![op],
    }
}

#[test]
fn one_service() {
    let app = DataShardingSchema::new(1);
    let storage_config = ShardedStorageConfig {
        num_node: 1,
        num_shard: 1,
        num_active_copy: 1,
    };
    let storage = ShardedStorage::new(storage_config, 0, [0].into(), &app);
    let service = Service::new(app, Replica::new(), storage);
    let mut state = SystemState {
        services: vec![service],
        service_network: Default::default(),
        replies: Default::default(),
    };
    state.services[0].receive(Message::Request(request(
        1,
        KvOp::Put("k".into(), "v".into()),
    )));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, vec![KvRes::Put]);
    state.services[0].receive(Message::Request(request(2, KvOp::Get("k".into()))));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, vec![KvRes::Get(Some("v".into()))]);
}

impl SystemState {
    fn new(num_service: ServiceIndex) -> Self {
        Self {
            services: (0..num_service)
                .map(|index| {
                    let app = DataShardingSchema::new(100);
                    let storage_config = ShardedStorageConfig {
                        num_node: num_service,
                        num_shard: 100,
                        num_active_copy: 1,
                    };
                    let storage = ShardedStorage::new(storage_config, index, [index].into(), &app);
                    Service::new(app, Replica::new(), storage)
                })
                .collect(),
            service_network: Default::default(),
            replies: Default::default(),
        }
    }

    fn receive(&mut self, request: Request<Vec<KvOp>>, since_start: Duration) -> Option<Duration> {
        earliest((0..self.services.len()).map(|index| {
            self.services[index].receive(Message::Request(request.clone()));
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
