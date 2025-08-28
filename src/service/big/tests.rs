use std::{collections::HashSet, iter::repeat_n};

use test_log::test;

use crate::{
    app::kv::{Kv, KvOp, KvRes},
    replication::{ReplicaIndex, unreplicated::UnreplicatedReplica},
    service::Store,
};

use super::{storage::*, *};

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
    fn proceed_service(&mut self, index: ReplicaIndex, since_start: Duration) -> Option<Duration> {
        loop {
            let (service, storage) = &mut self.hosts[index as usize];
            match service.proceed(since_start) {
                Action::Pending(tick_after) => break tick_after,
                Action::Perform(Effect::Reply(client_id, reply)) => {
                    self.replies.push((client_id, reply))
                }

                // remark: current implementation only works for 1-1 mapping of services and
                // storage nodes
                Action::Perform(Effect::Intermediate(ServiceEffect::Storage(
                    StorageStateEffect::Send((Dest::One(index), message)),
                ))) => self
                    .service_network
                    .push_back((index, ServiceMessage::Storage(message))),
                Action::Perform(Effect::Intermediate(ServiceEffect::Storage(
                    StorageStateEffect::Send((Dest::Multi(indices), message)),
                ))) => {
                    for node_index in indices {
                        if node_index == index {
                            continue;
                        }
                        self.service_network
                            .push_back((node_index, ServiceMessage::Storage(message.clone())))
                    }
                }
                Action::Perform(Effect::Intermediate(ServiceEffect::Storage(
                    StorageStateEffect::Send((Dest::All, message)),
                ))) => {
                    for node_index in 0..self.hosts.len() {
                        if node_index as ReplicaIndex == index {
                            continue;
                        }
                        self.service_network
                            .push_back((node_index as _, ServiceMessage::Storage(message.clone())))
                    }
                }

                Action::Perform(Effect::Intermediate(ServiceEffect::Storage(
                    StorageStateEffect::Store(Store::Get(key)),
                ))) => {
                    let value = storage[&key].clone();
                    service.get_complete(key, value)
                }
                Action::Perform(Effect::Intermediate(ServiceEffect::Storage(
                    StorageStateEffect::Store(Store::Put(key, value)),
                ))) => {
                    storage.insert(key.clone(), value);
                    service.put_complete(key)
                }
                Action::Perform(Effect::Intermediate(ServiceEffect::Storage(
                    StorageStateEffect::Store(Store::Delete(key)),
                ))) => {
                    storage.remove(&key);
                }
            }
        }
    }

    fn run(&mut self, since_start: Duration) -> Option<Duration> {
        let mut earliest_tick_after = None;
        while let Some((index, message)) = self.service_network.pop_front() {
            tracing::trace!(%index, ?message);
            self.hosts[index as usize]
                .0
                .receive(Message::Intermediate(message));
            let tick_after = self.proceed_service(index, since_start);
            earliest_tick_after = earliest([tick_after, earliest_tick_after])
        }
        earliest_tick_after
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
    fn with_config(config: ShardedStorageConfig) -> Self {
        Self {
            hosts: (0..config.num_node)
                .map(|index| {
                    let app = Kv;
                    let storage = ShardedStorage::new(config.clone(), [index].into());
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
        let tick_afters = (0..self.hosts.len()).map(|index| {
            self.hosts[index]
                .0
                .receive(Message::Request(request.clone()));
            self.proceed_service(index as _, since_start)
        });
        earliest(tick_afters)
    }
}

#[test]
fn multiple_services() {
    let mut state = SystemState::with_config(ShardedStorageConfig {
        num_node: 2,
        num_faulty_node: 0,
        num_stripe: 2,
        num_active_copy: 1,
        bypass_vote: true,
    });
    state.receive(
        request(1, KvOp::Put("k".into(), "v".into())),
        Duration::ZERO,
    );
    state.run(Duration::ZERO);
    assert_eq!(state.replies.len(), 2);
}

fn garbage_collect(config: ShardedStorageConfig, keys: impl IntoIterator<Item = String> + Clone) {
    let mut state = SystemState::with_config(config.clone());
    let mut seq = 0;
    for (i, key) in keys.clone().into_iter().enumerate() {
        seq += 1;
        state.receive(
            request(seq, KvOp::Put(key.clone(), format!("v{i}"))),
            Duration::ZERO,
        );
        seq += 1;
        state.receive(request(seq, KvOp::Get(key)), Duration::ZERO);
    }
    state.run(Duration::ZERO);
    assert_eq!(state.replies.len(), 6 * config.num_node as usize);
    for i in 0..keys.clone().into_iter().count() {
        let count = state
            .replies
            .iter()
            .filter(|(_, reply)| match &reply.res {
                KvRes::Put | KvRes::Get(None) => false,
                KvRes::Get(Some(v)) => v == &format!("v{i}"),
            })
            .count();
        assert_eq!(count, config.num_node as usize, "v{i}")
    }
    let mut active_count = 0;
    for (_, storage) in &state.hosts {
        active_count += storage.iter().filter(|(k, _)| k.contains('.')).count();
        let archive_count = storage.iter().filter(|(k, _)| k.contains('-')).count();
        assert_eq!(archive_count, config.num_stripe as usize)
    }
    assert_eq!(
        active_count,
        HashSet::<String>::from_iter(keys.into_iter()).len()
    )
}

#[test]
fn garbage_collect1() {
    garbage_collect(
        ShardedStorageConfig {
            num_node: 1,
            num_faulty_node: 0,
            num_stripe: 1,
            num_active_copy: 1,
            bypass_vote: true,
        },
        repeat_n("k".into(), 3),
    )
}

#[test]
fn garbage_collect4() {
    garbage_collect(
        ShardedStorageConfig {
            num_node: 4,
            num_faulty_node: 1,
            num_stripe: 1,
            num_active_copy: 1,
            bypass_vote: true,
        },
        repeat_n("k".into(), 3),
    )
}

#[test]
fn garbage_collect4_stripe2() {
    garbage_collect(
        ShardedStorageConfig {
            num_node: 4,
            num_faulty_node: 1,
            num_stripe: 2,
            num_active_copy: 1,
            bypass_vote: true,
        },
        repeat_n("k".into(), 3),
    )
}

#[test]
fn garbage_collect4_key3() {
    garbage_collect(
        ShardedStorageConfig {
            num_node: 4,
            num_faulty_node: 1,
            num_stripe: 1,
            num_active_copy: 1,
            bypass_vote: true,
        },
        (0..3).map(|i| format!("k{i}")),
    )
}
