use crate::replication::unreplicated::Replica;

use super::{
    app::{DataShardingSchema, Kv, KvOp},
    *,
};

#[test]
fn output_service_indices_of() {
    let config = StateConfig {
        num_service: 100,
        num_active_copy: 7,
    };
    println!("{:2?}", config.service_indices_of(0));
    println!("{:2?}", config.service_indices_of(1));
    println!("{:2?}", config.service_indices_of(2));
    println!("{:2?}", config.service_indices_of(3))
}

type A = DataShardingSchema<Kv>;
type R = Replica<Request<Vec<KvOp>>>;

struct SystemState {
    services: Vec<Service<A, R>>,
    network: VecDeque<(ServiceIndex, ServiceMessage<KvOp, Message<A>, Never>)>,
}

#[test]
fn idle_pending() {
    let service = Service::new(
        Replica::new(),
        DataShardingSchema::new(1),
        0,
        StateConfig {
            num_service: 1,
            num_active_copy: 1,
        },
    );
    let mut state = SystemState {
        services: vec![service],
        network: Default::default(),
    };
    let proceed = state.services[0].proceed(Duration::ZERO);
    assert!(matches!(proceed, Proceed::Pending(None)));
}
