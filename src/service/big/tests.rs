use test_log::test;

use crate::replication::unreplicated::Replica;

use super::{
    app::{DataShardingSchema, Kv, KvOp, KvRes},
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
    service_network: VecDeque<(ServiceIndex, ServiceMessage<A>)>,
    replies: Vec<(ClientId, Reply<Vec<KvRes>, ()>)>,
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
        service_network: Default::default(),
        replies: Default::default(),
    };
    let proceed = state.services[0].proceed(Duration::ZERO);
    assert!(matches!(proceed, Proceed::Pending(None)));
}

impl SystemState {
    fn deliver_messages(&mut self) {
        while let Some((index, message)) = self.service_network.pop_front() {
            self.services[index as usize]
                .receive(Message::Intermediate(IntermediateMessage::Service(message)))
        }
    }

    fn proceed_service(&mut self, index: ServiceIndex, since_start: Duration) -> Option<Duration> {
        loop {
            match self.services[index as usize].proceed(since_start) {
                Proceed::Pending(tick_after) => break tick_after,
                Proceed::Send(Send::Reply(client_id, reply)) => {
                    self.replies.push((client_id, reply))
                }
                Proceed::Send(Send::Intermediate(IntermediateSend::Service(
                    ServiceRecipient::Uni(index),
                    message,
                ))) => self.service_network.push_back((index, message)),
                Proceed::Send(Send::Intermediate(IntermediateSend::Service(
                    ServiceRecipient::Multi(indices),
                    message,
                ))) => {
                    for index in indices {
                        self.service_network.push_back((index, message.clone()))
                    }
                }
                Proceed::Send(Send::Intermediate(IntermediateSend::Service(
                    ServiceRecipient::All,
                    message,
                ))) => {
                    for index in 0..self.services.len() {
                        self.service_network
                            .push_back((index as _, message.clone()))
                    }
                }
            }
        }
    }
}

fn request(op: KvOp) -> Request<Vec<KvOp>> {
    Request {
        client_id: 0,
        client_seq: 0,
        op: vec![op],
    }
}

#[test]
fn one_service() {
    let mut state = SystemState {
        services: vec![Service::new(
            Replica::new(),
            DataShardingSchema::new(1),
            0,
            StateConfig {
                num_service: 1,
                num_active_copy: 1,
            },
        )],
        service_network: Default::default(),
        replies: Default::default(),
    };
    state.services[0].receive(Message::Request(request(KvOp::Put("k".into(), "v".into()))));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, vec![KvRes::Put]);
    state.services[0].receive(Message::Request(request(KvOp::Get("k".into()))));
    state.proceed_service(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, vec![KvRes::Get(Some("v".into()))]);
}

impl SystemState {
    fn new(num_service: ServiceIndex) -> Self {
        Self {
            services: (0..num_service)
                .map(|i| {
                    Service::new(
                        Replica::new(),
                        DataShardingSchema::new(10),
                        i,
                        StateConfig {
                            num_service,
                            num_active_copy: 1,
                        },
                    )
                })
                .collect(),
            service_network: Default::default(),
            replies: Default::default(),
        }
    }

    fn receive(&mut self, request: Request<Vec<KvOp>>) {
        for service in &mut self.services {
            service.receive(Message::Request(request.clone()));
        }
    }

    fn proceed(&mut self, since_start: Duration) -> Option<Duration> {
        loop {
            let mut min_tick_after = None;
            for index in 0..self.services.len() as ServiceIndex {
                let tick_after = self.proceed_service(index, since_start);
                min_tick_after =
                    if let (Some(tick_after), Some(min_tick_after)) = (tick_after, min_tick_after) {
                        Some(tick_after.min(min_tick_after))
                    } else {
                        min_tick_after.or(tick_after)
                    }
            }
        }
    }

    fn run(&mut self, since_start: Duration) -> Option<Duration> {
        let mut tick_after;
        while {
            tick_after = self.proceed(since_start);
            self.service_network.len() > 0
        } {
            self.deliver_messages()
        }
        tick_after
    }
}

#[test]
fn multiple_services() {
    let mut state = SystemState::new(2);
    state.receive(request(KvOp::Put("k".into(), "v".into())));
    state.run(Duration::ZERO);
    assert_eq!(state.replies.len(), 2);
}
