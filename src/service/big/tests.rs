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

    fn progress(&mut self, index: ServiceIndex, since_start: Duration) -> Option<Duration> {
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
    state.progress(0, Duration::ZERO);
    let (_, reply) = state.replies.remove(0);
    assert_eq!(reply.res, vec![KvRes::Put]);
}
