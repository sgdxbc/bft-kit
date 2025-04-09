use std::{collections::VecDeque, fmt::Debug, mem::take};

use super::{ClientSeq, Command, ReplicaId};

pub struct System<R, M> {
    pub replicas: Vec<R>,
    pub events: VecDeque<Event<M>>,
}

#[derive(Debug)]
pub enum Event<M> {
    SendToReplica(ReplicaId, M),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Action {
    Finalize(ReplicaId, Vec<Command>),
}

pub type Actions = Vec<Action>;

pub trait AbstractReplica: Sized {
    type Action: Effect<System<Self, Self::Message>>;
    type Message;

    fn init(&mut self, actions: &mut Vec<Self::Action>);
    fn request(&mut self, command: Command, actions: &mut Vec<Self::Action>);
    fn receive(&mut self, message: Self::Message, actions: &mut Vec<Self::Action>);
}

pub trait Effect<S> {
    fn effect(self, replica_id: ReplicaId, system: &mut S, actions: &mut Actions);
}

impl<R: AbstractReplica> System<R, R::Message> {
    pub fn init(&mut self) {
        let mut actions = Vec::new();
        for replica_id in 0..self.replicas.len() as ReplicaId {
            let mut replica_actions = Vec::new();
            self.replicas[replica_id as usize].init(&mut replica_actions);
            self.effect(replica_id, replica_actions, &mut actions);
            assert!(actions.is_empty())
        }
    }

    pub fn request(&mut self, replica_id: ReplicaId, command: Command) {
        let mut replica_actions = Vec::new();
        self.replicas[replica_id as usize].request(command, &mut replica_actions);
        let mut actions = Vec::new();
        self.effect(replica_id, replica_actions, &mut actions);
        assert!(actions.is_empty())
    }

    pub fn step(&mut self, actions: &mut Actions) -> bool
    where
        R::Message: Debug,
    {
        let Some(event) = self.events.pop_front() else {
            return false;
        };
        tracing::debug!(?event);
        let Event::SendToReplica(replica_id, message) = event;
        let mut replica_actions = Vec::new();
        self.replicas[replica_id as usize].receive(message, &mut replica_actions);
        self.effect(replica_id, replica_actions, actions);
        true
    }

    pub fn effect(
        &mut self,
        replica_id: ReplicaId,
        mut replica_actions: Vec<R::Action>,
        actions: &mut Actions,
    ) {
        while !replica_actions.is_empty() {
            for action in take(&mut replica_actions) {
                action.effect(replica_id, self, actions)
            }
        }
    }

    pub fn exhaust(&mut self, max_num_step: u32, actions: &mut Actions)
    where
        R::Message: Debug,
    {
        for _ in 0..max_num_step {
            if !self.step(actions) {
                return;
            }
        }
        unreachable!()
    }
}

fn replica_commands(actions: Actions, num_replica: ReplicaId) -> Vec<Vec<Command>> {
    let mut categories = vec![Vec::new(); num_replica as _];
    for action in actions {
        let Action::Finalize(replica_id, commands) = action;
        categories[replica_id as usize].extend(commands)
    }
    categories
}

impl<R: AbstractReplica> System<R, R::Message>
where
    R::Message: Debug,
{
    fn num_replica(&self) -> ReplicaId {
        self.replicas.len() as _
    }

    pub fn normal_1(system: &mut Self) {
        system.init();
        let mut actions = Vec::new();
        system.exhaust(100, &mut actions);

        system.request(0, Command::new(0, 1));
        system.exhaust(100, &mut actions);
        for replica_id in 0..system.replicas.len() as ReplicaId {
            assert!(actions.contains(&Action::Finalize(replica_id, vec![Command::new(0, 1)])))
        }
        assert_eq!(actions.len(), system.replicas.len())
    }

    pub fn close_loop(system: &mut Self, num_command: ClientSeq, num_finalize: usize) {
        system.init();
        let mut actions = Vec::new();
        system.exhaust(100, &mut actions);

        for seq in 1..=num_command {
            system.request(0, Command::new(0, seq));
            for num_step in 0.. {
                assert!(num_step < 100);
                system.step(&mut actions);
                if (0..system.num_replica())
                    .filter(|&replica_id| {
                        actions.contains(&Action::Finalize(replica_id, vec![Command::new(0, seq)]))
                    })
                    .count()
                    >= num_finalize
                {
                    break;
                }
            }
        }
        let logs = replica_commands(actions, system.num_replica());
        for replica_id in 0..system.num_replica() {
            for (i, replica_action) in logs[replica_id as usize].iter().enumerate() {
                assert_eq!(replica_action, &Command::new(0, (i + 1) as _))
            }
        }
    }

    pub fn concurrent_clients(system: &mut Self, num_client: u32, num_finalize: usize) {
        system.init();
        let mut actions = Vec::new();
        system.exhaust(100, &mut actions);

        for client_id in 0..num_client {
            system.request(0, Command::new(client_id, 1));
        }
        for client_id in 0..num_client {
            for num_step in 0.. {
                if (0..system.num_replica())
                    .filter(|replica_id| {
                        actions.iter().any(|action| {
                            matches!(action, Action::Finalize(
                                id,
                                commands,
                            ) if id == replica_id && commands.contains(&Command::new(client_id, 1)))
                        })
                    })
                    .count()
                    >= num_finalize
                {
                    break;
                }
                assert!(num_step < 100);
                system.step(&mut actions);
            }
        }
        let logs = replica_commands(actions, system.num_replica());
        for i in 0..logs.iter().map(|log| log.len()).min().unwrap() {
            assert!(logs.iter().skip(1).all(|log| log[i] == logs[0][i]))
        }
    }
}
