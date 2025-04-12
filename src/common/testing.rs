use std::{collections::VecDeque, fmt::Debug, iter::repeat, mem::take};

use super::{AbstractReplica, ClientSeq, Command, ReplicaId};

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

pub trait Effect<S> {
    fn effect(self, replica_id: ReplicaId, system: &mut S, actions: &mut Actions);
}

impl<R: AbstractReplica> System<R, R::Message> {
    pub fn init(&mut self)
    where
        R::Action: Effect<Self>,
    {
        let mut actions = Vec::new();
        for replica_id in 0..self.replicas.len() as ReplicaId {
            let mut replica_actions = Vec::new();
            self.replicas[replica_id as usize].init(&mut replica_actions);
            self.effect(replica_id, replica_actions, &mut actions);
            assert!(actions.is_empty())
        }
    }

    pub fn request(&mut self, replica_id: ReplicaId, command: Command)
    where
        R::Action: Effect<Self>,
    {
        let mut replica_actions = Vec::new();
        self.replicas[replica_id as usize].request(command, &mut replica_actions);
        let mut actions = Vec::new();
        self.effect(replica_id, replica_actions, &mut actions);
        assert!(actions.is_empty())
    }

    pub fn step(&mut self, actions: &mut Actions) -> bool
    where
        R::Action: Effect<Self>,
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
    ) where
        R::Action: Effect<Self>,
    {
        while !replica_actions.is_empty() {
            for action in take(&mut replica_actions) {
                action.effect(replica_id, self, actions)
            }
        }
    }

    pub fn exhaust(&mut self, max_num_step: u32, actions: &mut Actions)
    where
        R::Action: Effect<Self>,
        R::Message: Debug,
    {
        for _ in 0..max_num_step {
            if !self.step(actions) {
                return;
            }
        }
        unreachable!()
    }

    pub fn filter_exhaust(
        &mut self,
        skip: impl Fn(&Event<R::Message>) -> bool,
        max_num_step: u32,
        actions: &mut Actions,
    ) where
        R::Action: Effect<Self>,
        R::Message: Debug,
    {
        let mut num_step = 0;
        while let Some(event) = self.events.front() {
            if skip(event) {
                self.events.pop_front();
                continue;
            }
            assert!(num_step < max_num_step);
            num_step += 1;
            self.step(actions);
        }
    }
}

pub fn is_finalized(
    actions: &[Action],
    command: Command,
    num_replica: ReplicaId,
    num_finalize: usize,
) -> bool {
    (0..num_replica)
        .filter(|replica_id| {
            actions.iter().any(|action| {
                matches!(action, Action::Finalize(
                    id,
                    commands,
                ) if id == replica_id && commands.contains(&command))
            })
        })
        .count()
        >= num_finalize
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
    R::Action: Effect<Self>,
    R::Message: Debug,
{
    pub fn num_replica(&self) -> ReplicaId {
        self.replicas.len() as _
    }

    pub fn normal_1(system: &mut Self) {
        system.init();
        let mut actions = Vec::new();
        system.exhaust(100, &mut actions);

        system.request(0, Command::new(0, 1));
        system.exhaust(100, &mut actions);
        for replica_id in 0..system.num_replica() {
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
                if is_finalized(
                    &actions,
                    Command::new(0, seq),
                    system.num_replica(),
                    num_finalize,
                ) {
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
        System::concurrent_clients_with_step_thresholds(
            system,
            num_client,
            num_finalize,
            repeat(100),
        )
    }

    pub fn concurrent_clients_with_step_thresholds(
        system: &mut Self,
        num_client: u32,
        num_finalize: usize,
        thresholds: impl IntoIterator<Item = u32>,
    ) {
        system.init();
        let mut actions = Vec::new();
        system.exhaust(100, &mut actions);

        for client_id in 0..num_client {
            system.request(0, Command::new(client_id, 1));
        }
        let mut thresholds = thresholds.into_iter();
        for client_id in 0..num_client {
            let threshold = thresholds.next();
            for num_step in 0.. {
                if is_finalized(
                    &actions,
                    Command::new(client_id, 1),
                    system.num_replica(),
                    num_finalize,
                ) {
                    break;
                }
                assert!(Some(num_step) < threshold);
                system.step(&mut actions);
            }
        }
        let logs = replica_commands(actions, system.num_replica());
        for i in 0..logs.iter().map(|log| log.len()).min().unwrap() {
            assert!(logs.iter().skip(1).all(|log| log[i] == logs[0][i]))
        }
    }
}
