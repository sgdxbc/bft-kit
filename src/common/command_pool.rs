use super::{ClientId, ClientSeq, Command};

// "mempool" in cryptocurrency term
pub enum CommandPool {
    CloseLoop(close_loop::CommandPool),
    OpenLoop(open_loop::CommandPool),
}

impl CommandPool {
    pub fn close_loop() -> Self {
        Self::CloseLoop(Default::default())
    }

    pub fn open_loop() -> Self {
        Self::OpenLoop(Default::default())
    }

    pub fn push(&mut self, command: Command) -> bool {
        match self {
            Self::CloseLoop(pool) => pool.push(command),
            Self::OpenLoop(pool) => pool.push(command),
        }
    }

    pub fn close_batch(&mut self, max_batch_size: usize) -> Option<Vec<Command>> {
        match self {
            Self::CloseLoop(pool) => pool.close_batch(max_batch_size),
            Self::OpenLoop(pool) => pool.close_batch(max_batch_size),
        }
    }

    pub fn commit(&mut self, command: &Command) {
        match self {
            Self::CloseLoop(pool) => pool.commit(command),
            Self::OpenLoop(_pool) => {}
        }
    }
}

// unlike close loop pool, the open loop pool does _not_ guarantee idempotent: a
// `push` with a command that is previously returned from `close_batch` will be
// pending again, and will later be returned by `close_batch` again
// this limitation is to prevent the pool to record for ever growing submitted
// requests, since we cannot compress the recording with sequence number as the
// close loop pool
// nevertheless, the idempotent is guaranteed before a command is submitted i.e.
// returned by `close_batch`. and nevertheless, we don't expect open loop
// clients to resend their requests in practice
mod open_loop {
    use std::collections::HashSet;

    use super::{ClientId, Command};

    #[derive(Debug, Default)]
    pub struct CommandPool {
        pending_buf: Vec<Command>,
        client_pending_offsets: HashSet<ClientId>,
    }

    impl CommandPool {
        pub fn push(&mut self, command: Command) -> bool {
            assert_eq!(command.seq, 1);
            if self.client_pending_offsets.insert(command.client_id) {
                self.pending_buf.push(command);
                true
            } else {
                false
            }
        }

        pub fn close_batch(&mut self, max_batch_size: usize) -> Option<Vec<Command>> {
            if self.pending_buf.is_empty() {
                None
            } else {
                self.client_pending_offsets.clear();
                let num_discarded = self.pending_buf.len().max(max_batch_size) - max_batch_size;
                Some(self.pending_buf.drain(..).skip(num_discarded).collect())
            }
        }
    }
}

mod close_loop {
    use std::{collections::HashMap, mem::swap};

    use super::{ClientId, ClientSeq, Command};

    #[derive(Debug, Default)]
    pub struct CommandPool {
        pending_queue: Vec<Command>,
        pending_queue_offset: usize,
        client_pending_offsets: HashMap<ClientId, usize>,
        submitted_seqs: HashMap<ClientId, ClientSeq>,
    }

    impl CommandPool {
        #[cfg(test)]
        fn new() -> Self {
            Self::default()
        }

        fn client_pending_index(&self, client_id: ClientId) -> Option<usize> {
            Some(self.client_pending_offsets.get(&client_id)? - self.pending_queue_offset)
        }

        pub fn push(&mut self, command: Command) -> bool {
            if self.submitted_seqs.get(&command.client_id) >= Some(&command.seq) {
                return false;
            }
            if let Some(pending_index) = self.client_pending_index(command.client_id) {
                let pending_command = &mut self.pending_queue[pending_index];
                assert_eq!(pending_command.client_id, command.client_id);
                if command.seq > pending_command.seq {
                    // pending command has been committed by other replicas; we are out of sync with
                    // majority's view
                    // in place replace is not completely "fair", but it is simple and we only need
                    // to ensure basic fairness i.e. client liveness
                    *pending_command = command;
                    true
                } else {
                    false
                }
            } else {
                self.client_pending_offsets.insert(
                    command.client_id,
                    self.pending_queue_offset + self.pending_queue.len(),
                );
                self.pending_queue.push(command);
                true
            }
        }

        pub fn close_batch(&mut self, max_batch_size: usize) -> Option<Vec<Command>> {
            if self.pending_queue.is_empty() {
                None
            } else {
                let batch_size = max_batch_size.min(self.pending_queue.len());
                let mut commands = self.pending_queue.split_off(batch_size);
                swap(&mut commands, &mut self.pending_queue);
                self.pending_queue_offset += batch_size;
                // for (i, request) in requests.iter().enumerate() {
                for command in &commands {
                    let client_id = command.client_id;
                    // assert_eq!(self.client_pending_index(client_id), Some(i));
                    self.client_pending_offsets.remove(&client_id);
                    let replaced = self.submitted_seqs.insert(client_id, command.seq);
                    assert!(replaced < Some(command.seq))
                }
                Some(commands)
            }
        }

        pub fn commit(&mut self, command: &Command) {
            let seq = self.submitted_seqs.entry(command.client_id).or_default();
            // committed sequence may be less than submitted sequence
            // is it necessary to separately count the two sequences?
            *seq = (*seq).max(command.seq);
            if let Some(pending_index) = self.client_pending_index(command.client_id) {
                if self.pending_queue[pending_index].seq <= command.seq {
                    self.pending_queue.swap_remove(pending_index);
                    if let Some(swapped_request) = self.pending_queue.get(pending_index) {
                        self.client_pending_offsets.insert(
                            swapped_request.client_id,
                            self.pending_queue_offset + pending_index,
                        );
                    }
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn command(seq: ClientSeq) -> Command {
            Command {
                client_id: ClientId(0),
                seq,
                op: Default::default(),
            }
        }

        #[test]
        fn normal() {
            let mut pool = CommandPool::new();
            pool.push(command(1));
            let batch = pool.close_batch(1);
            let Some([command]) = batch.as_deref() else {
                unreachable!()
            };
            assert_eq!(command.seq, 1)
        }

        #[test]
        fn resend_pending() {
            let mut pool = CommandPool::new();
            pool.push(command(1));
            let pushed2 = pool.push(command(1));
            assert!(!pushed2);
            let batch = pool.close_batch(100);
            assert_eq!(batch.map(|batch| batch.len()), Some(1))
        }

        #[test]
        fn resend_submitted() {
            let mut pool = CommandPool::new();
            pool.push(command(1));
            let _batch = pool.close_batch(100);
            let pushed2 = pool.push(command(1));
            assert!(!pushed2);
            let batch2 = pool.close_batch(100);
            assert!(batch2.is_none())
        }

        #[test]
        fn resend_committed() {
            let mut pool = CommandPool::new();
            pool.commit(&command(1));
            let pushed = pool.push(command(1));
            assert!(!pushed);
            let batch = pool.close_batch(100);
            assert!(batch.is_none())
        }

        #[test]
        fn stalled_pending_superior_push() {
            let mut pool = CommandPool::new();
            pool.push(command(1));
            let pushed = pool.push(command(2));
            assert!(pushed);
            let batch = pool.close_batch(100);
            let Some([command]) = batch.as_deref() else {
                unreachable!()
            };
            assert_eq!(command.seq, 2)
        }

        fn stalled_pending_commit(seq: ClientSeq) {
            let mut pool = CommandPool::new();
            pool.push(command(1));
            pool.commit(&command(seq));
            let batch = pool.close_batch(100);
            assert!(batch.is_none())
        }

        #[test]
        fn stalled_pending_commit_1() {
            stalled_pending_commit(1)
        }

        #[test]
        fn stalled_pending_commit_2() {
            stalled_pending_commit(2)
        }

        #[test]
        fn stalled_pending_commit_multiple_clients() {
            let mut pool = CommandPool::new();
            pool.push(command(1));
            pool.push(Command {
                client_id: ClientId(1),
                seq: 1,
                op: Default::default(),
            });
            pool.commit(&command(1));
            let batch = pool.close_batch(100);
            let Some([command]) = batch.as_deref() else {
                unreachable!()
            };
            assert_eq!(command.client_id, ClientId(1))
        }

        #[test]
        fn commit_remote() {
            let mut pool = CommandPool::new();
            pool.commit(&command(1))
        }
    }
}
