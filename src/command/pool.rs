use crate::{ClientId, ClientSeq, Command};

// "mempool" in cryptocurrency term
// assist (primary) replica to propose blocks with sensible commands
// note that command pool is not aware of protocol details and may return
// duplicated commands during primary change (on different primaries), so the
// replicated services still need to bring their own solutions for at most once
// semantic, but simply record the highest executed sequence numbers of each
// client should be sufficient
// both variants of command pool preserve the ordering. if a `command` is
// `push`ed later than the other one, it will not be returned by `close_batch`
// earlier
// they also ensure idempotent: if a command is `push`ed or `commit`ed, `push`
// it again will return false and it will not be returned by `close_batch` one
// more time for that
// finally, they are also bounded. even if `close_batch` is never called (such
// as the case of a backup replica), the maximum size of open loop pool is
// MAX_LEN while the maximum size of close loop pool is the number of concurrent
// clients as it precisely keeps at most one command per client
// the difference of the pools is noted below
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
            Self::OpenLoop(pool) => pool.commit(command),
        }
    }
}

// open loop pool allows multiple concurrent commands (sequence numbers) from
// the same client, but assumes that clients send commands with increasing
// sequence numbers
// unlike close loop pool, the open loop pool does _not_ guarantee liveness: a
// `push`ed command is not guaranteed to be returned by `close_batch` even if it
// is later re`push`ed. this is acceptable by the nature of open loop, where new
// commands are produced whether or not the old ones are committed
pub mod open_loop {
    use std::collections::{HashMap, VecDeque};

    use super::{ClientId, ClientSeq, Command};

    #[derive(Debug, Default)]
    pub struct CommandPool {
        pending_buf: VecDeque<Command>,
        client_seqs: HashMap<ClientId, ClientSeq>,
        num_push: usize,
        num_commit: usize,
    }

    impl CommandPool {
        pub fn new() -> Self {
            Self::default()
        }

        const MAX_LEN: usize = 5000;

        pub fn push(&mut self, command: Command) -> bool {
            self.num_push += 1;
            if matches!(self.client_seqs.get(&command.client_id), Some(&seq) if seq >= command.seq)
            {
                return false;
            }
            self.client_seqs.insert(command.client_id, command.seq);
            if self.pending_buf.len() == Self::MAX_LEN {
                self.pending_buf.pop_front();
            }
            self.pending_buf.push_back(command);
            true
        }

        pub fn close_batch(&mut self, max_batch_size: usize) -> Option<Vec<Command>> {
            assert!(max_batch_size <= Self::MAX_LEN);
            if self.pending_buf.is_empty() {
                None
            } else {
                // it seems a bit wasteful to drop all commands unconditionally, but as the
                // sending rate increases, replica can receive a batch worth of commands during
                // a batch interval eventually
                let num_skipped = self.pending_buf.len().max(max_batch_size) - max_batch_size;
                Some(self.pending_buf.drain(..).skip(num_skipped).collect())
            }
        }

        pub fn commit(&mut self, command: &Command) {
            self.num_commit += 1;
            let seq = self.client_seqs.entry(command.client_id).or_default();
            *seq = (*seq).max(command.seq)
            // it is possible to achieve "high resolution" command purging here, based on
            // the fact that the replicated service will not respect any future committed
            // commands from the same client with lower sequence numbers
            // however, based on how open loop works (e.g. implied by the `close_batch`
            // logic), those older commands are probably not in the buffer already
        }
    }

    impl Drop for CommandPool {
        fn drop(&mut self) {
            let commit_rate = self.num_commit as f32 / self.num_push as f32;
            tracing::info!(%commit_rate)
        }
    }
}

// close loop pool assumes assumes causal behavior of close loop clients: `push`
// a command of the same client with higher sequence number happens after all
// lower sequence numbers have been committed by the client
// a close loop pool guarantees liveness mentioned above. combining with the
// common ordering property this becomes fairness, which is critical for tail
// latency
pub mod close_loop {
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
        pub fn new() -> Self {
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
                    // in place replace defeats the ordering (and hence the fairness) a little bit,
                    // but it is simple and the damage is strictly limited by the fact that each
                    // close loop client has at most one active command
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
                    self.client_pending_offsets.remove(&command.client_id);
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
