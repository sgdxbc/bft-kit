use super::{ClientId, ClientSeq, Command};

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
        // using a VecDeque as a ring buffer
        pending_buf: VecDeque<Command>,
        client_seqs: HashMap<ClientId, ClientSeq>,
    }

    impl CommandPool {
        pub fn new() -> Self {
            Self::default()
        }

        const MAX_LEN: usize = 5000;

        pub fn push(&mut self, command: Command) -> bool {
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
    }
}

// close loop pool assumes assumes causal behavior of close loop clients: `push`
// a command of the same client with higher sequence number happens after all
// lower sequence numbers have been committed by the client
// a close loop pool guarantees liveness mentioned above. combining with the
// common ordering property this becomes fairness, which is critical for tail
// latency
pub mod close_loop {
    use std::{collections::HashMap, mem::replace};

    use super::{ClientId, ClientSeq, Command};

    #[derive(Debug, Default)]
    pub struct CommandPool {
        pending_queue: Vec<Command>,
        client_seqs: HashMap<ClientId, ClientSeq>,
    }

    impl CommandPool {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn push(&mut self, command: Command) -> bool {
            if self.client_seqs.get(&command.client_id) >= Some(&command.seq) {
                return false;
            }
            self.client_seqs.insert(command.client_id, command.seq);
            self.pending_queue.push(command);
            true
        }

        pub fn close_batch(&mut self, max_batch_size: usize) -> Option<Vec<Command>> {
            if self.pending_queue.is_empty() {
                None
            } else {
                let batch_size = max_batch_size.min(self.pending_queue.len());
                let remaining_queue = self.pending_queue.split_off(batch_size);
                Some(replace(&mut self.pending_queue, remaining_queue))
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
    }
}
