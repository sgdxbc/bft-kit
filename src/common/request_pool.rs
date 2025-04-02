use std::collections::HashMap;

use super::{ClientId, ClientSeq, Request};

// "mempool" in cryptocurrency term
#[derive(Debug, Default)]
pub struct RequestPool {
    pending_queue: Vec<Request>,
    pending_queue_offset: usize,
    client_pending_offsets: HashMap<ClientId, usize>,
    submitted_seqs: HashMap<ClientId, ClientSeq>,
}

impl RequestPool {
    pub fn new() -> Self {
        Self::default()
    }

    fn client_pending_index(&self, client_id: ClientId) -> Option<usize> {
        Some(self.client_pending_offsets.get(&client_id)? - self.pending_queue_offset)
    }

    pub fn push(&mut self, request: Request) -> bool {
        if self.submitted_seqs.get(&request.client_id) >= Some(&request.seq) {
            return false;
        }
        if let Some(pending_index) = self.client_pending_index(request.client_id) {
            let pending_request = &mut self.pending_queue[pending_index];
            assert_eq!(pending_request.client_id, request.client_id);
            if request.seq > pending_request.seq {
                // pending request has been committed by other replicas; we are out of sync with
                // majority's view
                // in place replace is not completely "fair", but it is simple and we only need
                // to ensure basic fairness i.e. client liveness
                *pending_request = request;
                true
            } else {
                false
            }
        } else {
            self.client_pending_offsets.insert(
                request.client_id,
                self.pending_queue_offset + self.pending_queue.len(),
            );
            self.pending_queue.push(request);
            true
        }
    }

    pub fn close_batch(&mut self, max_batch_size: usize) -> Option<Vec<Request>> {
        if self.pending_queue.is_empty() {
            None
        } else {
            let batch_size = max_batch_size.min(self.pending_queue.len());
            for (i, request) in self.pending_queue[..batch_size].iter().enumerate() {
                let client_id = request.client_id;
                assert_eq!(self.client_pending_index(client_id), Some(i));
                self.client_pending_offsets.remove(&client_id);
                let replaced = self.submitted_seqs.insert(client_id, request.seq);
                assert!(replaced < Some(request.seq))
            }
            self.pending_queue_offset += batch_size;
            Some(self.pending_queue.drain(..batch_size).collect())
        }
    }

    pub fn commit(&mut self, request: &Request) {
        let seq = self.submitted_seqs.entry(request.client_id).or_default();
        // committed sequence may be less than submitted sequence
        // is it necessary to separately count the two sequences?
        *seq = (*seq).max(request.seq);
        if let Some(pending_index) = self.client_pending_index(request.client_id) {
            if self.pending_queue[pending_index].seq <= request.seq {
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

    fn request(seq: ClientSeq) -> Request {
        Request {
            client_id: ClientId(0),
            seq,
            op: Default::default(),
        }
    }

    #[test]
    fn normal() {
        let mut pool = RequestPool::new();
        pool.push(request(1));
        let batch = pool.close_batch(1);
        let Some([request]) = batch.as_deref() else {
            unreachable!()
        };
        assert_eq!(request.seq, 1)
    }

    #[test]
    fn resend_pending() {
        let mut pool = RequestPool::new();
        pool.push(request(1));
        let pushed2 = pool.push(request(1));
        assert!(!pushed2);
        let batch = pool.close_batch(100);
        assert_eq!(batch.map(|batch| batch.len()), Some(1))
    }

    #[test]
    fn resend_submitted() {
        let mut pool = RequestPool::new();
        pool.push(request(1));
        let _batch = pool.close_batch(100);
        let pushed2 = pool.push(request(1));
        assert!(!pushed2);
        let batch2 = pool.close_batch(100);
        assert!(batch2.is_none())
    }

    #[test]
    fn resend_committed() {
        let mut pool = RequestPool::new();
        pool.commit(&request(1));
        let pushed = pool.push(request(1));
        assert!(!pushed);
        let batch = pool.close_batch(100);
        assert!(batch.is_none())
    }

    #[test]
    fn stalled_pending_superior_push() {
        let mut pool = RequestPool::new();
        pool.push(request(1));
        let pushed = pool.push(request(2));
        assert!(pushed);
        let batch = pool.close_batch(100);
        let Some([request]) = batch.as_deref() else {
            unreachable!()
        };
        assert_eq!(request.seq, 2)
    }

    fn stalled_pending_commit(seq: ClientSeq) {
        let mut pool = RequestPool::new();
        pool.push(request(1));
        pool.commit(&request(seq));
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
        let mut pool = RequestPool::new();
        pool.push(request(1));
        pool.push(Request {
            client_id: ClientId(1),
            seq: 1,
            op: Default::default(),
        });
        pool.commit(&request(1));
        let batch = pool.close_batch(100);
        let Some([request]) = batch.as_deref() else {
            unreachable!()
        };
        assert_eq!(request.client_id, ClientId(1))
    }

    #[test]
    fn commit_remote() {
        let mut pool = RequestPool::new();
        pool.commit(&request(1))
    }
}
