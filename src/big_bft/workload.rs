use super::{DigestHash, Op, Txn};

#[derive(Debug, Clone)]
pub struct Workload {
    key_index: usize,
}

impl Default for Workload {
    fn default() -> Self {
        Self::new()
    }
}

impl Workload {
    pub fn new() -> Self {
        Self { key_index: 0 }
    }
}

impl Iterator for Workload {
    type Item = Txn;

    fn next(&mut self) -> Option<Self::Item> {
        let mut hash = DigestHash::default();
        hash[..size_of::<usize>()].copy_from_slice(&self.key_index.to_be_bytes());
        self.key_index = (self.key_index + 1) % 100;
        Some(Txn(vec![Op::Read(hash)]))
    }
}

pub fn initial_state() -> impl Iterator<Item = (DigestHash, String)> {
    (0..100usize).map(|index| {
        let mut hash = DigestHash::default();
        hash[..size_of::<usize>()].copy_from_slice(&index.to_be_bytes());
        (hash, format!("value{index}"))
    })
}
