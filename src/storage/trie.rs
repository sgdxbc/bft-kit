use std::sync::Arc;

use rocksdb::DB;
use tokio_util::bytes::Bytes;

use crate::crypto::Digest;

use super::StorageKey;

pub struct StatefulTrie {
    db: Arc<DB>,
    prefix: String,
}

pub struct Proof {
    //
}

impl StatefulTrie {
    pub fn get(&self, key: &StorageKey) -> Option<(Bytes, Proof)> {
        let mut db_key = self.prefix.clone();
        todo!()
    }
}

pub struct StatelessTrie {
    root: Digest,
}
