use std::collections::BTreeMap;

use super::{
    AppProtocol, AppState,
    ycsb::{Ycsb, YcsbOp, YcsbRes},
};

pub struct BTree {
    store: BTreeMap<String, String>,
}

impl BTree {
    pub fn new() -> Self {
        Self {
            store: Default::default(),
        }
    }
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState for BTree {
    type Protocol = Ycsb;

    fn execute(
        &mut self,
        op: <Self::Protocol as AppProtocol>::Op,
    ) -> <Self::Protocol as AppProtocol>::Res {
        match op {
            YcsbOp::Insert(key, value) => {
                self.store.insert(key, value);
                YcsbRes::Ok
            }
            YcsbOp::Update(key, value) => match self.store.get_mut(&key) {
                Some(existing_value) => {
                    *existing_value = value;
                    YcsbRes::Ok
                }
                None => YcsbRes::NotFound,
            },
            YcsbOp::Get(key) => match self.store.get(&key) {
                Some(value) => YcsbRes::Get(value.clone()),
                None => YcsbRes::NotFound,
            },
            YcsbOp::Scan(prefix, limit) => YcsbRes::Scan(
                self.store
                    .range(prefix..)
                    .take(limit)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        }
    }
}
