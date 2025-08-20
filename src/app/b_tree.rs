use std::collections::BTreeMap;

use crate::service::ServiceApp;

use super::{
    AppState,
    ycsb::{YcsbOp, YcsbRes},
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

impl ServiceApp for BTree {
    type Op = YcsbOp;
    type Res = YcsbRes;
}

impl AppState for BTree {
    fn execute(&mut self, op: Self::Op) -> Self::Res {
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
