use std::collections::BTreeMap;

use super::AppState;

pub struct Kv {
    store: BTreeMap<String, String>,
}

impl Kv {
    pub fn new() -> Self {
        Self {
            store: Default::default(),
        }
    }
}

impl Default for Kv {
    fn default() -> Self {
        Self::new()
    }
}

pub enum KvOp {
    Insert(String, String),
    Update(String, String),
    Get(String),
}

pub enum KvRes {
    InsertOk,
    UpdateOk,
    GetOk(String),
    NotFound,
}

impl Kv {
    pub fn execute_with_store(op: &KvOp, store: &mut BTreeMap<String, String>) -> KvRes {
        match op {
            KvOp::Insert(key, value) => {
                store.insert(key.clone(), value.clone());
                KvRes::InsertOk
            }
            KvOp::Update(key, value) => match store.get_mut(key) {
                Some(existing_value) => {
                    *existing_value = value.clone();
                    KvRes::UpdateOk
                }
                None => KvRes::NotFound,
            },
            KvOp::Get(key) => match store.get(key) {
                Some(value) => KvRes::GetOk(value.clone()),
                None => KvRes::NotFound,
            },
        }
    }
}

impl AppState for Kv {
    type Op = KvOp;
    type Res = KvRes;

    fn execute(&mut self, op: &Self::Op) -> Self::Res {
        Self::execute_with_store(op, &mut self.store)
    }
}
