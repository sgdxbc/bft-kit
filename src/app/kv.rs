use std::collections::HashMap;

use super::AppState;

pub struct Kv {
    store: HashMap<String, String>,
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

impl AppState for Kv {
    type Op = KvOp;
    type Res = KvRes;

    fn execute(&mut self, op: &Self::Op) -> Self::Res {
        match op {
            KvOp::Insert(key, value) => {
                self.store.insert(key.clone(), value.clone());
                KvRes::InsertOk
            }
            KvOp::Update(key, value) => match self.store.get_mut(key) {
                Some(existing_value) => {
                    *existing_value = value.clone();
                    KvRes::UpdateOk
                }
                None => KvRes::NotFound,
            },
            KvOp::Get(key) => match self.store.get(key) {
                Some(value) => KvRes::GetOk(value.clone()),
                None => KvRes::NotFound,
            },
        }
    }
}
