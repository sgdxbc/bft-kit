use std::collections::BTreeMap;

use super::AppState;

pub struct Db {
    store: BTreeMap<String, String>,
}

impl Db {
    pub fn new() -> Self {
        Self {
            store: Default::default(),
        }
    }
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

pub enum YcsbOp {
    Insert(String, String),
    Update(String, String),
    Get(String),
    Scan(String, usize),
}

pub enum YcsbRes {
    Ok,
    GetResult(String),
    ScanResult(Vec<(String, String)>),
    NotFound,
}

impl AppState for Db {
    type Op = YcsbOp;
    type Res = YcsbRes;

    fn execute(&mut self, op: &Self::Op) -> Self::Res {
        match op {
            YcsbOp::Insert(key, value) => {
                self.store.insert(key.clone(), value.clone());
                YcsbRes::Ok
            }
            YcsbOp::Update(key, value) => match self.store.get_mut(key) {
                Some(existing_value) => {
                    *existing_value = value.clone();
                    YcsbRes::Ok
                }
                None => YcsbRes::NotFound,
            },
            YcsbOp::Get(key) => match self.store.get(key) {
                Some(value) => YcsbRes::GetResult(value.clone()),
                None => YcsbRes::NotFound,
            },
            YcsbOp::Scan(prefix, limit) => YcsbRes::ScanResult(
                self.store
                    .range(prefix.clone()..)
                    .take(*limit)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect(),
            ),
        }
    }
}
