pub enum YcsbOp {
    Insert(String, String),
    Update(String, String),
    Get(String),
    Scan(String, usize),
}

pub enum YcsbRes {
    Ok,
    Err(String),
    Get(String),
    NotFound,
    Scan(Vec<(String, String)>),
}

// TODO workload
