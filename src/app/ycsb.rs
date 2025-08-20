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

// TODO workload
