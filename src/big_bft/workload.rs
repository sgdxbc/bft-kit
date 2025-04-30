use super::Txn;

#[derive(Debug, Clone)]
pub struct Workload {
    //
}

impl Iterator for Workload {
    type Item = Txn;

    fn next(&mut self) -> Option<Self::Item> {
        None // TODO
    }
}
