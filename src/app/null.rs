use crate::workload::WorkloadState;

use super::{AppProtocol, AppState};

pub struct Null;

impl AppProtocol for Null {
    type Op = ();
    type Res = ();
}

impl AppState for Null {
    fn execute(&mut self, (): Self::Op) -> Self::Res {}
}

impl WorkloadState for Null {
    type Metadata = ();

    fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)> {
        Some(((), ()))
    }
}
