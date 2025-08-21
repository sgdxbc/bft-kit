use crate::workload::WorkloadState;

use super::{AppProtocol, AppState};

pub struct Null;

impl AppProtocol for Null {
    type Op = ();
    type Res = ();
}

impl AppState for Null {
    type Protocol = Self;
    fn execute(
        &mut self,
        (): <Self::Protocol as AppProtocol>::Op,
    ) -> <Self::Protocol as AppProtocol>::Res {
    }
}

impl WorkloadState for Null {
    type Protocol = Self;
    type Metadata = ();

    fn next_op(&mut self) -> Option<(<Self::Protocol as AppProtocol>::Op, Self::Metadata)> {
        Some(((), ()))
    }
}
