use crate::{service::ServiceApp, workload::WorkloadState};

use super::AppState;

pub struct Null;

impl ServiceApp for Null {
    type Op = ();
    type Res = ();
}

impl AppState for Null {
    fn execute(&mut self, (): Self::Op) -> Self::Res {}
}

impl WorkloadState for Null {
    type App = Self;
    type Metadata = ();

    fn next_op(&mut self) -> Option<(<Self::App as ServiceApp>::Op, Self::Metadata)> {
        Some(((), ()))
    }
}
