use crate::{app::AppState, workload::WorkloadState};

pub struct Null;

impl AppState for Null {
    type Op = ();
    type Res = ();
    fn update(&mut self, &(): &Self::Op) -> Self::Res {}
}

impl WorkloadState for Null {
    type Op = ();
    type Res = ();

    fn next_op(&mut self) -> Option<Self::Op> {
        Some(())
    }

    fn validate(&self, (): Self::Op, (): Self::Res) -> anyhow::Result<()> {
        Ok(())
    }
}
