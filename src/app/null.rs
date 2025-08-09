use crate::{app::AppState, workload::WorkloadState};

pub struct Null;

impl AppState for Null {
    type Op = ();
    type Res = ();
    fn update(&mut self, &(): &Self::Op) -> Self::Res {}
}

impl WorkloadState for Null {
    type App = Self;

    fn next_op(&mut self) -> Option<<Self::App as AppState>::Op> {
        Some(())
    }

    fn validate(
        &self,
        (): <Self::App as AppState>::Op,
        (): <Self::App as AppState>::Res,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
