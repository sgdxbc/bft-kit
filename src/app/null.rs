use crate::{Never, workload::WorkloadState};

use super::{
    AppProtocol, AppState, DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState,
};

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

impl DataShardingApp for Null {
    type Key = Never;
    type Value = Never;
    type ExecuteState = Null;
    fn new_execute(&self, (): Self::Op) -> Self::ExecuteState {
        Null
    }
}

impl DataShardingExecuteState for Null {
    type App = Null;
    fn get_ok(
        &mut self,
        _key: <Self::App as DataShardingApp>::Key,
        _value: <Self::App as DataShardingApp>::Value,
    ) {
        unreachable!()
    }
    fn proceed(&mut self) -> DataShardingExecuteOutput<Self::App> {
        DataShardingExecuteOutput::Complete(())
    }
}
