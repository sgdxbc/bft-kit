use bincode::{Decode, Encode};

use super::{AppProtocol, DataShardingApp, DataShardingExecuteOutput, DataShardingExecuteState};

pub struct Kv;

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum KvOp {
    Put(String, String),
    Get(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum KvRes {
    Put,
    Get(Option<String>),
}

impl AppProtocol for Kv {
    type Op = KvOp;
    type Res = KvRes;
}

impl DataShardingApp for Kv {
    type Key = String;
    type Value = String;
    type ExecuteState = KvExecute;
    fn new_execute(&self, op: Self::Op) -> Self::ExecuteState {
        match op {
            KvOp::Put(key, value) => KvExecute::ToPut(key, value),
            KvOp::Get(key) => KvExecute::ToGet(key),
        }
    }
}

pub enum KvExecute {
    ToPut(String, String),
    ToGet(String),
    Getting(String),
    Res(KvRes),
}

impl DataShardingExecuteState for KvExecute {
    type App = Kv;
    fn get_result(
        &mut self,
        key: <Self::App as DataShardingApp>::Key,
        value: Option<<Self::App as DataShardingApp>::Value>,
    ) {
        match self {
            KvExecute::Getting(get_key) if get_key == &key => {
                *self = KvExecute::Res(KvRes::Get(value));
            }
            _ => unimplemented!(),
        }
    }
    fn proceed(&mut self) -> DataShardingExecuteOutput<Self::App> {
        match self {
            Self::Getting(_) => DataShardingExecuteOutput::Pending,
            Self::Res(res) => DataShardingExecuteOutput::Complete(res.clone()),
            KvExecute::ToPut(key, value) => {
                let output = DataShardingExecuteOutput::Put(key.clone(), value.clone());
                *self = KvExecute::Res(KvRes::Put);
                output
            }
            KvExecute::ToGet(key) => {
                let output = DataShardingExecuteOutput::Get(key.clone());
                *self = KvExecute::Getting(key.clone());
                output
            }
        }
    }
}

pub mod ycsb {
    use crate::{
        app::{
            AppProtocol,
            ycsb::{YcsbOp, YcsbRes},
        },
        workload::WorkloadState,
    };

    use super::{KvOp, KvRes};

    pub struct AdaptKv<W>(pub W);

    impl<W> AppProtocol for AdaptKv<W> {
        type Op = KvOp;
        type Res = KvRes;
    }

    impl<W: WorkloadState<Op = YcsbOp, Res = YcsbRes>> WorkloadState for AdaptKv<W> {
        type Metadata = W::Metadata;

        fn next_op(&mut self) -> Option<(Self::Op, Self::Metadata)> {
            let (op, metadata) = self.0.next_op()?;
            let op = match op {
                YcsbOp::Insert(key, value) | YcsbOp::Update(key, value) => KvOp::Put(key, value),
                YcsbOp::Get(key) => KvOp::Get(key),
                _ => unimplemented!(),
            };
            Some((op, metadata))
        }

        fn complete(&mut self, metadata: Self::Metadata, res: Self::Res) -> anyhow::Result<()> {
            let res = match res {
                KvRes::Put => YcsbRes::Ok,
                KvRes::Get(Some(value)) => YcsbRes::Get(value),
                KvRes::Get(None) => YcsbRes::NotFound,
            };
            self.0.complete(metadata, res)
        }
    }
}
