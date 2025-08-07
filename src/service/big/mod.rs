use crate::{app::ShardedAppState, replication::ReplicationState};

pub struct Service<R: ReplicationState<A::Op>, A: ShardedAppState> {
    replication: R,
    app: A,
}
