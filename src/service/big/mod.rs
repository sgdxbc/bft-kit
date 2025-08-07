use crate::{app::AppState, replication::ReplicationState};

pub struct Service<R: ReplicationState<A::Op>, A: AppState> {
    replication: R,
    app: A,
}
