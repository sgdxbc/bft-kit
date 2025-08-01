use crate::state::State;

pub trait ClientState<Op>: State {
    fn submit(&mut self, op: Op);
}
