use std::backtrace::BacktraceStatus;

use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct TaskGroup(pub CancellationToken);

impl TaskGroup {
    pub async fn wrap_fallible(self, task: impl Future<Output = anyhow::Result<()>>) {
        if let Some(Err(err)) = self.0.run_until_cancelled(task).await {
            tracing::error!(%err);
            let backtrace = err.backtrace();
            if backtrace.status() == BacktraceStatus::Captured {
                tracing::error!(%backtrace);
            }
            self.0.cancel()
        }
    }
}
