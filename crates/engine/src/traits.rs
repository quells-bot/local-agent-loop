use crate::{ActivityLease, CreateOutcome, ExecStatus, RunMeta, StoredEvent, TurnCommit};
use workflow::CommandResult;

/// History store + atomic decision-turn commit (spec §15).
#[async_trait::async_trait]
pub trait History: Send + Sync {
    /// Idempotent by workflow_id (start dedup, spec §7.1). On first creation,
    /// also appends `WorkflowStarted` and marks the run runnable. Returns the
    /// effective run_id (the new one, or the pre-existing one).
    async fn create_execution(
        &self,
        candidate_run_id: &str,
        workflow_id: &str,
        workflow_type: &str,
        input: &[u8],
    ) -> anyhow::Result<(CreateOutcome, String)>;

    async fn read_history(&self, run_id: &str) -> anyhow::Result<Vec<StoredEvent>>;

    /// Resolve a run_id to its metadata (workflow id/type/status).
    async fn load_run(&self, run_id: &str) -> anyhow::Result<Option<RunMeta>>;

    /// Atomically: append events, enqueue tasks, set status/result, clear
    /// runnable for this run (spec §5.1 — the exactly-once boundary).
    async fn commit_turn(&self, run_id: &str, commit: &TurnCommit) -> anyhow::Result<()>;

    /// (run_id, status, result) for a workflow_id, if it exists.
    async fn find_execution(
        &self,
        workflow_id: &str,
    ) -> anyhow::Result<Option<(String, ExecStatus, Option<Vec<u8>>)>>;
}

/// Work queue: activity tasks, timers (later), and the runnable set (spec §15).
#[async_trait::async_trait]
pub trait TaskQueue: Send + Sync {
    /// A run with unprocessed events, if any.
    async fn next_runnable(&self) -> anyhow::Result<Option<String>>;

    /// Lease one due pending activity task, marking it running and bumping its
    /// attempt count. Returns None if nothing is due.
    async fn lease_activity(&self) -> anyhow::Result<Option<ActivityLease>>;

    /// Terminal outcome: mark the task done, append the completion event, mark
    /// the run runnable — all in one transaction (spec §5.2).
    async fn complete_activity(
        &self,
        lease: &ActivityLease,
        result: CommandResult,
    ) -> anyhow::Result<()>;

    /// Non-terminal retry: return the task to pending with a new backoff time.
    async fn reschedule_activity(
        &self,
        lease: &ActivityLease,
        next_run_at: i64,
    ) -> anyhow::Result<()>;

    /// Atomically fire one timer whose `fire_at <= now`: append `TimerFired`,
    /// delete the timer row, and mark the run runnable (spec §5.3). Returns false
    /// if no timer is due. Single combined method (no lease/retry — timers carry no
    /// side effect) so two service iterations cannot double-fire the same timer.
    async fn fire_due_timer(&self) -> anyhow::Result<bool>;
}
