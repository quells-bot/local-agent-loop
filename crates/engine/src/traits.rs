// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec: docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use crate::{
    ActivityLease, CreateOutcome, ExecStatus, ExecutionSummary, HistoryRecord, RunMeta,
    SignalOutcome, StoredEvent, TurnCommit,
};
use workflow::CommandResult;

/// # Migration seam (spec §15)
///
/// `History` and `TaskQueue` are the *complete* boundary between the backend-agnostic
/// engine and a concrete store. `persist::Sqlite` implements exactly these two and
/// nothing else; a cloud backend is "implement these two traits" rather than a
/// rewrite. `crates/engine/tests/migration_seam.rs` asserts this at compile time.
///
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

    /// Append a `SignalReceived` event to the run identified by `workflow_id` and
    /// mark it runnable, in ONE transaction (spec §6.1 — the durable-before-return
    /// boundary). The status check and the append are atomic: a signal is appended
    /// only if the run is still `running`. Returns a typed outcome rather than
    /// erroring on not-found / not-running, so the host can map it to `SignalError`.
    async fn append_signal(
        &self,
        workflow_id: &str,
        name: &str,
        payload: &[u8],
    ) -> anyhow::Result<SignalOutcome>;

    /// (run_id, status, result) for a workflow_id, if it exists.
    async fn find_execution(
        &self,
        workflow_id: &str,
    ) -> anyhow::Result<Option<(String, ExecStatus, Option<Vec<u8>>)>>;

    /// Read model for the history viewer (history-viewer design §4.2). NOT part of
    /// the exactly-once boundary: the engine never calls these. They ride on
    /// `History` because they are history-store reads, keeping the migration seam
    /// at two traits. Root executions only (`parent_run_id IS NULL`), newest first.
    async fn list_executions(&self) -> anyhow::Result<Vec<ExecutionSummary>>;

    /// Read model for the history viewer: all events for a run in `event_id` order,
    /// carrying timestamps. For `ChildScheduled`/`ChildCompleted`, resolves the
    /// child's run_id from `executions`. NOT used by replay.
    async fn read_events(&self, run_id: &str) -> anyhow::Result<Vec<HistoryRecord>>;
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

    /// Crash recovery: return in-flight leases whose TTL has elapsed (`status =
    /// 'running'` with `lease_expires_at <= now`) to `pending`, so a fresh worker
    /// can re-lease them. Returns the number reclaimed (spec §5.2 — at-least-once).
    async fn reclaim_expired_activities(&self) -> anyhow::Result<u64>;
}
