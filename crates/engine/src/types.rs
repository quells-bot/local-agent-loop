use workflow::{Event, RetryPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecStatus {
    Running,
    Completed,
    Failed,
}

impl ExecStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecStatus::Running => "running",
            ExecStatus::Completed => "completed",
            ExecStatus::Failed => "failed",
        }
    }
    // Inherent Option-returning `from_str` is the intended API across the
    // engine/persist crates (`from_str(&status).unwrap_or(..)`); a Result-based
    // `FromStr` impl would be the wrong shape here.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "running" => Some(ExecStatus::Running),
            "completed" => Some(ExecStatus::Completed),
            "failed" => Some(ExecStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub event_id: i64,
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewActivityTask {
    pub seq: i64,
    pub activity_type: String,
    pub input: Vec<u8>,
    pub next_run_at: i64, // epoch ms; <= now means runnable immediately
}

/// A timer to enqueue this turn. `fire_at` is the absolute epoch-ms deadline the
/// driver computes from the StartTimer command's duration (spec §5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTimer {
    pub seq: i64,
    pub fire_at: i64,
}

/// A child workflow to create this turn (spec §5.4). Created atomically inside the
/// parent's decision-turn transaction so a crash can never orphan it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewChild {
    pub seq: i64, // the parent's StartChild command seq -> child's parent_seq
    pub child_run_id: String,
    pub child_workflow_id: String,
    pub workflow_type: String,
    pub input: Vec<u8>,
}

/// A child→parent terminal notification (spec §5.4): a `ChildCompleted` event the
/// child's terminal turn appends to its PARENT's history, marking the parent runnable
/// — in the same transaction, so completion+notification is atomic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentNotify {
    pub parent_run_id: String,
    pub event: Event, // a ChildCompleted event
}

/// Everything a single decision turn commits atomically (spec §5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnCommit {
    pub events: Vec<Event>, // new history events emitted this turn
    pub new_tasks: Vec<NewActivityTask>,
    pub new_timers: Vec<NewTimer>, // timers to enqueue this turn
    pub new_children: Vec<NewChild>,
    pub parent_notify: Option<ParentNotify>,
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>, // Some iff status != Running
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityLease {
    pub run_id: String,
    pub workflow_id: String,
    pub seq: i64,
    pub activity_type: String,
    pub input: Vec<u8>,
    pub attempt: u32,       // 1-based; this is the current attempt number
    pub retry: RetryPolicy, // read from the ActivityScheduled event by the queue
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    AlreadyExists,
}

/// Result of an `append_signal` attempt (spec §6.1). The host maps this to the
/// public `SignalError`; the trait stays free of the host-facing error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    Delivered,
    WorkflowNotFound,
    NotRunning,
}

/// Metadata for one run, resolved by `run_id` (driver needs this to build
/// `workflow::Info` and pick the replay closure).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMeta {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub status: ExecStatus,
    pub parent_run_id: Option<String>,
    pub parent_seq: Option<i64>,
}
