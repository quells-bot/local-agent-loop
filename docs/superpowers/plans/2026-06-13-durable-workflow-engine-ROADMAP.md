# Durable Workflow Engine — Implementation Roadmap

> **For agentic workers:** This is the **index**, not an executable plan. Each
> chunk below has (or will have) its own plan file under
> `docs/superpowers/plans/`. Execute chunks in order; each produces working,
> tested software on its own. Detailed plans for Pass 2–5 are authored
> **just-in-time**, right before that pass starts, so their exact
> types/signatures are written against the real code from earlier passes.

**Source spec:** `docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md`
(section references below, e.g. "spec §6", point back to it).

**Goal:** An in-process, SQLite-backed, Temporal-style durable workflow engine in
Rust — the backend of a Tauri desktop app — built as a series of small,
independently-testable chunks.

---

## Why this decomposition

The spec is long. A single monolithic plan would be hard to keep correct. Instead
the work follows the spec's five **passes**, and each pass subdivides into
**chunks** that each produce working, tested software. Roughly 11 chunks total,
each small-to-medium. Signals land in Pass 3 (the earliest non-artificial slot,
spec §13).

---

## Shared technical decisions

These apply to every chunk; chunk plans assume them.

- **Edition / workspace:** Rust 2021, workspace `resolver = "2"`. Crates under
  `crates/`, integration under `examples/`. All crates `publish = false`.
- **Crate DAG (spec §10):** `activity ← workflow ← engine ← persist`, plus
  `persist → workflow`. `engine` never depends on `persist`.
- **Async:** `tokio` (multi-thread runtime). The `History` / `TaskQueue` traits
  are `#[async_trait]` so a future cloud backend can be genuinely async; the
  SQLite impl does synchronous `rusqlite` work inside those async fns (acceptable
  at desktop scale, parallelism = 1 on decisions).
- **Persistence:** `rusqlite` with the `bundled` feature; a single
  `Arc<Mutex<Connection>>`. (Chosen in chunk 1c.)
- **Serialization:** `serde` + `serde_json`. All payloads (user input/output,
  command/event payloads) are `Vec<u8>` = `serde_json::to_vec`. JSON keeps
  history human-inspectable during desktop iteration; `bincode` is a later
  optimization (spec §14 spirit).
- **Errors:** `thiserror`. `run_id`: `uuid` v4 string (engine-side, not subject
  to workflow determinism).
- **Naming (spec §10):** never stutter the crate name into a type
  (`workflow::Context`, not `WfContext`; marker traits `workflow::Definition` /
  `activity::Definition`).

### Workspace dependency versions (`[workspace.dependencies]`)

```toml
serde       = { version = "1", features = ["derive"] }
serde_json  = "1"
thiserror   = "1"
async-trait = "0.1"
tokio       = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time"] }
rusqlite    = { version = "0.31", features = ["bundled"] }
uuid        = { version = "1", features = ["v4"] }
futures     = "0.3"
tempfile    = "3"     # dev-dependency, for SQLite tests
```

---

## Canonical types (single source of truth)

Defined in chunk **1a**; later chunks reference these exact signatures. If a
chunk needs to change one, it updates this list too.

**crate `activity`** (leaf):

```rust
pub struct Execution { pub workflow_id: String, pub run_id: String }
//   derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize

pub struct Info {                       // Go: activity.ActivityInfo
    pub execution: Execution,
    pub activity_id: String,
    pub activity_type: String,
    pub attempt: u32,
}                                       // derive: Clone, Debug, PartialEq, Eq

pub struct Error { pub message: String, pub non_retryable: bool }
//   derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize; impl std::error::Error
//   ctors: Error::retryable(msg), Error::fatal(msg)

pub struct Context { /* info */ }       // new(Info); info() -> &Info;
                                        // idempotency_key() -> "{run_id}:{activity_id}"

#[async_trait] pub trait Definition: 'static {
    type Input:  Serialize + DeserializeOwned + Send;
    type Output: Serialize + DeserializeOwned + Send;
    const TYPE: &'static str;
    async fn run(ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}
```

**crate `workflow`** (`-> activity`):

```rust
pub use activity::Execution;            // re-export

pub struct Info {                       // Go: workflow.Info
    pub execution: Execution,
    pub parent: Option<Execution>,
    pub workflow_type: String,
}                                       // derive: Clone, Debug, PartialEq, Eq

pub struct RetryPolicy { pub max_attempts: u32, pub initial_ms: u64, pub multiplier: u32 }
//   derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize
//   RetryPolicy::exponential(max) = { max, 100, 2 };  RetryPolicy::none() = { 1, 0, 1 }
//   backoff_ms(attempt: u32) -> u64   (attempt is 1-based; attempt 1 -> 0 delay)

pub enum Command {                      // emitted by futures, drained by driver
    ScheduleActivity { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    // Pass 2 adds StartTimer; Pass 4 adds StartChild; Pass 3 adds nothing (signals are inbound)
}                                       // derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize

pub enum Event {                        // history record, applied into ctx + persisted
    WorkflowStarted   { input: Vec<u8> },
    ActivityScheduled { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    ActivityCompleted { seq: u64, output: Vec<u8> },
    ActivityFailed    { seq: u64, error: activity::Error },
    // Pass 2: TimerFired; Pass 3: SignalReceived; Pass 4: ChildCompleted;
    // reserved: WorkflowCancelRequested
}                                       // derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize
                                        // method: kind(&self) -> &'static str

pub enum CommandResult { ActivityCompleted(Vec<u8>), ActivityFailed(activity::Error) }
//   From<CommandResult> for Result<Vec<u8>, activity::Error>

pub struct Error { pub message: String }      // workflow failure
//   derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize; impl std::error::Error

pub struct Context { /* Rc<ContextInner>; minimal in 1a, replay state added in 1b */ }
//   info() -> &Info  (1a);  activity::<A>(input) -> ActivityFuture, now(), random() (1b)

// `?Send`: workflow futures hold Rc/RefCell (single-threaded loop), so they are
// NOT Send. Associated types are `'static` (not Send — values never cross threads).
#[async_trait(?Send)] pub trait Definition: 'static {
    type Input:  Serialize + DeserializeOwned + 'static;
    type Output: Serialize + DeserializeOwned + 'static;
    const TYPE: &'static str;
    async fn run(ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}
```

**crate `engine`** (defines the migration-boundary traits, spec §15):

```rust
pub struct StoredEvent { pub event_id: i64, pub event: workflow::Event }
pub struct TurnCommit { pub events: Vec<Event>, pub new_tasks: Vec<NewActivityTask>,
                        pub status: ExecStatus, pub result: Option<Vec<u8>> }

#[async_trait] pub trait History {
    async fn create_execution(&self, candidate_run_id: &str, workflow_id: &str,
                              workflow_type: &str, input: &[u8]) -> Result<(CreateOutcome, String)>;
    async fn read_history(&self, run_id: &str) -> Result<Vec<StoredEvent>>;
    async fn load_run(&self, run_id: &str) -> Result<Option<RunMeta>>;
    async fn commit_turn(&self, run_id: &str, commit: &TurnCommit) -> Result<()>;
    async fn find_execution(&self, workflow_id: &str)
        -> Result<Option<(String, ExecStatus, Option<Vec<u8>>)>>;
}
#[async_trait] pub trait TaskQueue {
    async fn next_runnable(&self) -> Result<Option<String>>;
    async fn lease_activity(&self) -> Result<Option<ActivityLease>>;
    async fn complete_activity(&self, lease: &ActivityLease, result: CommandResult) -> Result<()>;
    async fn reschedule_activity(&self, lease: &ActivityLease, next_run_at: i64) -> Result<()>;
}
pub enum SignalError { WorkflowNotFound, NotRunning }   // Pass 3
```

(`anyhow::Result` elided as `Result` above. Supporting types — `ExecStatus`,
`NewActivityTask`, `ActivityLease`, `CreateOutcome`, `RunMeta` — are defined in
chunk **1c**.)

---

## Chunk list & status

| Chunk | Title | Spec refs | Plan file | Status |
| --- | --- | --- | --- | --- |
| 1a | Workspace + protocol types | §3, §9, §10 | `2026-06-13-pass-1a-workspace-and-protocol-types.md` | planned |
| 1b | Replay core (pure) | §3, §4, §12 | `2026-06-13-pass-1b-replay-core.md` | planned |
| 1c | Backend traits + SQLite persist | §5, §11, §15 | `2026-06-13-pass-1c-persist-and-traits.md` | planned |
| 1d | Driver + workers + start + observer | §5, §6.1(start), §7, §8 | `2026-06-13-pass-1d-driver-and-workers.md` | planned |
| 2a | Timers (`sleep`/`timer` + service) | §4, §5.3 | _(JIT)_ | not yet authored |
| 2b | Combinators + spawn scheduler | §4.2, §4.4 | _(JIT)_ | not yet authored |
| 3a | Inbound-event pipeline + signal channel | §6.1–6.3, §12 | _(JIT)_ | not yet authored |
| 3b | `signal_workflow` + signal-or-timeout e2e | §6.1, §7.2 | _(JIT)_ | not yet authored |
| 4a | Child workflows | §5.4, §9(info.parent) | _(JIT)_ | not yet authored |
| 5a | Cache vs cold-replay equivalence + hardening | §12, §14 | _(JIT)_ | not yet authored |
| 5b | `ctx.patched` + trait cleanup + macros lint | §4.2, §14, §15 | _(JIT)_ | not yet authored |

---

## Pass acceptance gates (from spec §13)

- **Pass 1:** an activity-only workflow runs to completion; killing the process
  mid-run cold-replays to the same logical position; a forced cache-evict
  reproduces an identical command stream; `info()` reports the right ids.
- **Pass 2:** concurrent branches and a `select_biased!` race replay
  deterministically across cold recovery; losing branches behave per §4.3.
- **Pass 3:** a workflow blocked on `recv()` resumes when signaled;
  signal-or-timeout resolves each way deterministically; signals before/after a
  crash replay identically; signaling a completed run returns `NotRunning`.
- **Pass 4:** a parent awaiting a child completes when the child does, across
  cold recovery of either.
- **Pass 5:** the cache/cold-replay equivalence test passes as a CI guard; the
  two traits compile as the only seam `persist` implements.
