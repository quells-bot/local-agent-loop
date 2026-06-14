# Pass 1d — Driver + Workers + Start + Observer — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tie the pieces together into a runnable `engine::Engine`: a decision
loop that cold-replays runnable workflows and persists new commands, a parallel
activity worker with retries, `start_workflow` with dedup, and a completion
observer hook (spec §5, §7.1, §7.3, §8). Ends with the Pass 1 acceptance test.

**Architecture:** No sticky cache yet (deferred to Pass 5). On each wake the
driver **cold-replays from full history** via `workflow::cold_replay`, diffs the
emitted commands against already-recorded `ActivityScheduled` seqs, and commits
only the new ones. Because `cold_replay` runs synchronously (no `.await`), the
driver future stays `Send` and runs as an ordinary tokio task — the `!Send`
workflow futures live only inside that synchronous call. The driver and worker
are exposed as single-step methods (`process_one_runnable`, `process_one_activity`)
so tests can pump deterministically; `start()` just spawns loops over them.

**Tech Stack:** tokio, uuid, serde_json, anyhow.

**Depends on:** chunks 1a, 1b, 1c.

---

## File Structure

```
/crates/engine/Cargo.toml          # MODIFY: add tokio, uuid, serde, serde_json; dev: persist
/crates/engine/src/lib.rs          # MODIFY: wire `engine` module
/crates/engine/src/engine.rs       # NEW: Engine, Handle, StartOptions, RunCompleted, loops
/crates/engine/tests/end_to_end.rs # NEW: integration tests (Pass 1 acceptance gate)
```

---

### Task 1: Engine deps + skeleton (Engine, StartOptions, RunCompleted, registries)

**Files:**
- Modify: `crates/engine/Cargo.toml`, `crates/engine/src/lib.rs`
- Create: `crates/engine/src/engine.rs`

- [ ] **Step 1: Add deps**

Set `crates/engine/Cargo.toml`:

```toml
[package]
name = "engine"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
workflow    = { path = "../workflow" }
activity    = { path = "../activity" }
async-trait = { workspace = true }
anyhow      = "1"
tokio       = { workspace = true }
uuid        = { workspace = true }
serde       = { workspace = true }
serde_json  = { workspace = true }

[dev-dependencies]
persist = { path = "../persist" }
```

- [ ] **Step 2: Create `engine.rs` skeleton**

```rust
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;

use crate::{ExecStatus, History, NewActivityTask, TaskQueue, TurnCommit};

/// Options for starting a workflow (spec §7.1). `id` is the dedup key.
pub struct StartOptions {
    pub id: String,
}
impl Default for StartOptions {
    fn default() -> Self {
        Self { id: String::new() }
    }
}

/// Emitted to the completion observer after a turn drives a run terminal (spec §7.3).
#[derive(Debug, Clone)]
pub struct RunCompleted {
    pub run_id: String,
    pub workflow_id: String,
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>,
}

type ReplayFn = Arc<
    dyn Fn(workflow::Info, &[workflow::Event]) -> Result<workflow::ReplayOutcome, workflow::Nondeterminism>
        + Send
        + Sync,
>;
type RunnerFn = Arc<
    dyn Fn(activity::Context, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, activity::Error>> + Send>>
        + Send
        + Sync,
>;
type Observer = Arc<dyn Fn(RunCompleted) + Send + Sync>;

pub struct Engine {
    history: Arc<dyn History>,
    queue: Arc<dyn TaskQueue>,
    workflows: HashMap<String, ReplayFn>,
    activities: HashMap<String, RunnerFn>,
    observer: Option<Observer>,
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

impl Engine {
    pub fn new(history: Arc<dyn History>, queue: Arc<dyn TaskQueue>) -> Self {
        Self { history, queue, workflows: HashMap::new(), activities: HashMap::new(), observer: None }
    }

    pub fn register_workflow<W: workflow::Definition>(&mut self) {
        self.workflows.insert(
            W::TYPE.to_string(),
            Arc::new(|info, events| workflow::cold_replay::<W>(info, events)),
        );
    }

    pub fn register_activity<A: activity::Definition>(&mut self) {
        self.activities.insert(
            A::TYPE.to_string(),
            Arc::new(|ctx, bytes| {
                Box::pin(async move {
                    let input: A::Input = serde_json::from_slice(&bytes)
                        .map_err(|e| activity::Error::fatal(format!("activity input deserialize: {e}")))?;
                    let out = A::run(ctx, input).await?;
                    serde_json::to_vec(&out)
                        .map_err(|e| activity::Error::fatal(format!("activity output serialize: {e}")))
                })
            }),
        );
    }

    pub fn on_run_completed<F: Fn(RunCompleted) + Send + Sync + 'static>(&mut self, f: F) {
        self.observer = Some(Arc::new(f));
    }
}
```

- [ ] **Step 3: Wire module + build**

Append to `crates/engine/src/lib.rs`:

```rust
mod engine;
pub use engine::{Engine, RunCompleted, StartOptions};
```

Run: `cargo build -p engine`
Expected: compiles (methods added in later tasks).

- [ ] **Step 4: Commit**

```bash
git add crates/engine Cargo.toml
git commit -m "feat(engine): Engine skeleton + registries"
```

---

### Task 2: `start_workflow` + `Handle`

**Files:**
- Modify: `crates/engine/src/engine.rs`, `crates/engine/src/lib.rs`

- [ ] **Step 1: Add `Handle` and `start_workflow`** (append to `engine.rs`)

```rust
/// Durable handle to a started run (spec §7.1).
pub struct Handle {
    run_id: String,
    workflow_id: String,
    history: Arc<dyn History>,
}

impl Handle {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Await durable completion, deserializing the workflow output (spec §9).
    pub async fn result<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        loop {
            match self.history.find_execution(&self.workflow_id).await? {
                Some((_, ExecStatus::Completed, Some(bytes))) => {
                    return Ok(serde_json::from_slice(&bytes)?);
                }
                Some((_, ExecStatus::Completed, None)) => anyhow::bail!("completed without result"),
                Some((_, ExecStatus::Failed, _)) => anyhow::bail!("workflow failed"),
                Some((_, ExecStatus::Running, _)) => tokio::time::sleep(Duration::from_millis(5)).await,
                None => anyhow::bail!("no execution for workflow id {}", self.workflow_id),
            }
        }
    }
}

impl Engine {
    /// Start a workflow, deduping by `opts.id` (spec §7.1).
    pub async fn start_workflow<W: workflow::Definition>(
        &self,
        input: W::Input,
        opts: StartOptions,
    ) -> anyhow::Result<Handle> {
        let input_bytes = serde_json::to_vec(&input)?;
        let candidate = uuid::Uuid::new_v4().to_string();
        let (_outcome, run_id) = self
            .history
            .create_execution(&candidate, &opts.id, W::TYPE, &input_bytes)
            .await?;
        Ok(Handle { run_id, workflow_id: opts.id, history: self.history.clone() })
    }
}
```

- [ ] **Step 2: Export `Handle`**

Update the `pub use` in `crates/engine/src/lib.rs`:

```rust
pub use engine::{Engine, Handle, RunCompleted, StartOptions};
```

- [ ] **Step 3: Build**

Run: `cargo build -p engine`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): start_workflow with dedup + Handle"
```

---

### Task 3: `process_one_runnable` (the decision turn)

**Files:**
- Modify: `crates/engine/src/engine.rs`

- [ ] **Step 1: Implement** (append inside an `impl Engine { ... }` block in `engine.rs`)

```rust
use std::collections::HashSet;

impl Engine {
    /// Process one runnable workflow: cold-replay, persist newly-emitted commands,
    /// update status, fire the observer on terminal (spec §5.1). Returns false if
    /// nothing was runnable.
    pub async fn process_one_runnable(&self) -> anyhow::Result<bool> {
        let Some(run_id) = self.queue.next_runnable().await? else {
            return Ok(false);
        };
        let meta = self
            .history
            .load_run(&run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("runnable run {run_id} has no execution row"))?;

        let stored = self.history.read_history(&run_id).await?;
        let events: Vec<workflow::Event> = stored.into_iter().map(|s| s.event).collect();
        let recorded: HashSet<u64> = events
            .iter()
            .filter_map(|e| match e {
                workflow::Event::ActivityScheduled { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect();

        let info = workflow::Info {
            execution: workflow::Execution {
                workflow_id: meta.workflow_id.clone(),
                run_id: run_id.clone(),
            },
            parent: None,
            workflow_type: meta.workflow_type.clone(),
        };
        let replay = self
            .workflows
            .get(&meta.workflow_type)
            .ok_or_else(|| anyhow::anyhow!("unregistered workflow {}", meta.workflow_type))?
            .clone();

        let outcome = replay(info, &events)
            .map_err(|e| anyhow::anyhow!("nondeterminism in {}: {e}", meta.workflow_type))?;

        // Persist only commands not already recorded in history.
        let mut new_events = Vec::new();
        let mut new_tasks = Vec::new();
        for cmd in &outcome.commands {
            let workflow::Command::ScheduleActivity { seq, activity_type, input, retry } = cmd;
            if recorded.contains(seq) {
                continue;
            }
            new_events.push(workflow::Event::ActivityScheduled {
                seq: *seq,
                activity_type: activity_type.clone(),
                input: input.clone(),
                retry: retry.clone(),
            });
            new_tasks.push(NewActivityTask {
                seq: *seq as i64,
                activity_type: activity_type.clone(),
                input: input.clone(),
                next_run_at: 0,
            });
        }

        let (status, result) = match &outcome.completion {
            Some(Ok(bytes)) => (ExecStatus::Completed, Some(bytes.clone())),
            Some(Err(err)) => (ExecStatus::Failed, Some(serde_json::to_vec(err)?)),
            None => (ExecStatus::Running, None),
        };

        let commit = TurnCommit { events: new_events, new_tasks, status, result: result.clone() };
        self.history.commit_turn(&run_id, &commit).await?;

        if status != ExecStatus::Running {
            if let Some(obs) = &self.observer {
                obs(RunCompleted {
                    run_id: run_id.clone(),
                    workflow_id: meta.workflow_id,
                    status,
                    result,
                });
            }
        }
        Ok(true)
    }
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p engine`
Expected: compiles. If the `let Command::ScheduleActivity { .. } = cmd` line warns
(`irrefutable_let_patterns` is fine for now while there is one variant), leave it.

- [ ] **Step 3: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): decision-turn driver (process_one_runnable)"
```

---

### Task 4: `process_one_activity` (worker with retries)

**Files:**
- Modify: `crates/engine/src/engine.rs`

- [ ] **Step 1: Implement** (append inside an `impl Engine { ... }` block)

```rust
impl Engine {
    /// Lease one due activity task, run it, and record the outcome — completing on
    /// success/terminal failure, rescheduling with backoff otherwise (spec §5.2, §8).
    /// Returns false if nothing was due.
    pub async fn process_one_activity(&self) -> anyhow::Result<bool> {
        let Some(lease) = self.queue.lease_activity().await? else {
            return Ok(false);
        };

        let runner = match self.activities.get(&lease.activity_type) {
            Some(r) => r.clone(),
            None => {
                self.queue
                    .complete_activity(
                        &lease,
                        workflow::CommandResult::ActivityFailed(activity::Error::fatal(format!(
                            "unregistered activity {}",
                            lease.activity_type
                        ))),
                    )
                    .await?;
                return Ok(true);
            }
        };

        let ctx = activity::Context::new(activity::Info {
            execution: activity::Execution {
                workflow_id: lease.workflow_id.clone(),
                run_id: lease.run_id.clone(),
            },
            activity_id: lease.seq.to_string(),
            activity_type: lease.activity_type.clone(),
            attempt: lease.attempt,
        });

        match runner(ctx, lease.input.clone()).await {
            Ok(output) => {
                self.queue
                    .complete_activity(&lease, workflow::CommandResult::ActivityCompleted(output))
                    .await?;
            }
            Err(e) => {
                let exhausted = e.non_retryable || lease.attempt >= lease.retry.max_attempts;
                if exhausted {
                    self.queue
                        .complete_activity(&lease, workflow::CommandResult::ActivityFailed(e))
                        .await?;
                } else {
                    let delay = lease.retry.backoff_ms(lease.attempt + 1) as i64;
                    self.queue.reschedule_activity(&lease, now_ms() + delay).await?;
                }
            }
        }
        Ok(true)
    }
}
```

- [ ] **Step 2: Build**

Run: `cargo build -p engine`
Expected: compiles.

- [ ] **Step 3: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): activity worker with retry/backoff (process_one_activity)"
```

---

### Task 5: `start()` — spawn the loops

**Files:**
- Modify: `crates/engine/src/engine.rs`

- [ ] **Step 1: Implement** (append inside an `impl Engine { ... }` block)

```rust
impl Engine {
    /// Spawn the driver and activity-worker loops as background tokio tasks and
    /// return a shared handle. Use the `process_one_*` methods directly in tests
    /// for deterministic stepping.
    pub fn start(self) -> Arc<Engine> {
        let engine = Arc::new(self);

        let driver = engine.clone();
        tokio::spawn(async move {
            loop {
                match driver.process_one_runnable().await {
                    Ok(true) => {}
                    Ok(false) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(err) => {
                        eprintln!("driver error: {err:#}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        let worker = engine.clone();
        tokio::spawn(async move {
            loop {
                match worker.process_one_activity().await {
                    Ok(true) => {}
                    Ok(false) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(err) => {
                        eprintln!("worker error: {err:#}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        engine
    }
}
```

- [ ] **Step 2: Build + commit**

Run: `cargo build -p engine`

```bash
git add crates/engine
git commit -m "feat(engine): start() spawns driver + worker loops"
```

---

### Task 6: Pass 1 acceptance — end-to-end + cold recovery

**Files:**
- Create: `crates/engine/tests/end_to_end.rs`

- [ ] **Step 1: Write the integration tests**

```rust
use std::sync::Arc;

use engine::{Engine, ExecStatus, History, StartOptions};
use persist::Sqlite;

// Activity: Add(a, b) -> a + b.
struct Add;
#[async_trait::async_trait]
impl activity::Definition for Add {
    type Input = (i64, i64);
    type Output = i64;
    const TYPE: &'static str = "Add";
    async fn run(_c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
        Ok(i.0 + i.1)
    }
}

// Workflow: Sum() -> Add(Add(1, 2), 10) == 13, via two sequential activities.
struct Sum;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Sum {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Sum";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let a = ctx.activity::<Add>((1, 2)).await?;
        let b = ctx.activity::<Add>((a, 10)).await?;
        Ok(b)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Sum>();
    e.register_activity::<Add>();
    e
}

/// Pump driver+worker turns until quiescent (deterministic; no background loops).
async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        if !drove && !worked {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn activity_workflow_runs_to_completion() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Sum>((), StartOptions { id: "sum-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 13);
}

#[tokio::test]
async fn cold_recovery_completes_identically() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn + one activity, then drop the engine (crash).
    {
        let engine = build(&db);
        engine
            .start_workflow::<Sum>((), StartOptions { id: "sum-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // schedules Add #0
        assert!(engine.process_one_activity().await.unwrap()); // completes Add #0
        // engine dropped here; only the shared `db` connection survives
    }

    // Phase 2: a fresh engine with NO in-memory state cold-replays and finishes.
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("sum-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 13);
}

#[tokio::test]
async fn completion_observer_fires_on_terminal() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let db = Sqlite::open_in_memory().unwrap();
    let mut engine = build(&db);
    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    engine.on_run_completed(move |ev| {
        if matches!(ev.status, ExecStatus::Completed) {
            f.store(true, Ordering::SeqCst);
        }
    });
    engine
        .start_workflow::<Sum>((), StartOptions { id: "sum-3".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    assert!(fired.load(Ordering::SeqCst), "observer should fire on completion");
}

#[tokio::test]
async fn start_is_idempotent_by_id() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let h1 = engine.start_workflow::<Sum>((), StartOptions { id: "dup".into() }).await.unwrap();
    let h2 = engine.start_workflow::<Sum>((), StartOptions { id: "dup".into() }).await.unwrap();
    assert_eq!(h1.run_id(), h2.run_id(), "same id returns the existing run");
}
```

The test crate needs `async-trait` and `anyhow` available. Add to
`crates/engine/Cargo.toml` `[dev-dependencies]`:

```toml
async-trait = { workspace = true }
anyhow      = "1"
```

(`async-trait`/`anyhow` are already normal deps, but tests are a separate crate;
listing them in dev-deps too is harmless and explicit.)

- [ ] **Step 2: Run the acceptance tests**

Run: `cargo test -p engine --test end_to_end`
Expected: all four tests PASS:
- `activity_workflow_runs_to_completion`
- `cold_recovery_completes_identically`
- `completion_observer_fires_on_terminal`
- `start_is_idempotent_by_id`

- [ ] **Step 3: Commit**

```bash
git add crates/engine
git commit -m "test(engine): pass 1 acceptance — e2e, cold recovery, observer, dedup"
```

---

### Task 7: Whole-workspace green + clippy + roadmap update

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS.
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean.
- [ ] **Step 3:** Mark Pass 1 chunks `done` in the status table of
  `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`.
- [ ] **Step 4:** Commit:

```bash
git add -A
git commit -m "chore: pass 1 complete — clippy clean, roadmap updated"
```

---

## Notes for Pass 2+

- The driver currently cold-replays on every wake. Pass 5 adds the sticky cache
  (a held `WorkflowState` per run on a `LocalSet`/dedicated thread) plus the
  cache-vs-cold-replay equivalence guard.
- `ctx.now()` / `ctx.random()` still deferred — add recorded marker events before
  any workflow needs them (likely alongside Pass 2 timers).
- Pass 2 timers add `Command::StartTimer` / `Event::TimerFired`; the driver's
  command-diff loop and `commit_turn` gain a timers branch mirroring activities.

## Self-Review (completed during authoring)

- **Spec coverage:** §5.1 decision turn (`process_one_runnable` → `commit_turn`),
  §5.2 worker (`process_one_activity`), §7.1 start+dedup (`start_workflow`),
  §7.3 observer (`on_run_completed`/`RunCompleted`), §8 idempotency/retries
  (worker uses `lease.retry` + `idempotency_key` available via `activity::Context`),
  §13 Pass 1 gate (e2e + cold recovery + dedup tests). Sticky-cache equivalence is
  explicitly Pass 5.
- **Placeholders:** none — full code and exact commands throughout.
- **Type consistency:** uses `History`/`TaskQueue`/`RunMeta`/`TurnCommit`/
  `NewActivityTask`/`ActivityLease`(`.retry`)/`ExecStatus` from 1c, `cold_replay`/
  `ReplayOutcome.completion`/`Command`/`Event`/`Info` from 1b/1a, all matching the
  ROADMAP canonical types.
```
