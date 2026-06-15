# Pass 3b — `signal_workflow` host delivery + signal-or-timeout e2e — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the host side of signals — `engine.signal_workflow(workflow_id, name,
&payload)` and `handle.signal(name, &payload)` returning a typed `SignalError`,
backed by a new `History::append_signal` trait method whose `persist` impl appends a
`SignalReceived` row (seq NULL) and marks the run runnable in **one transaction** —
then prove Pass 3 with end-to-end acceptance tests: a workflow blocked on `recv()`
resumes when signaled, the signal-or-timeout `select_biased!` pattern resolves each
way deterministically, a signal delivered before a crash replays to completion, and
signaling a completed (or unknown) run returns the right error (spec §6.1, §7.2, §13).

**Architecture:** `signal_workflow` is a host→engine entrypoint (spec §7), not a
workflow command. It performs a **single transaction** through a new `History`
method `append_signal(workflow_id, name, payload)`: look up the run by
`workflow_id`, check its status, and — only if `running` — append `SignalReceived`
(no `seq`) and `INSERT OR REPLACE` the `runnable` row. The method returns a typed
`SignalOutcome { Delivered, WorkflowNotFound, NotRunning }`; the engine maps it to
the public `SignalError`. Doing the status-check + append atomically makes the call
**durable-before-return** (the Tauri command can give the frontend synchronous
"recorded" confirmation, spec §6.1) and avoids buffering a signal for a run that will
never consume it (Temporal-faithful, spec §6.1). The replay machinery that turns a
delivered `SignalReceived` back into a resolved `recv()` already exists from Pass 3a;
this chunk only adds durable delivery and the live-driver acceptance tests.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow, thiserror, futures.

**Depends on:** Pass 3a (`2026-06-14-pass-3a-inbound-events-and-signal-channel.md`),
merged. (3a gave us `Event::SignalReceived`, the per-name buffer, `signal_channel`/
`recv`, `cold_replay` signal support, and the `persist` encode path.)

---

## Canonical type additions (update the ROADMAP "Canonical types" list)

```rust
// crate `engine`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome { Delivered, WorkflowNotFound, NotRunning }   // NEW — trait-level result

#[async_trait] pub trait History {
    /* …existing… */
    /// Append a SignalReceived event to the run named by workflow_id and mark it
    /// runnable, in one transaction (spec §6.1). Status check + append are atomic.
    async fn append_signal(&self, workflow_id: &str, name: &str, payload: &[u8])
        -> anyhow::Result<SignalOutcome>;                            // NEW (Pass 3b)
}

#[derive(Debug, thiserror::Error)]
pub enum SignalError {                                               // host-facing (spec §6.1)
    WorkflowNotFound,   // no execution with that workflow_id
    NotRunning,         // execution is completed/failed (matches Temporal)
    Internal(anyhow::Error),  // unexpected backend failure (#[from]; escape hatch for IPC)
}

impl Engine {
    pub async fn signal_workflow(&self, workflow_id: &str, name: &str, payload: &[u8])
        -> Result<(), SignalError>;                                  // NEW (spec §7.2)
}
impl Handle {
    pub async fn signal(&self, name: &str, payload: &[u8])
        -> Result<(), SignalError>;                                  // NEW (spec §6.1)
}
```

> **Note on `SignalError::Internal`:** the spec's §6.1 enum lists the two *domain*
> outcomes the frontend reacts to (`WorkflowNotFound`, `NotRunning`). A production
> Rust API also needs to surface unexpected backend (SQLite/IO) errors, so we add an
> `Internal(anyhow::Error)` variant with `#[from]` — it is the engine's escape hatch,
> not a domain outcome, and does not affect Go-SDK portability (the frontend treats it
> as a generic failure). The trait stays clean by returning `SignalOutcome`, never
> `SignalError`.

---

## File Structure

```
/crates/engine/Cargo.toml          # MODIFY: add thiserror dependency
/crates/engine/src/types.rs        # MODIFY: add SignalOutcome enum
/crates/engine/src/traits.rs       # MODIFY: History::append_signal
/crates/engine/src/engine.rs       # MODIFY: SignalError, Engine::signal_workflow, Handle::signal
/crates/engine/src/lib.rs          # MODIFY: export SignalError, SignalOutcome
/crates/persist/src/history_impl.rs# MODIFY: append_signal impl + unit tests
/crates/engine/tests/signals.rs    # NEW: Pass-3 e2e acceptance tests
```

> **Build-order note:** Task 1 adds the required (no-default) `append_signal` method
> to the `History` trait, so **after Task 1 the `persist` crate does not compile**
> (it no longer satisfies the trait). The `engine` crate itself compiles. Task 1
> verifies with `cargo build -p engine`; Task 2 implements the method and makes the
> workspace green.

---

### Task 1: Engine surface — `SignalOutcome`, `SignalError`, `signal_workflow`

**Files:**
- Modify: `crates/engine/Cargo.toml`, `crates/engine/src/types.rs`, `traits.rs`, `engine.rs`, `lib.rs`

- [ ] **Step 0: Add `thiserror` to the engine crate**

`SignalError` derives `thiserror::Error`, but `engine` does not yet depend on
`thiserror` (only `workflow` does). In `crates/engine/Cargo.toml`, add it to
`[dependencies]` (the version is pinned in `[workspace.dependencies]`):

```toml
[dependencies]
workflow    = { path = "../workflow" }
activity    = { path = "../activity" }
async-trait = { workspace = true }
anyhow      = "1"
thiserror   = { workspace = true }
tokio       = { workspace = true }
uuid        = { workspace = true }
serde       = { workspace = true }
serde_json  = { workspace = true }
```

- [ ] **Step 1: Add `SignalOutcome` to `types.rs`**

In `crates/engine/src/types.rs`, add (after `CreateOutcome`):

```rust
/// Result of an `append_signal` attempt (spec §6.1). The host maps this to the
/// public `SignalError`; the trait stays free of the host-facing error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalOutcome {
    Delivered,
    WorkflowNotFound,
    NotRunning,
}
```

- [ ] **Step 2: Add `append_signal` to the `History` trait**

In `crates/engine/src/traits.rs`, extend the `use` line and add the method.

Update the imports at the top:

```rust
use crate::{ActivityLease, CreateOutcome, ExecStatus, RunMeta, SignalOutcome, StoredEvent, TurnCommit};
```

Add to the `History` trait (after `find_execution`):

```rust
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
```

- [ ] **Step 3: Add `SignalError`, `signal_workflow`, and `Handle::signal`**

In `crates/engine/src/engine.rs`, update the imports at the top to add
`SignalOutcome`:

```rust
use crate::{ExecStatus, History, NewActivityTask, NewTimer, SignalOutcome, TaskQueue, TurnCommit};
```

Add the `SignalError` type near `RunCompleted` (after the `RunCompleted` struct):

```rust
/// Typed result of a host signal attempt (spec §6.1), so the IPC layer can forward a
/// meaningful outcome to the frontend. `WorkflowNotFound` / `NotRunning` are the
/// domain outcomes; `Internal` carries an unexpected backend failure.
#[derive(Debug, thiserror::Error)]
pub enum SignalError {
    #[error("no workflow with that id")]
    WorkflowNotFound,
    #[error("workflow is not running")]
    NotRunning,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Shared mapping from the trait-level outcome to the host-facing result.
fn outcome_to_result(outcome: SignalOutcome) -> Result<(), SignalError> {
    match outcome {
        SignalOutcome::Delivered => Ok(()),
        SignalOutcome::WorkflowNotFound => Err(SignalError::WorkflowNotFound),
        SignalOutcome::NotRunning => Err(SignalError::NotRunning),
    }
}
```

Add `signal_workflow` to an `impl Engine` block — place it next to
`start_workflow` (inside the existing `impl Engine { ... }` that holds
`start_workflow`, after that method):

```rust
    /// Durably deliver a signal to a running workflow by `workflow_id` (spec §7.2).
    /// Returns `Ok(())` only once the `SignalReceived` event is committed, so the
    /// caller (a Tauri command) can confirm to the frontend synchronously.
    pub async fn signal_workflow(
        &self,
        workflow_id: &str,
        name: &str,
        payload: &[u8],
    ) -> Result<(), SignalError> {
        let outcome = self.history.append_signal(workflow_id, name, payload).await?;
        outcome_to_result(outcome)
    }
```

Add `signal` to the `impl Handle` block (after `result`):

```rust
    /// Durably deliver a signal to this run (spec §6.1). Same contract as
    /// `Engine::signal_workflow`, scoped to the handle's `workflow_id`.
    pub async fn signal(&self, name: &str, payload: &[u8]) -> Result<(), SignalError> {
        let outcome = self
            .history
            .append_signal(&self.workflow_id, name, payload)
            .await?;
        outcome_to_result(outcome)
    }
```

- [ ] **Step 4: Export `SignalError` and `SignalOutcome`**

In `crates/engine/src/lib.rs`, update the `engine` module re-export line to add
`SignalError`:

```rust
mod engine;
pub use engine::{Engine, Handle, RunCompleted, SignalError, StartOptions};
```

(`SignalOutcome` is already exported by `pub use types::*;` at the top of `lib.rs`.)

- [ ] **Step 5: Build the engine crate**

Run: `cargo build -p engine`
Expected: compiles. (`persist` is RED until Task 2 — do not run `cargo test` across
the workspace yet.)

- [ ] **Step 6: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): signal_workflow/Handle::signal + SignalError, History::append_signal trait"
```

---

### Task 2: Persist — `append_signal` (single-transaction durable delivery)

**Files:**
- Modify: `crates/persist/src/history_impl.rs`

- [ ] **Step 1: Write the failing `append_signal` unit tests**

Add to `crates/persist/src/history_impl.rs` `mod tests` (after the last test). Note
`SignalOutcome` must be added to the test `use engine::{...}` — update the import in
the test module (it currently imports `engine::{ExecStatus, NewActivityTask}`):

```rust
    use engine::{ExecStatus, NewActivityTask, SignalOutcome};
```

Then add the tests:

```rust
    #[tokio::test]
    async fn append_signal_delivers_to_running_and_marks_runnable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        // Drive the create's runnable flag away first (simulate the run going idle).
        let idle = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &idle).await.unwrap();
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db).await.unwrap(),
            None
        );

        let out = db.append_signal("wf-A", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::Delivered);

        // The SignalReceived event is appended with NULL seq, and the run is runnable.
        let h = db.read_history("run-1").await.unwrap();
        match &h.last().unwrap().event {
            Event::SignalReceived { name, payload } => {
                assert_eq!(name, "approve");
                assert_eq!(payload, b"true");
            }
            other => panic!("expected SignalReceived, got {other:?}"),
        }
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db).await.unwrap(),
            Some("run-1".into())
        );
    }

    #[tokio::test]
    async fn append_signal_unknown_id_is_not_found() {
        let db = Sqlite::open_in_memory().unwrap();
        let out = db.append_signal("nope", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::WorkflowNotFound);
    }

    #[tokio::test]
    async fn append_signal_to_terminal_run_is_not_running() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        // Mark the run completed.
        let done = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            status: ExecStatus::Completed,
            result: Some(b"\"done\"".to_vec()),
        };
        db.commit_turn("run-1", &done).await.unwrap();

        let out = db.append_signal("wf-A", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::NotRunning);
        // No SignalReceived was appended (last event is still WorkflowStarted).
        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(h.last().unwrap().event, Event::WorkflowStarted { .. }));
    }
```

Run: `cargo test -p persist --lib append_signal`
Expected: FAIL — `persist` does not compile because `append_signal` is not
implemented (unsatisfied trait method).

- [ ] **Step 2: Implement `append_signal`**

In `crates/persist/src/history_impl.rs`, update the engine import to bring in
`SignalOutcome`:

```rust
use engine::{CreateOutcome, ExecStatus, History, RunMeta, SignalOutcome, StoredEvent, TurnCommit};
```

Add the method to `impl History for Sqlite` (after `find_execution`):

```rust
    async fn append_signal(
        &self,
        workflow_id: &str,
        name: &str,
        payload: &[u8],
    ) -> anyhow::Result<SignalOutcome> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // Resolve the run + status under the same transaction as the append, so the
        // status check and the write are atomic (spec §6.1).
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT run_id, status FROM executions WHERE workflow_id = ?1",
                params![workflow_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;

        let Some((run_id, status)) = row else {
            tx.commit()?;
            return Ok(SignalOutcome::WorkflowNotFound);
        };
        if ExecStatus::from_str(&status) != Some(ExecStatus::Running) {
            tx.commit()?;
            return Ok(SignalOutcome::NotRunning);
        }

        // Append SignalReceived (inbound → seq NULL) and re-arm the runnable queue.
        let event = Event::SignalReceived {
            name: name.to_string(),
            payload: payload.to_vec(),
        };
        let payload_bytes = serde_json::to_vec(&event)?;
        let next_id = next_event_id(&tx, &run_id)?;
        tx.execute(
            "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, NULL, ?3, ?4, ?5)",
            params![run_id, next_id, event.kind(), payload_bytes, now_ms()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
            params![run_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(SignalOutcome::Delivered)
    }
```

- [ ] **Step 3: Run persist tests + whole-workspace build**

Run: `cargo test -p persist`
Expected: PASS — the three new `append_signal` tests plus all earlier persist tests.

Run: `cargo build`
Expected: the whole workspace compiles green again.

- [ ] **Step 4: Commit**

```bash
git add crates/persist
git commit -m "feat(persist): append_signal — single-transaction durable signal delivery"
```

---

### Task 3: Pass-3 acceptance — signal e2e (blocked-recv, signal-or-timeout, crash, NotRunning)

**Files:**
- Create: `crates/engine/tests/signals.rs`

- [ ] **Step 1: Write the integration tests**

Create `crates/engine/tests/signals.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use engine::{Engine, ExecStatus, History, SignalError, StartOptions};
use futures::{select_biased, FutureExt};
use persist::Sqlite;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Approval {
    ok: bool,
}

// Workflow that blocks on a single "approve" signal and returns its `ok` flag.
struct WaitApprove;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for WaitApprove {
    type Input = ();
    type Output = bool;
    const TYPE: &'static str = "WaitApprove";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<bool, workflow::Error> {
        let approvals = ctx.signal_channel::<Approval>("approve");
        let a = approvals.recv().await?;
        Ok(a.ok)
    }
}

// Signal-or-timeout: race a "approve" signal against a sleep whose duration is the
// workflow input (ms). `select_biased!` is the deterministic Selector analog (§6.3).
struct SignalOrTimeout;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for SignalOrTimeout {
    type Input = u64; // timeout in ms
    type Output = String;
    const TYPE: &'static str = "SignalOrTimeout";
    async fn run(ctx: workflow::Context, timeout_ms: u64) -> Result<String, workflow::Error> {
        let approvals = ctx.signal_channel::<Approval>("approve");
        let recv = approvals.recv().fuse();
        let nap = ctx.sleep(Duration::from_millis(timeout_ms)).fuse();
        futures::pin_mut!(recv, nap);
        let out = select_biased! {
            a = recv => {
                let a = a?;
                if a.ok { "approved" } else { "rejected" }
            }
            _ = nap => "timed_out",
        };
        Ok(out.to_string())
    }
}

fn build<W: workflow::Definition>(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<W>();
    e
}

/// Pump driver + worker + timer turns until quiescent (deterministic).
async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        let timed = engine.process_one_timer().await?;
        if !drove && !worked && !timed {
            return Ok(());
        }
    }
}

fn approval(ok: bool) -> Vec<u8> {
    serde_json::to_vec(&Approval { ok }).unwrap()
}

#[tokio::test]
async fn workflow_blocked_on_recv_resumes_when_signaled() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<WaitApprove>(&db);
    let handle = engine
        .start_workflow::<WaitApprove>((), StartOptions { id: "sig-1".into() })
        .await
        .unwrap();

    // Drive until it blocks on recv() — no further progress, still running.
    pump(&engine).await.unwrap();
    let (_, status, _) = db.find_execution("sig-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Running, "blocked on recv(), not yet complete");

    // Deliver the signal; it should resume and complete.
    engine
        .signal_workflow("sig-1", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine).await.unwrap();

    let out: bool = handle.result().await.unwrap();
    assert!(out, "the delivered Approval{{ ok: true }} resumes recv()");
}

#[tokio::test]
async fn signal_or_timeout_takes_the_signal() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<SignalOrTimeout>(&db);
    // A day-long timeout: the timer never fires, so the signal branch must win.
    let handle = engine
        .start_workflow::<SignalOrTimeout>(86_400_000, StartOptions { id: "sot-1".into() })
        .await
        .unwrap();

    assert!(engine.process_one_runnable().await.unwrap()); // turn 1: schedules the timer, blocks on recv
    engine
        .signal_workflow("sot-1", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine).await.unwrap();

    let out: String = handle.result().await.unwrap();
    assert_eq!(out, "approved", "the signal wins; the day-long timer never fires");
}

#[tokio::test]
async fn signal_or_timeout_times_out() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<SignalOrTimeout>(&db);
    // A 0ms timeout is due immediately; with no signal delivered, the timer wins.
    let handle = engine
        .start_workflow::<SignalOrTimeout>(0, StartOptions { id: "sot-2".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: String = handle.result().await.unwrap();
    assert_eq!(out, "timed_out", "no signal arrives; the immediate timer wins");
}

#[tokio::test]
async fn signal_before_crash_replays_to_completion() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn (recv pends), deliver the signal durably, crash.
    {
        let engine = build::<WaitApprove>(&db);
        engine
            .start_workflow::<WaitApprove>((), StartOptions { id: "sig-3".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // turn 1: recv pending, run goes idle
        engine
            .signal_workflow("sig-3", "approve", &approval(true))
            .await
            .unwrap();
        // engine dropped here; only the shared `db` survives. The SignalReceived row
        // is durable, but the driver has not yet consumed it.
    }

    // Phase 2: a fresh engine cold-replays [WorkflowStarted, SignalReceived] and
    // completes — the signal resolves recv() identically on replay (Invariant 10).
    let engine2 = build::<WaitApprove>(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("sig-3").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: bool = serde_json::from_slice(&result.unwrap()).unwrap();
    assert!(out);
}

#[tokio::test]
async fn signaling_completed_or_unknown_run_errors() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<WaitApprove>(&db);
    engine
        .start_workflow::<WaitApprove>((), StartOptions { id: "sig-4".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap(); // blocks on recv()

    // First signal completes it.
    engine
        .signal_workflow("sig-4", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let (_, status, _) = db.find_execution("sig-4").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);

    // Signaling the now-completed run errors NotRunning (Temporal-faithful, §6.1).
    let err = engine
        .signal_workflow("sig-4", "approve", &approval(true))
        .await
        .unwrap_err();
    assert!(matches!(err, SignalError::NotRunning), "got {err:?}");

    // Signaling an unknown id errors WorkflowNotFound.
    let err = engine
        .signal_workflow("does-not-exist", "approve", &approval(true))
        .await
        .unwrap_err();
    assert!(matches!(err, SignalError::WorkflowNotFound), "got {err:?}");
}
```

- [ ] **Step 2: Run the acceptance tests**

Run: `cargo test -p engine --test signals`
Expected: all five PASS — `workflow_blocked_on_recv_resumes_when_signaled`,
`signal_or_timeout_takes_the_signal`, `signal_or_timeout_times_out`,
`signal_before_crash_replays_to_completion`,
`signaling_completed_or_unknown_run_errors`.

- [ ] **Step 3: Commit**

```bash
git add crates/engine/tests/signals.rs
git commit -m "test(engine): pass 3 acceptance — blocked-recv resume, signal-or-timeout, crash replay, NotRunning"
```

---

### Task 4: Whole-workspace green + clippy + roadmap update

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS (all earlier passes
  still green; the new `signals.rs` is additive).
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean.
- [ ] **Step 3:** In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
  - In the chunk table, set chunk `3b` status to `done` and its Plan file to
    `2026-06-14-pass-3b-signal-workflow-and-e2e.md`.
  - In the "Canonical types" section: add `SignalOutcome` and the
    `History::append_signal` method to the `engine` block; replace the
    `pub enum SignalError { WorkflowNotFound, NotRunning }   // Pass 3` line with the
    realized three-variant enum (note the `Internal` escape hatch); and add
    `Engine::signal_workflow` / `Handle::signal` to the host-surface notes.
- [ ] **Step 4:** Commit:

```bash
git add -A
git commit -m "chore: pass 3b complete — signal host delivery + e2e, roadmap + canonical types updated"
```

---

## Notes

- **Why a `History` method rather than a check-then-act in the engine.** Doing the
  status lookup and the append in the same SQLite transaction is what makes the
  "durable-before-return, never buffer for a non-running run" contract (spec §6.1)
  race-free. The engine stays backend-agnostic: it maps `SignalOutcome` → `SignalError`
  and never touches SQL.
- **No inbound table, no dedup** (spec §6.1): a `SignalReceived` row in `history`
  plus the existing `runnable` queue is the entire mechanism. Duplicates are the
  frontend's call; the engine is at-least-once in contract, exactly-once in practice.
- **`select_biased!` requires fused, pinned futures** — the e2e workflow uses
  `.fuse()` + `futures::pin_mut!`, exactly as the Pass 2b `concurrency.rs` tests do.
  Biased order makes `recv` the priority branch; under one-event-per-turn only one
  branch can become ready per poll, so the winner is a pure function of history
  (spec §4.2).
- **Cooperative cancellation** remains deferred (spec §6.4): it slots in as a second
  inbound-event kind (`WorkflowCancelRequested`) reusing this exact append path —
  `append_signal` is deliberately the generic shape that makes that a later addition,
  not a refactor.

## Self-Review (completed during authoring)

- **Spec coverage:** §6.1 (single-transaction append + runnable, durable-before-return,
  `WorkflowNotFound`/`NotRunning` typed errors, no dedup/table), §7.2 (`signal_workflow`
  / `handle.signal` host entrypoints), §13 Pass-3 acceptance gate — all four clauses:
  blocked `recv()` resumes when signaled, signal-or-timeout resolves each way
  deterministically, a signal before a crash replays identically, signaling a completed
  run returns `NotRunning` (plus the unknown-id `WorkflowNotFound` case).
- **Placeholders:** none — full code, exact commands, expected outcomes throughout.
- **Type consistency:** `SignalOutcome { Delivered, WorkflowNotFound, NotRunning }`,
  `History::append_signal(&self, &str, &str, &[u8]) -> anyhow::Result<SignalOutcome>`,
  `SignalError { WorkflowNotFound, NotRunning, Internal(#[from] anyhow::Error) }`,
  `Engine::signal_workflow(&self, &str, &str, &[u8]) -> Result<(), SignalError>`,
  `Handle::signal(&self, &str, &[u8]) -> Result<(), SignalError>`, and the
  `outcome_to_result` mapping are used identically across the engine and persist tasks.
  Depends only on Pass 3a's `Event::SignalReceived` + `ctx.signal_channel`/`recv`,
  which the e2e workflows consume directly.
```
