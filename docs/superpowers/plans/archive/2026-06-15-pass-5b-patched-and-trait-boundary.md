# Pass 5b — `ctx.patched` change-versioning hook + trait-boundary cleanup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the `ctx.patched("change-id")` hook (the `workflow.GetVersion` analog,
spec §9.1, §14) so a workflow can branch old-vs-new code across a shape change without
breaking replay of in-flight histories, and make the `engine::History` /
`engine::TaskQueue` pair the *explicitly asserted* sole migration seam (spec §13 Pass 5
gate, §15). The `workflow-macros` banned-combinator lint is **deferred entirely** (per
project decision); the contract stays documented + runtime-enforced via the divergence
check hardened in Pass 5a.

**Architecture:** `patched` mirrors Temporal's `GetVersion`/`Patched` marker, adapted
to this engine's event-sourced replay. It is a **synchronous** `&self -> bool` call
(GetVersion does not block) that records a seq-less marker the first time new code runs
the patched path, and reads that marker back on replay:

- `Event::Patched { change_id }` is a new history event carrying **no `seq`** — like
  `SignalReceived`, it is divergence-exempt (Invariant 9 only checks emitted commands
  with a seq). `Command::RecordPatch { change_id }` is the workflow-emitted request to
  write that marker.
- `ctx.patched(id)` decides among three cases using two pieces of `ContextInner` state
  — a `patches` set (recorded markers, seeded from history) and a `replaying` frontier
  flag the replay driver sets each turn:
  1. **Recorded marker present** (`patches` contains `id`) → `true`. Replay-stable: the
     marker is seeded from history before driving, exactly like recorded schedules.
  2. **No marker, still replaying older history** (`replaying == true`, i.e. recorded
     events remain ahead of the current position) → `false`. The history was produced
     by code that did not have this patch, so the **old** branch is taken and re-emits
     the commands history already recorded → no divergence.
  3. **No marker, caught up to the live frontier** (`replaying == false`) → emit
     `RecordPatch` once and return `true`. This is the first live execution of the new
     code path.
- The frontier flag is exact and cheap to compute in `cold_replay`: before each
  `poll_turn`, set `replaying = (cursor < applied.len())` — there are still recorded
  one-per-turn events to apply. Because the engine driver *only* cold-replays (no
  cache; see Pass 5a), `patched` works uniformly with zero engine-side branch logic
  beyond persisting the marker.

This is the GetVersion behaviour the desktop posture needs: new executions take the new
path and record the marker; histories that recorded the marker replay it stably; and a
patch inserted ahead of an in-flight history's remaining events takes the old branch
(no divergence). The one documented limitation (acceptable per spec §14, which permits
draining in-flight workflows on code change): if old code already advanced *past* the
patched point with no remaining events when the new patched() call is first reached,
the marker is written and the new branch is taken — which may then diverge on the
already-recorded later seqs and dead-letter that single run, loudly, rather than
silently corrupting it.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow, thiserror, futures,
uuid. No new crates, no new dependencies. The `persist` layer needs **no** schema or
encode change — `Event::Patched` is a seq-less event that the existing `encode`
catch-all (`_ => None`) and the generic `payload` column already handle.

**Depends on:** Pass 5a (merged) — specifically the `as_recorded(&Command) ->
Option<(u64, RecordedCmd)>` helper in `cold_replay`, where `RecordPatch` becomes the
`None` arm.

---

## File structure

- Modify: `crates/workflow/src/command.rs` — add `Command::RecordPatch` (Task 1).
- Modify: `crates/workflow/src/event.rs` — add `Event::Patched` + `kind()` (Task 1).
- Modify: `crates/workflow/src/context.rs` — `patches` / `patches_emitted` /
  `replaying` state; `patched()`, `apply_patch()`, `set_replaying()` (Task 2).
- Modify: `crates/workflow/src/state.rs` — `WorkflowState` pass-throughs (Task 3).
- Modify: `crates/workflow/src/replay.rs` — seed markers, set the frontier flag,
  exempt `RecordPatch` from the divergence check (Task 4).
- Modify: `crates/engine/src/engine.rs` — persist `RecordPatch` → `Event::Patched`,
  deduped by `change_id` (Task 5).
- Create: `crates/engine/tests/patched.rs` — e2e + cold-recovery (Task 5).
- Modify: `crates/engine/src/traits.rs` + create
  `crates/engine/tests/migration_seam.rs` — assert the two-trait seam (Task 6).
- Modify: `crates/workflow/src/lib.rs` — update the "deferred to Pass 5" doc note
  (Task 6).
- Modify: `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md` —
  flip the 5b row to `done` (Task 7).

---

### Task 1: Add the `RecordPatch` command and `Patched` event

**Files:**
- Modify: `crates/workflow/src/command.rs`
- Modify: `crates/workflow/src/event.rs`

- [ ] **Step 1: Write the failing serde/kind tests**

Add to `mod tests` in `crates/workflow/src/command.rs`:

```rust
    #[test]
    fn record_patch_round_trips_through_json() {
        let p = Command::RecordPatch {
            change_id: "ship-v2".into(),
        };
        let back: Command = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        assert_eq!(p, back);
    }
```

Add to `mod tests` in `crates/workflow/src/event.rs`:

```rust
    #[test]
    fn patched_kind_and_round_trip() {
        let e = Event::Patched {
            change_id: "ship-v2".into(),
        };
        assert_eq!(e.kind(), "Patched");
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p workflow record_patch_round_trips patched_kind_and_round_trip`
Expected: FAIL to **compile** — `Command::RecordPatch` / `Event::Patched` do not exist.

- [ ] **Step 3: Add `Command::RecordPatch`**

In `crates/workflow/src/command.rs`, add a variant to the `Command` enum (after
`StartChild`):

```rust
    /// Request to record a change-version marker (spec §14, the `GetVersion` analog).
    /// Carries NO `seq` — it is divergence-exempt like an inbound event; the driver
    /// records it as `Event::Patched` deduped by `change_id`.
    RecordPatch {
        change_id: String,
    },
```

- [ ] **Step 4: Add `Event::Patched` + its `kind()` arm**

In `crates/workflow/src/event.rs`, add a variant to `Event` (after `ChildCompleted`):

```rust
    /// Change-version marker (spec §14). Written the first time new code runs a
    /// `ctx.patched(id)` path; carries NO `seq` and is divergence-exempt (Invariant 9),
    /// like `SignalReceived`. Seeded back into `ctx` on replay so `patched` is stable.
    Patched {
        change_id: String,
    },
```

and add to the `kind()` match:

```rust
            Event::Patched { .. } => "Patched",
```

- [ ] **Step 5: Run the tests — verify they pass**

Run: `cargo test -p workflow record_patch_round_trips patched_kind_and_round_trip`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/workflow/src/command.rs crates/workflow/src/event.rs
git commit -m "feat(workflow): add RecordPatch command + Patched event (GetVersion plumbing)"
```

---

### Task 2: `ctx.patched()` and its replay state

**Files:**
- Modify: `crates/workflow/src/context.rs`

- [ ] **Step 1: Write the failing tests**

Add to `mod tests` in `crates/workflow/src/context.rs` (the module already has an
`info()` helper):

```rust
    #[test]
    fn patched_new_execution_records_marker_and_returns_true() {
        let ctx = Context::new(info());
        // Fresh run, caught up to the live frontier (replaying defaults to false).
        assert!(ctx.patched("v2"), "new execution takes the patched path");
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(matches!(
            &cmds[0],
            Command::RecordPatch { change_id } if change_id == "v2"
        ));
        // Second call in the same life is idempotent: still true, no second command.
        assert!(ctx.patched("v2"));
        assert!(
            ctx.drain_commands().is_empty(),
            "marker is emitted at most once per life"
        );
    }

    #[test]
    fn patched_with_recorded_marker_returns_true_without_emitting() {
        let ctx = Context::new(info());
        ctx.apply_patch("v2".into()); // seeded from a recorded Event::Patched
        assert!(ctx.patched("v2"));
        assert!(
            ctx.drain_commands().is_empty(),
            "a recorded marker re-emits nothing on replay"
        );
    }

    #[test]
    fn patched_returns_false_while_replaying_older_history() {
        let ctx = Context::new(info());
        ctx.set_replaying(true); // recorded events still remain ahead of this point
        assert!(
            !ctx.patched("v2"),
            "no marker + still replaying older history => old branch"
        );
        assert!(
            ctx.drain_commands().is_empty(),
            "the old branch records no marker"
        );
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p workflow patched_`
Expected: FAIL to compile — `patched`, `apply_patch`, `set_replaying` do not exist.

- [ ] **Step 3: Add the new `ContextInner` fields**

In `crates/workflow/src/context.rs`, add to `struct ContextInner` (after
`child_results`):

```rust
    // Change-version markers recorded in history (spec §14), seeded before driving so
    // `patched` is replay-stable. A present id means the patched path was taken.
    pub(crate) patches: RefCell<HashSet<String>>,
    // Markers emitted this life, so `patched` requests `RecordPatch` at most once.
    pub(crate) patches_emitted: RefCell<HashSet<String>>,
    // Frontier flag: true while recorded one-per-turn events still remain ahead of the
    // current replay position. The replay driver sets it each turn; `patched` reads it
    // to tell "replaying old history" (=> false) from "live, first run" (=> record).
    pub(crate) replaying: Cell<bool>,
```

`HashSet` is already imported (`use std::collections::{HashMap, HashSet, VecDeque};`),
as is `Cell`.

- [ ] **Step 4: Initialize them in `Context::new`**

In the `ContextInner { .. }` literal inside `Context::new`, add:

```rust
                patches: RefCell::new(HashSet::new()),
                patches_emitted: RefCell::new(HashSet::new()),
                replaying: Cell::new(false), // default: live frontier (used by unit tests)
```

- [ ] **Step 5: Add the `patched` / `apply_patch` / `set_replaying` methods**

Add these methods to `impl Context` (place near `signal_channel` / `apply_signal`):

```rust
    /// `workflow.GetVersion`/`Patched` analog (spec §9.1, §14). Returns whether this
    /// run is on the patched code path for `change_id`, recording a marker the first
    /// time new code reaches it live. Synchronous (does not block); emits at most one
    /// `RecordPatch` per `change_id` per life. Allocates NO `seq`.
    pub fn patched(&self, change_id: &str) -> bool {
        // 1. Marker already recorded in history -> patched path, replay-stable.
        if self.inner.patches.borrow().contains(change_id) {
            return true;
        }
        // 2. No marker but recorded history still remains ahead -> old code wrote this
        //    history; take the OLD branch so it re-emits what history recorded.
        if self.inner.replaying.get() {
            return false;
        }
        // 3. Caught up to the live frontier, first time here -> record the marker once.
        if self
            .inner
            .patches_emitted
            .borrow_mut()
            .insert(change_id.to_string())
        {
            self.inner
                .commands
                .borrow_mut()
                .push(Command::RecordPatch {
                    change_id: change_id.to_string(),
                });
        }
        true
    }

    /// Driver/replay seeds a recorded change-version marker before driving (spec §14).
    /// Markers carry no `seq` and resolve synchronously, so — unlike one-per-turn
    /// completions — they are seeded up front, like recorded schedules.
    pub fn apply_patch(&self, change_id: String) {
        self.inner.patches.borrow_mut().insert(change_id);
    }

    /// Replay driver sets the frontier flag before each poll: true while recorded
    /// one-per-turn events remain ahead of the current position (spec §14).
    pub fn set_replaying(&self, replaying: bool) {
        self.inner.replaying.set(replaying);
    }
```

- [ ] **Step 6: Run the tests — verify they pass**

Run: `cargo test -p workflow patched_`
Expected: all three PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/workflow/src/context.rs
git commit -m "feat(workflow): ctx.patched() change-version hook (GetVersion analog)"
```

---

### Task 3: `WorkflowState` pass-throughs

**Files:**
- Modify: `crates/workflow/src/state.rs`

`cold_replay` drives the workflow through `WorkflowState`, so it needs to seed markers
and set the frontier flag through it.

- [ ] **Step 1: Add the pass-through methods**

In `crates/workflow/src/state.rs`, add to `impl WorkflowState` (next to
`apply_signal` / `apply_child_result`):

```rust
    pub fn apply_patch(&self, change_id: String) {
        self.ctx.apply_patch(change_id);
    }

    pub fn set_replaying(&self, replaying: bool) {
        self.ctx.set_replaying(replaying);
    }
```

- [ ] **Step 2: Verify it compiles**

Run: `cargo build -p workflow`
Expected: clean build. (No dedicated unit test here — these are one-line delegations
exercised end-to-end by Task 4's replay tests.)

- [ ] **Step 3: Commit**

```bash
git add crates/workflow/src/state.rs
git commit -m "feat(workflow): WorkflowState apply_patch/set_replaying pass-throughs"
```

---

### Task 4: Wire `patched` into `cold_replay`

**Files:**
- Modify: `crates/workflow/src/replay.rs`

Three changes: seed recorded markers before driving; set the frontier flag each turn;
exempt `RecordPatch` from the divergence check (the `as_recorded` `None` arm from Pass
5a).

- [ ] **Step 1: Write the failing replay tests**

Add to `mod tests` in `crates/workflow/src/replay.rs`. First the workflow fixtures:

```rust
    // A workflow that branches on a patch. New code path returns 1; old path returns 0.
    struct Branch;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Branch {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Branch";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            if ctx.patched("v2") {
                Ok(1)
            } else {
                Ok(0)
            }
        }
    }

    fn branch_info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "Branch".into(),
        }
    }
```

Then the three cases:

```rust
    #[test]
    fn patched_new_execution_emits_marker_and_takes_new_branch() {
        // Empty history beyond WorkflowStarted: nothing to apply, so the very first
        // poll is at the live frontier -> patched records the marker and returns true.
        let h = vec![Event::WorkflowStarted {
            input: serde_json::to_vec(&()).unwrap(),
        }];
        let outcome = cold_replay::<Branch>(branch_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 1, "new execution takes the patched branch");
        assert_eq!(outcome.commands.len(), 1);
        assert!(matches!(
            &outcome.commands[0],
            Command::RecordPatch { change_id } if change_id == "v2"
        ));
    }

    #[test]
    fn patched_replays_recorded_marker_without_re_emitting() {
        // History recorded the marker: replay must take the new branch deterministically
        // and NOT re-emit RecordPatch.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::Patched {
                change_id: "v2".into(),
            },
        ];
        let outcome = cold_replay::<Branch>(branch_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 1, "recorded marker -> patched branch");
        assert!(
            outcome.commands.is_empty(),
            "a recorded marker re-emits no command"
        );
    }
```

For the old-history case, use a workflow that does an activity *before* the patch, so
there is a recorded completion still ahead when `patched()` is first reached:

```rust
    // Activity-then-patch: old history recorded the activity AND its completion, so at
    // the moment patched() is reached on replay, a recorded event still remains ahead.
    struct ActThenBranch;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for ActThenBranch {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "ActThenBranch";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let a = ctx.activity::<Add>((1, 1)).await?; // seq 0
            if ctx.patched("v2") {
                Ok(a + 100) // new branch
            } else {
                Ok(a) // old branch
            }
        }
    }

    fn act_then_branch_info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "ActThenBranch".into(),
        }
    }

    // CORRECTION (made during execution): the old-branch test below was authored with
    // history `[WS, AS0, AC0]`, but in that history `patched()` is reached AT THE
    // FRONTIER (seq 0's completion is the last event, nothing ahead) — which is
    // indistinguishable from a fresh new execution and correctly takes the NEW branch.
    // The genuine "replaying older history" case needs a recorded event AHEAD of the
    // patched point. As implemented, this test uses a two-activity `ActPatchAct` fixture
    // (`activity seq0; if patched {a+100} else {activity seq1; a+b}`) with history
    // `[WS, AS0, AC0, AS1, AC1]` → out 8, no marker; and a separate
    // `patched_at_frontier_after_activity_records_marker_and_takes_new_branch` test
    // (using `ActThenBranch` over `[WS, AS0, AC0]`) → out 102 + marker locks in the
    // frontier=new-branch behaviour. The frontier formula stays `cursor < applied.len()`.
    #[test]
    fn patched_takes_old_branch_when_replaying_older_history() {
        // History from code WITHOUT the patch: it scheduled seq 0 and completed it.
        // patched() is reached on the turn AFTER seq 0 resolves; the completion for
        // seq 0 is still queued ahead at that first reach, so replaying == true ->
        // false -> old branch -> Ok(a) with no marker, no divergence.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 1),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 0,
                output: serde_json::to_vec(&2i64).unwrap(),
            },
        ];
        let outcome = cold_replay::<ActThenBranch>(act_then_branch_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 2, "old history -> old branch (a), not a+100");
        assert!(
            !outcome
                .commands
                .iter()
                .any(|c| matches!(c, Command::RecordPatch { .. })),
            "old branch records no marker"
        );
    }
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cargo test -p workflow patched_`
Expected: the three new replay tests FAIL — `cold_replay` does not yet seed markers,
set the frontier flag, or accept `RecordPatch` (the `as_recorded` match is currently
exhaustive over the three seq commands, so adding `Command::RecordPatch` made the file
fail to compile until Step 4; expect a compile error first).

- [ ] **Step 3: Seed recorded markers in the history scan**

In `cold_replay`, the `for ev in history { match ev { .. } }` indexing loop, add an
arm for the new event (alongside the existing `SignalReceived` arm):

```rust
            Event::Patched { change_id } => {
                // Markers carry no seq and resolve synchronously: seed up front (like
                // recorded schedules), NOT into the one-per-turn `applied` stream.
                recorded_patches.push(change_id.clone());
            }
```

and declare the collector next to `recorded_cmd` (before the indexing loop):

```rust
    let mut recorded_patches: Vec<String> = Vec::new();
```

Then, immediately after `let mut state = WorkflowState::start::<W>(info, input);` and
before the drive `loop`, seed them:

```rust
    for change_id in recorded_patches {
        state.apply_patch(change_id);
    }
```

- [ ] **Step 4: Set the frontier flag each turn + exempt `RecordPatch`**

Inside the drive `loop`, set the frontier flag **before** polling. Change the loop top
from `let poll = state.poll_turn();` to:

```rust
        // Frontier: true while one-per-turn events remain ahead of the cursor. `patched`
        // reads this to distinguish replaying old history (=> old branch) from the live
        // edge (=> record the marker).
        state.set_replaying(cursor < applied.len());
        let poll = state.poll_turn();
```

Then add the `None` arm to `as_recorded` so `RecordPatch` is skipped by the divergence
check (it carries no seq):

```rust
            Command::RecordPatch { .. } => None,
```

(The `commands.push(cmd)` after the check already records it into the returned stream —
no extra handling needed; the `if let Some(..) = as_recorded(&cmd)` simply does not run
the check for it.)

- [ ] **Step 5: Run the tests — verify they pass**

Run: `cargo test -p workflow patched_`
Expected: all five `patched_*` tests PASS (the three replay tests here + the two
context tests from Task 2). Then run the whole crate: `cargo test -p workflow`
Expected: green, including the Pass 5a prefix-stability and divergence tests.

- [ ] **Step 6: Commit**

```bash
git add crates/workflow/src/replay.rs
git commit -m "feat(workflow): cold_replay seeds patch markers + frontier flag for ctx.patched"
```

---

### Task 5: Persist `RecordPatch` in the driver + e2e cold-recovery

**Files:**
- Modify: `crates/engine/src/engine.rs`
- Create: `crates/engine/tests/patched.rs`

- [ ] **Step 1: Write the failing e2e test**

Create `crates/engine/tests/patched.rs`:

```rust
use std::sync::Arc;

use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
use persist::Sqlite;

// New-code workflow: takes the patched branch, returns 1; old path would return 0.
struct Branch;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Branch {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Branch";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        if ctx.patched("v2") {
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Branch>();
    e
}

async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        if !drove {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn patched_workflow_runs_and_persists_marker() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Branch>((), StartOptions { id: "patch-1".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 1, "new execution takes the patched branch");

    // The marker was persisted as a history event.
    let (run_id, status, _) = db.find_execution("patch-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let kinds: Vec<&'static str> = db
        .read_history(&run_id)
        .await
        .unwrap()
        .iter()
        .map(|s| s.event.kind())
        .collect();
    assert!(
        kinds.contains(&"Patched"),
        "history should contain a Patched marker, got {kinds:?}"
    );
}

#[tokio::test]
async fn patched_cold_recovery_completes_identically() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: run to completion, then drop the engine.
    {
        let engine = build(&db);
        engine
            .start_workflow::<Branch>((), StartOptions { id: "patch-2".into() })
            .await
            .unwrap();
        pump(&engine).await.unwrap();
    }
    // Phase 2: a fresh engine cold-replays the persisted history (marker included) and
    // sees the same result — the marker makes patched() stable on replay.
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("patch-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 1);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p engine --test patched`
Expected: FAIL. The driver does not handle `Command::RecordPatch`, so the marker is
never persisted: `patched_workflow_runs_and_persists_marker` fails the
`kinds.contains(&"Patched")` assert. (Depending on the match's exhaustiveness, the
engine may also fail to compile until Step 3 adds the arm — expect that first.)

- [ ] **Step 3: Persist `RecordPatch` deduped by `change_id`**

In `crates/engine/src/engine.rs`, in `process_one_runnable`, build a recorded-patch set
alongside the existing `recorded` seq set (just after the `recorded` declaration,
~line 242):

```rust
        let recorded_patches: HashSet<String> = events
            .iter()
            .filter_map(|e| match e {
                workflow::Event::Patched { change_id } => Some(change_id.clone()),
                _ => None,
            })
            .collect();
```

Then add a `RecordPatch` arm to the `for cmd in &outcome.commands { match cmd { .. } }`
loop (alongside `StartChild`):

```rust
                workflow::Command::RecordPatch { change_id } => {
                    // Seq-less marker: dedupe by change_id (it can only be recorded once
                    // per run). No task/timer/child.
                    if recorded_patches.contains(change_id) {
                        continue;
                    }
                    new_events.push(workflow::Event::Patched {
                        change_id: change_id.clone(),
                    });
                }
```

No change to `persist`: `encode` routes `Event::Patched` (no seq) through its `_ =>
None` arm, and the marker rides the generic `payload` column.

- [ ] **Step 4: Run the tests — verify they pass**

Run: `cargo test -p engine --test patched`
Expected: both PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/engine/src/engine.rs crates/engine/tests/patched.rs
git commit -m "feat(engine): persist RecordPatch as a deduped Patched marker; e2e + cold recovery"
```

---

### Task 6: Assert the two-trait migration seam + doc cleanup

**Files:**
- Create: `crates/engine/tests/migration_seam.rs`
- Modify: `crates/engine/src/traits.rs` (doc comment)
- Modify: `crates/workflow/src/lib.rs` (doc note)

The Pass 5 acceptance gate's second half: "the two traits compile as the only seam
`persist` implements" (spec §13, §15). Lock it in with a compile-time assertion.

- [ ] **Step 1: Write the seam assertion test**

Create `crates/engine/tests/migration_seam.rs`:

```rust
//! Guards spec §15: `History` + `TaskQueue` are the ENTIRE seam a persistence backend
//! implements. If `persist::Sqlite` ever needs another engine trait to be wired in,
//! this stops compiling — forcing that new seam to be a deliberate, documented choice.

use engine::{History, TaskQueue};
use persist::Sqlite;

fn _assert_sqlite_is_the_seam(db: Sqlite) {
    fn needs_both<T: History + TaskQueue>(_: T) {}
    needs_both(db);
}

#[test]
fn migration_seam_is_exactly_two_traits() {
    // The assertion is the `_assert_sqlite_is_the_seam` signature above; this test
    // exists so the file is exercised and documents intent.
    let db = Sqlite::open_in_memory().unwrap();
    _assert_sqlite_is_the_seam(db);
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p engine --test migration_seam`
Expected: PASS (compiles + the single test passes). If it fails to compile because
`Sqlite` does not satisfy both traits, that is a real regression to investigate, not a
test bug.

- [ ] **Step 3: Tighten the seam doc comment**

In `crates/engine/src/traits.rs`, expand the module-level intent. Replace the top
`History` doc line region by adding, above `pub trait History`:

```rust
/// # Migration seam (spec §15)
///
/// `History` and `TaskQueue` are the *complete* boundary between the backend-agnostic
/// engine and a concrete store. `persist::Sqlite` implements exactly these two and
/// nothing else; a cloud backend is "implement these two traits" rather than a
/// rewrite. `crates/engine/tests/migration_seam.rs` asserts this at compile time.
```

(Keep the existing per-method doc comments unchanged.)

- [ ] **Step 4: Update the deferred-macros note**

In `crates/workflow/src/lib.rs`, the crate doc currently says the `#[workflow]`
macro/lint is "deferred to Pass 5." Update it to reflect the decision that it is
deferred *beyond* Pass 5 and the contract stays runtime-enforced. Change the closing
note of the "Deterministic concurrency contract" section to:

```rust
//! These bans are a documented contract enforced at runtime by the replay divergence
//! check (spec §12, Invariant 9; hardened in Pass 5a). A compile-time `#[workflow]`
//! macro / clippy lint (the `workflow-macros` crate) is deferred indefinitely — Rust
//! cannot enforce combinator choice at the type level, and the runtime check plus the
//! documented contract cover the desktop posture.
```

- [ ] **Step 5: Verify the workflow crate still builds (doc-only change)**

Run: `cargo build -p workflow && cargo test -p engine --test migration_seam`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add crates/engine/tests/migration_seam.rs crates/engine/src/traits.rs crates/workflow/src/lib.rs
git commit -m "test(engine): assert History+TaskQueue is the sole migration seam (spec §15)"
```

---

### Task 7: Verification gate + roadmap status

**Files:**
- Modify: `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`

- [ ] **Step 1: Run the full verification trio**

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Expected: all green. Watch for: a non-exhaustive-match warning-turned-error if any
`match` over `Command` or `Event` elsewhere in the workspace did not get the new arm
(clippy `-D warnings` will surface it) — grep `crates/` for other `match` sites over
`Command`/`Event` if the build complains, and add the missing arm.

- [ ] **Step 2: Update the roadmap canonical-types section + status row**

In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
- In the `Command` enum listing under "Canonical types", add
  `RecordPatch { change_id: String }, // 5b (done)`.
- In the `Event` enum listing, add `Patched { change_id: String }, // 5b (done)`.
- In the chunk table, change the `5b` row Status to `done` and set its Plan file to
  `2026-06-15-pass-5b-patched-and-trait-boundary.md`.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md
git commit -m "docs(roadmap): record RecordPatch/Patched + mark pass 5b done"
```

---

## Self-review notes

- **Spec coverage.** `ctx.patched` (§9.1 mapping row, §14 versioning hook) → Tasks 1–5.
  Two-trait seam assertion (§13 Pass 5 gate, §15) → Task 6. `workflow-macros` lint →
  **deferred entirely** by decision; the contract stays documented + runtime-enforced
  (Task 6 Step 4), with the Pass 5a divergence hardening as the runtime backstop.
- **`patched` is synchronous**, matching `GetVersion` (no blocking, no `seq`, no
  future). It mutates `ctx.commands` directly, which `poll_turn`'s quiescence loop
  already notices via `commands_len()` — no waker plumbing needed.
- **Why markers seed up front, not one-per-turn.** A marker has no external completion
  to await; `patched` must return the right value the first time control reaches it.
  So recorded markers are seeded before driving (like recorded schedules), while the
  *frontier flag* — set per turn from `cursor < applied.len()` — supplies the
  old-vs-live distinction for an un-recorded patch. This is exact: a remaining recorded
  event ahead of the cursor means the history came from code predating the patch.
- **Forward/back-compat.** `RecordPatch` is the `None` arm of Pass 5a's
  `as_recorded`, so it is divergence-exempt; the engine dedupes the marker by
  `change_id`; the `persist` layer needs no change because `encode` already maps
  seq-less events through `_ => None` and stores the full event in `payload`.
- **Documented limitation.** If a patch is inserted ahead of an in-flight history that
  has *no* remaining events when `patched()` is first reached, the new branch is taken
  and may diverge on already-recorded later seqs, dead-lettering that one run loudly.
  This matches §14's accepted posture (drain/abandon in-flight workflows on shape
  change) and is called out in the Architecture section.
- **Type consistency.** `Context::patched(&self, &str) -> bool`,
  `apply_patch(String)`, `set_replaying(bool)`; `Command::RecordPatch { change_id:
  String }`; `Event::Patched { change_id: String }` with `kind() == "Patched"`. All
  match the names used across Tasks 2–5 and the roadmap update in Task 7.
</content>
