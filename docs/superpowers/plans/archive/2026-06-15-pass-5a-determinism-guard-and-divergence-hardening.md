# Pass 5a — Determinism guard + divergence-check hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the determinism contract (spec §12) a standing, CI-enforced guard and
close the one real hole in the replay divergence check — a command emitted at a `seq`
whose recorded history is a *different kind* of command (activity-vs-timer-vs-child)
is currently not detected (Invariant 9).

**Architecture:** This codebase has **no sticky cache** — the driver cold-replays the
full history from SQLite every turn (`engine::Engine::process_one_runnable` →
`workflow::cold_replay`). That is correct and, at desktop scale, cheap (a `SELECT`
over a few hundred history rows), so per the project decision we **punt on the live
cache indefinitely**. The spec's "cache vs cold-replay equivalence" guard (§14) is
therefore reframed against the only path we have: **replay determinism**.

Two guards plus one hardening:

1. **Divergence hardening (Invariant 9):** `cold_replay` builds three *separate*
   `seq`-keyed maps (`recorded_sched` / `recorded_timer` / `recorded_child`), so when
   the workflow emits e.g. `ScheduleActivity { seq: 0 }` it only consults
   `recorded_sched`. If history recorded a `TimerStarted { seq: 0 }` at that `seq`,
   the activity map has no entry and **no divergence is raised** — a silent
   nondeterminism. Fix: one `seq`-keyed map carrying the recorded command's *kind +
   payload*, compared structurally against each emitted command.
2. **Prefix-replay stability guard (pure, `workflow` crate):** replaying every prefix
   of a history must produce a command stream that only *grows* — earlier decisions
   are never rewritten. This is the spec's "force-evict at every point" guard
   expressed without a cache: evicting and reloading at any history position
   reproduces the same prior commands. `cold_replay` is pure (spec §4.4), so this
   lives as a `workflow`-crate test.
3. **Live-vs-replay equivalence guard (engine integration, the CI standing guard):**
   drive representative workflows to completion through the real engine, read the
   persisted history back, and assert `cold_replay` of that history (a) returns `Ok`
   (no nondeterminism), (b) completes with the same result the engine stored, and (c)
   is **idempotent** — two `cold_replay` calls return equal `ReplayOutcome`s.
   `ReplayOutcome` already derives `PartialEq, Eq`, so equality is a direct assert.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow, thiserror, futures,
uuid. No new crates, no new dependencies.

**Depends on:** Pass 4a (merged). Touches only `crates/workflow/src/replay.rs` and a
new `crates/engine/tests/equivalence.rs`.

---

## File structure

- Modify: `crates/workflow/src/replay.rs` — unify the divergence maps + kind-mismatch
  detection (Task 1); add the prefix-stability test (Task 2).
- Create: `crates/engine/tests/equivalence.rs` — the live-vs-replay CI guard (Task 3).
- Modify: `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md` —
  flip the 5a row to `done` (Task 4).

No public API changes. `cold_replay`, `ReplayOutcome`, and `Nondeterminism` keep their
current signatures.

---

### Task 1: Detect command-*kind* divergence at a recorded `seq`

**Files:**
- Modify: `crates/workflow/src/replay.rs` (the `cold_replay` fn body, roughly lines
  47–161, and add two tests to the existing `#[cfg(test)] mod tests`).

**Background — what changes.** Inside `cold_replay`, replace the three recorded maps
with a single `seq → RecordedCmd` map and a structural comparison. The
one-event-per-turn `applied` stream (`Applied::Result` / `Timer` / `Signal` / `Child`)
is **unchanged** — only the divergence-check side is touched.

- [ ] **Step 1: Write the failing tests**

Add these two tests to `mod tests` in `crates/workflow/src/replay.rs` (they reuse the
existing `Sum` / `Add` / `info()` / `add_input` and `Parent` / `parent_info()` test
fixtures already defined in that module):

```rust
    #[test]
    fn detects_kind_divergence_activity_recorded_timer_in_history() {
        // History recorded a TIMER at seq 0, but Sum emits an ACTIVITY at seq 0.
        // Pre-hardening this was silent (the activity map had no seq-0 entry).
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::TimerStarted {
                seq: 0,
                duration_ms: 500,
            },
        ];
        let err = cold_replay::<Sum>(info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(
            err.detail.contains("timer") && err.detail.contains("activity"),
            "detail should name both the recorded kind and the emitted kind, got: {}",
            err.detail
        );
    }

    #[test]
    fn detects_kind_divergence_child_emitted_activity_recorded() {
        // History recorded an ACTIVITY at seq 0, but Parent emits a CHILD at seq 0.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
        ];
        let err = cold_replay::<Parent>(parent_info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(
            err.detail.contains("activity") && err.detail.contains("child"),
            "detail should name both the recorded kind and the emitted kind, got: {}",
            err.detail
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p workflow detects_kind_divergence`
Expected: both FAIL. Pre-hardening, `cold_replay` returns `Ok(..)` (no divergence
raised) because the wrong-kind `seq` is absent from the consulted map, so
`.unwrap_err()` panics with "called `Result::unwrap_err()` on an `Ok` value".

- [ ] **Step 3: Unify the recorded-command maps**

In `cold_replay`, **delete** these three declarations (currently around lines 57–59):

```rust
    let mut recorded_sched: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
    let mut recorded_timer: HashMap<u64, u64> = HashMap::new(); // seq -> duration_ms
    let mut recorded_child: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
```

and **replace** them with a single map plus a local kind-tagged type. Place the
`RecordedCmd` enum, its `describe`, and the `as_recorded` helper just above the
indexing loop (local items inside the fn are fine):

```rust
    // One recorded command per seq, carrying its kind + payload, so the divergence
    // check (Invariant 9) catches a *kind* mismatch (activity-vs-timer-vs-child at the
    // same seq), not just a same-kind payload mismatch.
    #[derive(PartialEq, Eq)]
    enum RecordedCmd {
        Activity { activity_type: String, input: Vec<u8> },
        Timer { duration_ms: u64 },
        Child { workflow_type: String, input: Vec<u8> },
    }
    impl RecordedCmd {
        fn describe(&self) -> String {
            match self {
                RecordedCmd::Activity { activity_type, .. } => format!("activity {activity_type}"),
                RecordedCmd::Timer { duration_ms } => format!("timer {duration_ms}ms"),
                RecordedCmd::Child { workflow_type, .. } => format!("child {workflow_type}"),
            }
        }
    }
    // Map an emitted command to (seq, RecordedCmd) for comparison. Returns None for
    // commands that carry no seq and are divergence-exempt (none today; pass 5b's
    // RecordPatch will add a None arm here).
    fn as_recorded(cmd: &Command) -> Option<(u64, RecordedCmd)> {
        match cmd {
            Command::ScheduleActivity {
                seq,
                activity_type,
                input,
                ..
            } => Some((
                *seq,
                RecordedCmd::Activity {
                    activity_type: activity_type.clone(),
                    input: input.clone(),
                },
            )),
            Command::StartTimer { seq, duration_ms } => {
                Some((*seq, RecordedCmd::Timer { duration_ms: *duration_ms }))
            }
            Command::StartChild {
                seq,
                workflow_type,
                input,
            } => Some((
                *seq,
                RecordedCmd::Child {
                    workflow_type: workflow_type.clone(),
                    input: input.clone(),
                },
            )),
        }
    }

    let mut recorded_cmd: HashMap<u64, RecordedCmd> = HashMap::new();
```

- [ ] **Step 4: Populate the unified map in the history scan**

In the `for ev in history` indexing loop, replace the three `recorded_*` inserts with
inserts into `recorded_cmd`. The `ActivityScheduled`, `TimerStarted`, and
`ChildScheduled` arms become:

```rust
            Event::ActivityScheduled {
                seq,
                activity_type,
                input,
                ..
            } => {
                recorded_cmd.insert(
                    *seq,
                    RecordedCmd::Activity {
                        activity_type: activity_type.clone(),
                        input: input.clone(),
                    },
                );
            }
            // ... ActivityCompleted / ActivityFailed arms UNCHANGED (push Applied::Result) ...
            Event::TimerStarted { seq, duration_ms } => {
                recorded_cmd.insert(*seq, RecordedCmd::Timer { duration_ms: *duration_ms });
            }
            // ... TimerFired arm UNCHANGED (push Applied::Timer) ...
            Event::ChildScheduled {
                seq,
                workflow_type,
                input,
            } => {
                recorded_cmd.insert(
                    *seq,
                    RecordedCmd::Child {
                        workflow_type: workflow_type.clone(),
                        input: input.clone(),
                    },
                );
            }
            // ... ChildCompleted arm UNCHANGED (push Applied::Child) ...
```

Leave the `ActivityCompleted`, `ActivityFailed`, `TimerFired`, `WorkflowStarted`,
`SignalReceived`, and `ChildCompleted` arms exactly as they are.

- [ ] **Step 5: Replace the per-command divergence check in the drive loop**

In the drive loop, replace the whole `for cmd in state.drain_commands() { match &cmd
{ ... } commands.push(cmd); }` block (currently lines ~112–161) with the unified
check:

```rust
        for cmd in state.drain_commands() {
            if let Some((seq, emitted)) = as_recorded(&cmd) {
                if let Some(rec) = recorded_cmd.get(&seq) {
                    if *rec != emitted {
                        return Err(Nondeterminism {
                            seq,
                            detail: format!(
                                "history recorded {} at seq {seq}, workflow emitted {}",
                                rec.describe(),
                                emitted.describe()
                            ),
                        });
                    }
                }
            }
            commands.push(cmd);
        }
```

This single comparison subsumes the old type/input/duration checks **and** adds the
kind mismatch: a wrong-kind emit no longer matches the recorded variant, so
`*rec != emitted` fires with a detail naming both kinds.

- [ ] **Step 6: Run the new tests — verify they pass**

Run: `cargo test -p workflow detects_kind_divergence`
Expected: both PASS.

- [ ] **Step 7: Run the full replay suite — verify the existing checks still pass**

Run: `cargo test -p workflow replay`
Expected: PASS, including the pre-existing `detects_divergent_activity_type`
(detail still contains "Charge"), `detects_divergent_activity_input` (asserts `.seq`
only), `detects_divergent_timer_duration` (detail still contains "timer"), and
`detects_divergent_child_type` (detail still contains "Other"). The new `describe()`
strings preserve those substrings by construction.

- [ ] **Step 8: Commit**

```bash
git add crates/workflow/src/replay.rs
git commit -m "fix(workflow): detect command-kind divergence at a recorded seq (Inv 9)"
```

---

### Task 2: Prefix-replay stability guard (pure)

**Files:**
- Modify: `crates/workflow/src/replay.rs` (add one test to `mod tests`).

This is the spec §14 "force-evict at every point" guard, expressed without a cache:
replaying any prefix of a history reproduces a command stream that only *extends* the
shorter prefix's stream — earlier decisions are never rewritten.

- [ ] **Step 1: Write the failing test**

Add to `mod tests` in `crates/workflow/src/replay.rs`. It reuses the existing
`Sum` / `info()` / `full_history()` and `Nap` / `nap_info()` fixtures:

```rust
    /// Replaying every prefix of a history yields a command stream that only GROWS:
    /// the stream for prefix k+1 starts with the stream for prefix k. This is the
    /// spec §14 "force-evict at every point" determinism guard without a live cache —
    /// evicting and cold-replaying at any history position reproduces prior commands.
    fn assert_prefix_stable<W: crate::Definition>(info: Info, full: &[Event]) {
        let mut prev: Vec<Command> = Vec::new();
        for k in 1..=full.len() {
            let outcome = cold_replay::<W>(info.clone(), &full[..k])
                .unwrap_or_else(|e| panic!("prefix {k} diverged: {e}"));
            assert!(
                outcome.commands.len() >= prev.len(),
                "prefix {k}: command stream shrank ({} < {})",
                outcome.commands.len(),
                prev.len()
            );
            assert!(
                outcome.commands.starts_with(&prev),
                "prefix {k}: command stream rewrote an earlier command"
            );
            prev = outcome.commands;
        }
    }

    #[test]
    fn prefix_replay_is_stable_activities() {
        assert_prefix_stable::<Sum>(info(), &full_history());
    }

    #[test]
    fn prefix_replay_is_stable_timer_then_activity() {
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::TimerStarted {
                seq: 0,
                duration_ms: 500,
            },
            Event::TimerFired { seq: 0 },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 1,
                output: serde_json::to_vec(&3i64).unwrap(),
            },
        ];
        assert_prefix_stable::<Nap>(nap_info(), &h);
    }
```

Note: `Info` derives `Clone` (roadmap "Canonical types"), so `info.clone()` per
iteration is valid.

- [ ] **Step 2: Run the test — verify it passes**

Run: `cargo test -p workflow prefix_replay_is_stable`
Expected: PASS. (This guard should already hold against the engine's determinism; the
test exists to *lock it in* against future regressions.)

- [ ] **Step 3: Sanity-check the guard actually bites**

Temporarily corrupt one prefix to prove the assertion fires. In
`assert_prefix_stable`, change `&full[..k]` to `&full[..k.min(full.len().saturating_sub(1)).max(1)]`
... — actually simpler: temporarily change the `starts_with(&prev)` assert to
`starts_with(&prev) && k != 3` and run; confirm prefix 3 now "passes" wrongly only
because of the hack, then revert. Skip if confident; the point is to confirm the
assertion is not vacuous. Revert any temporary edit before committing.

- [ ] **Step 4: Commit**

```bash
git add crates/workflow/src/replay.rs
git commit -m "test(workflow): standing prefix-replay stability guard (spec §14)"
```

---

### Task 3: Live-vs-replay equivalence guard (engine integration, CI standing guard)

**Files:**
- Create: `crates/engine/tests/equivalence.rs`

This is the spec §13 Pass 5 acceptance gate — "the cache/cold-replay equivalence test
passes as a CI guard" — realized against the cold-replay-only engine: a workflow that
*actually ran* through the engine, replayed from its persisted history, reproduces the
same decisions and result.

- [ ] **Step 1: Write the failing test file**

Create `crates/engine/tests/equivalence.rs`. It mirrors the harness in
`crates/engine/tests/end_to_end.rs` (the `build` / `pump` helpers and the `Add` /
`Sum` definitions) and adds a timer workflow so the guard covers mixed event kinds:

```rust
use std::sync::Arc;
use std::time::Duration;

use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
use persist::Sqlite;
use workflow::{cold_replay, Event, Execution, Info, ReplayOutcome};

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

// Activity-only workflow: Add(Add(1, 2), 10) == 13.
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

// Timer + activity: exercises TimerStarted/TimerFired interleaved with an activity.
struct Nap;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Nap {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Nap";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        ctx.sleep(Duration::from_millis(1)).await;
        let a = ctx.activity::<Add>((4, 5)).await?;
        Ok(a)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Sum>();
    e.register_workflow::<Nap>();
    e.register_activity::<Add>();
    e
}

/// Pump driver + worker + timer turns until quiescent (deterministic; no background
/// loops). Mirrors end_to_end.rs but also fires due timers so Nap can finish.
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

/// The standing CI guard: a workflow that ran to completion through the engine, when
/// cold-replayed from its PERSISTED history, must (1) not diverge, (2) complete with
/// the same result the engine stored, and (3) replay idempotently (spec §12, §14).
async fn assert_live_matches_replay<W: workflow::Definition>(
    db: &Sqlite,
    workflow_type: &str,
    workflow_id: &str,
) {
    let (run_id, status, stored_result) =
        db.find_execution(workflow_id).await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed, "{workflow_id} should complete");
    let stored_result = stored_result.expect("completed run has a result");

    let events: Vec<Event> = db
        .read_history(&run_id)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.event)
        .collect();

    let info = Info {
        execution: Execution {
            workflow_id: workflow_id.to_string(),
            run_id: run_id.clone(),
        },
        parent: None,
        workflow_type: workflow_type.to_string(),
    };

    let first: ReplayOutcome = cold_replay::<W>(info.clone(), &events)
        .expect("cold replay of a real history must not diverge");
    assert_eq!(
        first.completion,
        Some(Ok(stored_result)),
        "replayed completion must equal the engine's stored result"
    );

    let second = cold_replay::<W>(info, &events).unwrap();
    assert_eq!(first, second, "cold replay must be idempotent");
}

#[tokio::test]
async fn activity_workflow_live_matches_cold_replay() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<Sum>((), StartOptions { id: "eq-sum".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    assert_live_matches_replay::<Sum>(&db, "Sum", "eq-sum").await;
}

#[tokio::test]
async fn timer_workflow_live_matches_cold_replay() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<Nap>((), StartOptions { id: "eq-nap".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    assert_live_matches_replay::<Nap>(&db, "Nap", "eq-nap").await;
}
```

- [ ] **Step 2: Run the new integration tests**

Run: `cargo test -p engine --test equivalence`
Expected: both PASS. If `timer_workflow_live_matches_cold_replay` hangs, the `pump`
loop is missing `process_one_timer` — confirm it is present (it differs from the
`end_to_end.rs` pump, which has no timers).

- [ ] **Step 3: Commit**

```bash
git add crates/engine/tests/equivalence.rs
git commit -m "test(engine): live-vs-cold-replay equivalence CI guard (spec §13 pass 5)"
```

---

### Task 4: Verification gate + roadmap status

**Files:**
- Modify: `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`

- [ ] **Step 1: Run the full verification trio**

Run each and confirm clean (the roadmap's per-chunk gate):

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
```

Expected: `cargo test` all green; clippy clean (no warnings — watch for an unused
import if `TaskQueue` is brought in but the alias path differs; the test imports it);
fmt reports no drift.

- [ ] **Step 2: Flip the 5a roadmap row to done**

In the chunk table, change the `5a` row's Status cell from `not yet authored` to
`done` and set its Plan file cell to
`2026-06-15-pass-5a-determinism-guard-and-divergence-hardening.md`.

- [ ] **Step 3: Commit**

```bash
git add docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md
git commit -m "docs(roadmap): mark pass 5a done"
```

---

## Self-review notes

- **Spec coverage.** §12 Invariant 9 hardening → Task 1. §14 force-evict guard →
  Task 2. §13 Pass 5 acceptance ("equivalence test as a CI guard") → Task 3. The
  second half of the Pass 5 gate ("the two traits compile as the only seam `persist`
  implements") is handled in **Pass 5b**, not here.
- **No cache, by decision.** The project chose to punt on the live sticky cache
  indefinitely (cold-replay over SQLite is cheap), so this plan adds *no* engine
  cache and changes no driver behavior — only guards and the divergence check.
- **Forward-compat with 5b.** `as_recorded` returns `Option<(u64, RecordedCmd)>` so
  Pass 5b's seq-less `Command::RecordPatch` slots in as a `None` arm with no other
  change to the divergence loop. When 5b adds that variant, the `as_recorded` match
  stops being exhaustive and the compiler forces the `None` arm — intended.
- **Type consistency.** Uses the real signatures: `cold_replay::<W>(Info, &[Event]) ->
  Result<ReplayOutcome, Nondeterminism>`; `ReplayOutcome { commands, completion }`
  with `completion: Option<Result<Vec<u8>, workflow::Error>>` and derived `Eq`;
  `Sqlite::{read_history, find_execution}` via the `History` trait; `Info` derives
  `Clone`.
</content>
</invoke>
