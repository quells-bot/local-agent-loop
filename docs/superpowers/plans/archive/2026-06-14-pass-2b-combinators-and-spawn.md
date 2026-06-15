# Pass 2b — Combinators + Spawn Scheduler — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the real concurrency model — `ctx.spawn` plus its ordered turn
scheduler — and prove the deterministic combinators (`join!`, `try_join!`,
`select_biased!`) replay identically across cold recovery, with losing branches
behaving per spec §4.3. Document the banned-combinator contract (spec §4.2, §4.4).

**Architecture:** `join!` / `try_join!` / `select_biased!` already work with the
Pass 1 driver: they live inside the `main` future, the driver drains every command
a turn emits, and `cold_replay` applies one result per turn — so most of Pass 2b is
*acceptance tests that lock in this behavior*. The one genuinely new mechanism is
`ctx.spawn`: detached branches that nothing awaits inline must still be polled every
turn. `WorkflowState` gains an ordered `spawned` vec; `poll_turn` drives `main` and
every live spawned task to **quiescence** each turn (re-polling as freshly-spawned
tasks appear and as handles resolve), without applying any new event — so the
one-event-per-turn invariant is untouched and seq allocation stays deterministic.
A `SpawnHandle<T>` resolves when its task completes, via a shared `Rc<RefCell<Option<T>>>`
slot. No engine or persist changes are needed.

**Tech Stack:** Rust 2021, the `futures` crate (combinator macros), tokio, serde_json.

**Depends on:** Pass 2a (timers) — the `select_biased!` acceptance test races an
activity against `ctx.sleep`, and `pump` drives the timer service.

---

## Canonical type additions (update the ROADMAP "Canonical types" list)

```rust
// crate `workflow`
pub struct SpawnHandle<T> { /* Rc<RefCell<Option<T>>> */ }  // NEW (Pass 2b)
impl<T> Future for SpawnHandle<T> { type Output = T; }       // resolves when the task completes

// workflow::Context gains:
//   fn spawn<F, T>(&self, fut: F) -> SpawnHandle<T>
//       where F: Future<Output = T> + 'static, T: 'static     // workflow.Go analog (§4.4)
//   fn commands_len(&self) -> usize                            // turn-quiescence helper
//   (internal) fn drain_new_spawns(&self) -> Vec<Pin<Box<dyn Future<Output = ()>>>>
//
// ContextInner gains:  new_spawns: RefCell<Vec<Pin<Box<dyn Future<Output = ()>>>>>
// WorkflowState gains:  spawned: Vec<Option<Pin<Box<dyn Future<Output = ()>>>>>
//   and poll_turn drives main + spawned to quiescence per turn.
```

No changes to `Command`, `Event`, `CommandResult`, `engine`, or `persist`.

---

## File Structure

```
/crates/workflow/src/spawn.rs       # NEW: SpawnHandle<T>
/crates/workflow/src/context.rs      # MODIFY: new_spawns field, spawn(), drain_new_spawns(), commands_len()
/crates/workflow/src/state.rs        # MODIFY: spawned vec + quiescence poll_turn
/crates/workflow/src/lib.rs          # MODIFY: export SpawnHandle
/crates/engine/Cargo.toml            # MODIFY: add `futures` dev-dependency
/crates/engine/tests/concurrency.rs  # NEW: Pass-2b acceptance (join!, select_biased!, spawn + cold recovery)
```

---

### Task 1: `ctx.spawn` + ordered quiescence scheduler

**Files:**
- Create: `crates/workflow/src/spawn.rs`
- Modify: `crates/workflow/src/context.rs`, `state.rs`, `lib.rs`

- [ ] **Step 1: Write the failing pure replay test for `spawn`**

Append to `crates/workflow/src/replay.rs` `mod tests`:

```rust
    // Workflow that spawns a detached branch running one activity, then awaits it.
    // Exercises the ordered scheduler: the spawned task is polled though `main`
    // never polls it directly.
    struct Detached;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Detached {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Detached";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let ctx2 = ctx.clone();
            let h = ctx.spawn(async move { ctx2.activity::<Add>((3, 4)).await.unwrap() });
            let v = h.await;
            Ok(v)
        }
    }

    #[test]
    fn replays_spawned_branch() {
        let info = Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "Detached".into(),
        };
        let h = vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            // The spawned branch's activity is the first (and only) seq allocated.
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(3, 4),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted { seq: 0, output: serde_json::to_vec(&7i64).unwrap() },
        ];
        let outcome = cold_replay::<Detached>(info, &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 7);
        assert_eq!(outcome.commands.len(), 1);
        assert!(matches!(&outcome.commands[0], Command::ScheduleActivity { seq: 0, .. }));
    }
```

Run: `cargo test -p workflow --lib replay`
Expected: FAIL — `ctx.spawn` does not exist (compile error).

- [ ] **Step 2: Create `SpawnHandle`**

Create `crates/workflow/src/spawn.rs`:

```rust
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

/// Awaitable handle for a detached `ctx.spawn` branch (the `workflow.Go` analog,
/// spec §4.4). Resolves to the branch's output once it completes. The branch writes
/// its result into the shared slot; the handle takes it.
pub struct SpawnHandle<T> {
    pub(crate) slot: Rc<RefCell<Option<T>>>,
}

impl<T> Future for SpawnHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<T> {
        match self.slot.borrow_mut().take() {
            Some(v) => Poll::Ready(v),
            None => Poll::Pending,
        }
    }
}
```

- [ ] **Step 3: Add `new_spawns`, `spawn`, `drain_new_spawns`, `commands_len` to `Context`**

In `crates/workflow/src/context.rs`:

(a) Add imports at the top (the file already imports `Cell`, `RefCell`, `HashMap`,
`HashSet`, `Rc`):

```rust
use std::future::Future;
use std::pin::Pin;
```

(b) Add the staging field to `ContextInner` (after `fired`, added in Pass 2a):

```rust
    pub(crate) fired: RefCell<HashSet<u64>>,                   // timer seqs fired (no payload)
    // Futures spawned this turn, awaiting absorption by WorkflowState (spec §4.4).
    pub(crate) new_spawns: RefCell<Vec<Pin<Box<dyn Future<Output = ()>>>>>,
```

(c) Initialize it in `Context::new` (after `fired: RefCell::new(HashSet::new()),`):

```rust
                fired: RefCell::new(HashSet::new()),
                new_spawns: RefCell::new(Vec::new()),
```

(d) Add the methods to `impl Context` (after `apply_timer_fired`):

```rust
    /// Spawn a detached branch (the `workflow.Go` analog, spec §4.4). The branch is
    /// polled every turn in creation order by `WorkflowState`; it allocates `seq`s
    /// from the shared counter exactly like inline code, so replay is deterministic.
    /// Returns an awaitable handle for its output. Allocates no command and no `seq`
    /// for the spawn itself.
    pub fn spawn<F, T>(&self, fut: F) -> crate::SpawnHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let slot = std::rc::Rc::new(RefCell::new(None));
        let writer = slot.clone();
        let wrapped: Pin<Box<dyn Future<Output = ()>>> = Box::pin(async move {
            let v = fut.await;
            *writer.borrow_mut() = Some(v);
        });
        self.inner.new_spawns.borrow_mut().push(wrapped);
        crate::SpawnHandle { slot }
    }

    /// WorkflowState drains freshly-spawned futures into its ordered poll list.
    pub(crate) fn drain_new_spawns(&self) -> Vec<Pin<Box<dyn Future<Output = ()>>>> {
        self.inner.new_spawns.borrow_mut().drain(..).collect()
    }

    /// Number of commands buffered (not drained). Used by `poll_turn` to detect that
    /// a future made progress (emitted a command) during a quiescence iteration.
    pub fn commands_len(&self) -> usize {
        self.inner.commands.borrow().len()
    }
```

- [ ] **Step 4: Give `WorkflowState` the ordered scheduler**

In `crates/workflow/src/state.rs`:

(a) Add the field to the struct:

```rust
pub struct WorkflowState {
    ctx: Context,
    // Output is the JSON-encoded workflow result. !Send by construction.
    main: Pin<Box<dyn Future<Output = Result<Vec<u8>, crate::Error>>>>,
    // Detached spawned branches, polled every turn in creation order (spec §4.4).
    // `None` once a branch has completed (a completed future must not be re-polled).
    spawned: Vec<Option<Pin<Box<dyn Future<Output = ()>>>>>,
}
```

(b) Initialize `spawned` in `start`:

```rust
        Self { ctx, main, spawned: Vec::new() }
```

(c) Replace `poll_turn` with the quiescence loop:

```rust
    /// Drive `main` and every live spawned branch to quiescence for this turn:
    /// re-poll until nothing makes progress (no Ready transition, no new command, no
    /// new spawn). No new event is applied here — the caller has already applied at
    /// most one (spec §4.1) — so determinism is preserved while detached branches and
    /// resolved `SpawnHandle`s still get observed within the same turn.
    pub fn poll_turn(&mut self) -> Poll<Result<Vec<u8>, crate::Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        loop {
            let mut progressed = false;
            let before_cmds = self.ctx.commands_len();

            if let Poll::Ready(r) = self.main.as_mut().poll(&mut tcx) {
                return Poll::Ready(r);
            }

            // Absorb spawns created by `main` this iteration (creation order).
            for fut in self.ctx.drain_new_spawns() {
                self.spawned.push(Some(fut));
                progressed = true;
            }

            // Poll each live spawned branch once.
            for slot in self.spawned.iter_mut() {
                if let Some(fut) = slot.as_mut() {
                    if fut.as_mut().poll(&mut tcx).is_ready() {
                        *slot = None; // completed — do not poll again
                        progressed = true;
                    }
                }
            }

            // Absorb spawns created by spawned branches this iteration.
            for fut in self.ctx.drain_new_spawns() {
                self.spawned.push(Some(fut));
                progressed = true;
            }

            if self.ctx.commands_len() != before_cmds {
                progressed = true;
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }
```

- [ ] **Step 5: Export `SpawnHandle`**

In `crates/workflow/src/lib.rs`, add:

```rust
mod spawn;
pub use spawn::SpawnHandle;
```

(Place it near the other `mod`/`pub use` lines, e.g. after the `future` exports.)

- [ ] **Step 6: Run the workflow-crate tests**

Run: `cargo test -p workflow`
Expected: PASS — the new `replays_spawned_branch`, plus every Pass 1 and Pass 2a
test still green (the quiescence loop is behaviorally identical for spawn-free
workflows: it polls `main` to a fixpoint and drains the same commands).

- [ ] **Step 7: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): ctx.spawn + SpawnHandle + ordered quiescence scheduler (spec §4.4)"
```

---

### Task 2: Pass-2b acceptance — combinators + spawn across cold recovery

**Files:**
- Modify: `crates/engine/Cargo.toml`
- Create: `crates/engine/tests/concurrency.rs`

- [ ] **Step 1: Add `futures` as an engine dev-dependency**

In `crates/engine/Cargo.toml`, under `[dev-dependencies]`:

```toml
[dev-dependencies]
persist = { path = "../persist" }
futures = { workspace = true }
```

- [ ] **Step 2: Write the integration tests**

Create `crates/engine/tests/concurrency.rs`:

```rust
use std::sync::Arc;
use std::time::Duration;

use engine::{Engine, ExecStatus, History, StartOptions};
use futures::{select_biased, FutureExt};
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

// join!: two concurrent activity branches, summed. First poll emits BOTH schedule
// commands in one turn; completions are applied one-per-turn (spec §4.1).
struct Pair;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Pair {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Pair";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let (a, b) = futures::join!(
            ctx.activity::<Add>((1, 2)),
            ctx.activity::<Add>((10, 20)),
        );
        Ok(a? + b?)
    }
}

// select_biased!: an activity races a one-day sleep. The activity always wins (the
// timer never reaches its fire_at in the test), and the losing timer branch is just
// dropped — its `timers` row is never consumed (spec §4.3).
struct Pick;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Pick {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Pick";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let act = ctx.activity::<Add>((7, 8)).fuse();
        let nap = ctx.sleep(Duration::from_secs(86_400)).fuse();
        futures::pin_mut!(act, nap);
        let r = select_biased! {
            x = act => x?,
            _ = nap => -1,
        };
        Ok(r)
    }
}

// ctx.spawn: a detached branch runs an activity; main awaits its handle.
struct Detached;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Detached {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Detached";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let ctx2 = ctx.clone();
        let h = ctx.spawn(async move { ctx2.activity::<Add>((3, 4)).await.unwrap() });
        let v = h.await;
        Ok(v)
    }
}

fn build<W: workflow::Definition>(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<W>();
    e.register_activity::<Add>();
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

#[tokio::test]
async fn join_runs_concurrent_branches() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<Pair>(&db);
    let handle = engine
        .start_workflow::<Pair>((), StartOptions { id: "pair-1".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 3 + 30);
}

#[tokio::test]
async fn join_branches_replay_across_cold_recovery() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: start, drive one turn (schedules BOTH activities), run one activity,
    // then crash with the other still pending.
    {
        let engine = build::<Pair>(&db);
        engine
            .start_workflow::<Pair>((), StartOptions { id: "pair-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // emits two ScheduleActivity
        assert!(engine.process_one_activity().await.unwrap()); // completes one branch
    }
    // Phase 2: cold-recover and finish.
    let engine2 = build::<Pair>(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("pair-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 33);
}

#[tokio::test]
async fn select_biased_activity_beats_timer() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<Pick>(&db);
    let handle = engine
        .start_workflow::<Pick>((), StartOptions { id: "pick-1".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 15, "the activity branch wins; the day-long timer never fires");
}

#[tokio::test]
async fn select_biased_replays_across_cold_recovery() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: start, drive one turn (schedules the activity AND the timer), run the
    // activity, then crash before the workflow is finally driven.
    {
        let engine = build::<Pick>(&db);
        engine
            .start_workflow::<Pick>((), StartOptions { id: "pick-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // ScheduleActivity seq0 + StartTimer seq1
        assert!(engine.process_one_activity().await.unwrap()); // completes the activity
    }
    // Phase 2: cold-recover — the activity result is in history; the biased select
    // re-resolves to the same winner.
    let engine2 = build::<Pick>(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("pick-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 15);
}

#[tokio::test]
async fn spawn_detached_branch_completes() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<Detached>(&db);
    let handle = engine
        .start_workflow::<Detached>((), StartOptions { id: "spawn-1".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 7);
}

#[tokio::test]
async fn spawn_replays_across_cold_recovery() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: start, drive one turn (the spawned branch schedules its activity),
    // run the activity, then crash before main observes the handle.
    {
        let engine = build::<Detached>(&db);
        engine
            .start_workflow::<Detached>((), StartOptions { id: "spawn-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // spawned branch emits ScheduleActivity seq0
        assert!(engine.process_one_activity().await.unwrap()); // completes it
    }
    // Phase 2: cold-recover; the spawned branch is re-created, polled, resolves, and
    // main's `h.await` completes.
    let engine2 = build::<Detached>(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("spawn-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 7);
}
```

- [ ] **Step 3: Run the acceptance tests**

Run: `cargo test -p engine --test concurrency`
Expected: all six PASS:
- `join_runs_concurrent_branches`
- `join_branches_replay_across_cold_recovery`
- `select_biased_activity_beats_timer`
- `select_biased_replays_across_cold_recovery`
- `spawn_detached_branch_completes`
- `spawn_replays_across_cold_recovery`

- [ ] **Step 4: Commit**

```bash
git add crates/engine
git commit -m "test(engine): pass 2b acceptance — join!, select_biased!, spawn across cold recovery"
```

---

### Task 3: Banned-combinator docs + whole-workspace green + roadmap

**Files:**
- Modify: `crates/workflow/src/lib.rs` (doc), `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`

- [ ] **Step 1: Document the banned-combinator contract**

Add a module-level doc block to the top of `crates/workflow/src/lib.rs` (after the
existing `//!` line), so the contract lives next to the API it constrains (spec §4.2;
the lint enforcement is deferred to Pass 5's `workflow-macros`):

```rust
//! Workflow-authoring surface + replay protocol (mirrors Go SDK `workflow`).
//!
//! ## Deterministic concurrency contract (spec §4.2)
//!
//! Workflow code may use only combinators whose poll/branch order is deterministic,
//! so that with the one-event-per-turn rule (spec §4.1) they replay identically:
//!
//! - **Allowed:** [`futures::join`], [`futures::try_join`], ordered `join_all`, and
//!   [`futures::select_biased`] (the `workflow.Selector` analog — deterministic by
//!   registration order). Spawn detached branches with [`Context::spawn`].
//! - **Banned (non-deterministic):** `futures::select!` (randomizes branch order)
//!   and bare `FuturesUnordered` (reorders by wakeup/wall-clock order). Using either
//!   breaks replay.
//!
//! These bans are a documented contract today; a `#[workflow]` macro / clippy lint
//! (the `workflow-macros` crate) is deferred to Pass 5.
```

Run: `cargo build -p workflow`
Expected: compiles (doc-only change; intra-doc links resolve).

- [ ] **Step 2:** Run `cargo test` — every crate's tests PASS (Pass 1, 2a, and 2b).
- [ ] **Step 3:** Run `cargo clippy --all-targets -- -D warnings` — clean.
- [ ] **Step 4:** In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
  - Mark chunk `2b` status `done` and set its Plan file to
    `2026-06-14-pass-2b-combinators-and-spawn.md`.
  - Update the "Canonical types" section: add `SpawnHandle<T>`, `Context::spawn`,
    and a one-line note that `WorkflowState` polls spawned branches to quiescence
    per turn (mirror the "Canonical type additions" block above).
- [ ] **Step 5:** Commit:

```bash
git add -A
git commit -m "chore: pass 2b complete — banned-combinator docs, clippy clean, roadmap updated"
```

---

## Notes / decisions

- **Why `join!` / `select_biased!` need no engine work:** they are ordinary futures
  inside `main`. The driver already drains all commands a turn emits (the join's two
  `ScheduleActivity`s land together) and `cold_replay` already applies one result per
  turn. The acceptance tests *lock in* this behavior across cold recovery; only
  `ctx.spawn` adds machinery.
- **`select_biased!` winner is the first-completing branch, not purely registration
  order.** Under one-event-per-turn only one branch can become ready on a given poll,
  so the first completion in `history.event_id` order wins; registration order is the
  tie-break that one-event-per-turn never needs. Either way the winner is a pure
  function of recorded history, so replay is deterministic — which is exactly the
  Pass 2 acceptance requirement. The test pins a deterministic winner by racing
  against a day-long timer that never fires.
- **`ctx.now()` / `ctx.random()`** remain deferred (spec §9). Add them as recorded
  marker events (like Pass 2a's `TimerStarted`) when a workflow first needs them.
- **Hardening still deferred to Pass 5** (tracked in the `pass2-hardening-backlog`
  memory; the observer double-fire guard was already folded into Pass 2a): activity
  lease-expiry/heartbeat, unregistered-workflow poison-spin dead-lettering, and
  notify/channel wakeups in place of busy-poll loops. None gate Pass 2 acceptance.
- **Spawn error handling:** the `Detached` test body `.unwrap()`s the activity result
  for brevity. Real spawned branches should return a `Result` through the handle and
  let `main` decide; spawn is generic over the branch output type `T`.

## Self-Review (completed during authoring)

- **Spec coverage:** §4.2 (allowed/banned combinators — documented + exercised by
  `join!`/`select_biased!` tests), §4.3 (losing `select_biased!` branch dropped, its
  timer row unconsumed — `Pick`), §4.4 (`ctx.spawn` + ordered per-turn scheduler —
  `Context::spawn`, `WorkflowState.spawned`, quiescence `poll_turn`), §13 Pass 2
  acceptance gate (concurrent branches + `select_biased!` race replay across cold
  recovery — the six `concurrency.rs` tests).
- **Placeholders:** none — full code, exact commands, expected outcomes throughout.
- **Type consistency:** `SpawnHandle<T>` (field `slot: Rc<RefCell<Option<T>>>`),
  `Context::spawn<F,T>(…) -> SpawnHandle<T>`, `Context::drain_new_spawns`,
  `Context::commands_len`, `ContextInner.new_spawns`, `WorkflowState.spawned:
  Vec<Option<Pin<Box<dyn Future<Output=()>>>>>` used identically across `spawn.rs`,
  `context.rs`, and `state.rs`. Reuses Pass 2a's `ctx.sleep` and `process_one_timer`
  in the `select_biased!` test. No `Command`/`Event`/`engine`/`persist` changes.
```
