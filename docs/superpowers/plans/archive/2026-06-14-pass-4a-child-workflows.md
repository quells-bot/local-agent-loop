# Pass 4a — Child workflows — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add child workflows — `ctx.child_workflow::<W>(input).await` (the
`workflow.ExecuteChildWorkflow` analog, spec §9.1) — so a parent workflow can start
another registered workflow, block on it, and receive its typed result, with
`workflow::Info.parent` populated for the child and full durability across a crash of
either side (spec §5.4, §9).

**Architecture:** A child workflow is an ordinary `executions` row with
`parent_run_id` / `parent_seq` set (spec §5.4). It reuses the existing replay spine
end-to-end:

- The parent's `ChildFuture` emits a `Command::StartChild { seq, .. }` exactly once
  (like `ScheduleActivity`), then parks.
- The driver records a parent-side echo event `Event::ChildScheduled { seq, .. }`
  (the analog of `ActivityScheduled` / `TimerStarted`) **and** creates the child
  `executions` row + its `WorkflowStarted` + marks it runnable — all inside the
  parent's single decision-turn transaction (Invariant 5). Doing child creation in
  the *same* transaction as the `ChildScheduled` append is what prevents a crash from
  orphaning the child or deadlocking the parent.
- The child runs as a normal runnable workflow. When it reaches a terminal status,
  its terminal `commit_turn` **also** appends `Event::ChildCompleted { seq, result }`
  to the **parent's** history and marks the parent runnable — again in one
  transaction (spec §5.4).
- On the parent's next turn, `ChildCompleted` is applied one-per-turn (Invariant 3),
  resolving the `ChildFuture` to the child's `Result<W::Output, workflow::Error>`.

Children carry **no** new task table and no new queue — `ChildScheduled` /
`ChildCompleted` are `history` rows and the existing `runnable` queue carries the
wake-ups, exactly as signals reuse `runnable` (spec §11). `StartChild` is exempt from
nothing: it allocates a `seq` and is divergence-checked like any command (Invariant
9). `ChildCompleted` carries a `seq` (the parent's command seq) and is applied to a
new per-seq result map, mirroring how activity results resolve `ActivityFuture`.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow, thiserror, futures,
uuid.

**Depends on:** Pass 3 (merged). Builds on the existing `Command`/`Event` enums,
`cold_replay`, `WorkflowState`, the driver's command→event→task translation in
`engine::Engine::process_one_runnable`, and `persist`'s `commit_turn` /
`create_execution` transaction shapes.

---

## Canonical type additions (update the ROADMAP "Canonical types" list)

```rust
// crate `workflow`
pub enum Command {                      // + StartChild (Pass 4)
    ScheduleActivity { /* … */ },
    StartTimer       { /* … */ },
    StartChild { seq: u64, workflow_type: String, input: Vec<u8> },          // NEW
}

pub enum Event {                        // + ChildScheduled / ChildCompleted (Pass 4)
    /* …existing… */
    ChildScheduled { seq: u64, workflow_type: String, input: Vec<u8> },      // NEW (parent echo)
    ChildCompleted { seq: u64, result: ChildResult },                        // NEW (into parent)
}

pub enum ChildResult {                  // child's terminal outcome, recorded in history
    Completed(Vec<u8>),                 // child output bytes
    Failed(workflow::Error),            // child returned/raised a workflow error
}                                       // derive: Clone, Debug, PartialEq, Eq, Serialize, Deserialize
//   From<ChildResult> for Result<Vec<u8>, workflow::Error>

pub struct ChildFuture<W: workflow::Definition> { /* Rc<ContextInner> + seq + input */ }
//   Future::Output = Result<W::Output, workflow::Error>

impl workflow::Context {
    pub fn child_workflow<W: Definition>(&self, input: W::Input) -> ChildFuture<W>; // NEW (spec §9)
    pub fn apply_child_result(&self, seq: u64, result: Result<Vec<u8>, Error>);     // NEW (driver/replay)
}
// ContextInner gains: child_results: RefCell<HashMap<u64, Result<Vec<u8>, Error>>>
// WorkflowState gains: apply_child_result(&self, seq, result)  (delegates to ctx)

// crate `engine`
pub struct NewChild {                                   // a child to create this turn (Pass 4)
    pub seq: i64,                  // parent's StartChild command seq (becomes parent_seq)
    pub child_run_id: String,      // engine-generated uuid for the child run
    pub child_workflow_id: String, // child's dedup id (derived from parent + seq)
    pub workflow_type: String,
    pub input: Vec<u8>,
}                                                       // derive: Debug, Clone, PartialEq, Eq

pub struct ParentNotify {                               // child→parent terminal notification
    pub parent_run_id: String,
    pub event: workflow::Event,    // a ChildCompleted event
}                                                       // derive: Debug, Clone, PartialEq, Eq

pub struct TurnCommit {                                 // + two fields (Pass 4)
    pub events: Vec<Event>,
    pub new_tasks: Vec<NewActivityTask>,
    pub new_timers: Vec<NewTimer>,
    pub new_children: Vec<NewChild>,                    // NEW
    pub parent_notify: Option<ParentNotify>,           // NEW
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>,
}

pub struct RunMeta {                                    // + parent linkage (Pass 4)
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub status: ExecStatus,
    pub parent_run_id: Option<String>,                 // NEW
    pub parent_seq: Option<i64>,                       // NEW
}
```

The `History` / `TaskQueue` **trait signatures do not change** — `commit_turn` already
takes `&TurnCommit`, and `load_run` already returns `RunMeta`; only the struct shapes
grow. The `executions` table already has `parent_run_id` / `parent_seq` columns and
the `history.kind` set already reserves the child kinds (spec §11), so **no schema
migration is needed**.

---

## File Structure

```
/crates/workflow/src/command.rs    # MODIFY: Command::StartChild + round-trip test
/crates/workflow/src/event.rs      # MODIFY: Event::ChildScheduled/ChildCompleted + kind() + tests
/crates/workflow/src/result.rs     # MODIFY: ChildResult enum + From<ChildResult> + tests
/crates/workflow/src/context.rs    # MODIFY: child_results field, child_workflow(), apply_child_result()
/crates/workflow/src/future.rs     # MODIFY: ChildFuture + its poll tests
/crates/workflow/src/state.rs      # MODIFY: WorkflowState::apply_child_result delegate
/crates/workflow/src/replay.rs     # MODIFY: ChildScheduled/ChildCompleted replay + divergence + tests
/crates/workflow/src/lib.rs        # MODIFY: export ChildFuture, ChildResult
/crates/engine/src/types.rs        # MODIFY: NewChild, ParentNotify, TurnCommit fields, RunMeta fields
/crates/engine/src/lib.rs          # MODIFY: NewChild/ParentNotify exported via `pub use types::*`
/crates/engine/src/engine.rs       # MODIFY: driver — StartChild handling, parent_notify, info.parent
/crates/persist/src/history_impl.rs# MODIFY: encode() child kinds, commit_turn children+notify, load_run parent cols
/crates/engine/tests/children.rs   # NEW: Pass-4 e2e acceptance tests
```

> **Build-order note:** Tasks 1–3 are confined to the `workflow` crate and keep the
> whole workspace green (the new enum variants are additive; existing `match`es on
> `Event`/`Command` in `engine` and `persist` use catch-alls or are updated in their
> own tasks). Task 4 adds two fields to `TurnCommit` and `RunMeta`; this **breaks
> every `TurnCommit { … }` literal and `RunMeta { … }` constructor** until they are
> updated. Task 4 updates the engine's literals and `RunMeta` reader expectations and
> verifies with `cargo build -p engine`; **`persist` is RED between Task 4 and Task
> 5** (its `commit_turn`/`load_run` and test literals need the new fields). Task 5
> makes the workspace green again.

---

### Task 1: Workflow protocol — `Command::StartChild`, `Event::ChildScheduled/ChildCompleted`, `ChildResult`

**Files:**
- Modify: `crates/workflow/src/command.rs`, `event.rs`, `result.rs`, `lib.rs`

- [ ] **Step 1: Add `ChildResult` + its `From` to `result.rs` (write the test first)**

In `crates/workflow/src/result.rs`, add the test to `mod tests` (after the existing
two tests):

```rust
    #[test]
    fn child_completed_converts_to_ok_and_failed_to_err() {
        let ok: Result<Vec<u8>, crate::Error> =
            crate::ChildResult::Completed(b"hi".to_vec()).into();
        assert_eq!(ok.unwrap(), b"hi");

        let err: Result<Vec<u8>, crate::Error> =
            crate::ChildResult::Failed(crate::Error::new("boom")).into();
        assert_eq!(err.unwrap_err().message, "boom");
    }

    #[test]
    fn child_result_round_trips_through_json() {
        let c = crate::ChildResult::Completed(b"42".to_vec());
        let back: crate::ChildResult =
            serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
    }
```

Run: `cargo test -p workflow --lib result::` — FAIL (`ChildResult` undefined).

- [ ] **Step 2: Implement `ChildResult`**

At the top of `crates/workflow/src/result.rs`, add the `serde` import and the enum
(the file currently has no imports):

```rust
use serde::{Deserialize, Serialize};

/// A child workflow's terminal outcome, recorded once in the parent's history as the
/// payload of `Event::ChildCompleted` (spec §5.4). Mirrors `CommandResult`'s shape:
/// success carries the child's JSON-encoded output, failure carries the child's
/// `workflow::Error`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildResult {
    Completed(Vec<u8>),
    Failed(crate::Error),
}

impl From<ChildResult> for Result<Vec<u8>, crate::Error> {
    fn from(r: ChildResult) -> Self {
        match r {
            ChildResult::Completed(output) => Ok(output),
            ChildResult::Failed(error) => Err(error),
        }
    }
}
```

Run: `cargo test -p workflow --lib result::` — PASS.

- [ ] **Step 3: Add `Command::StartChild` (write the test first)**

In `crates/workflow/src/command.rs`, extend the round-trip test (`mod tests`) — add
to the end of `round_trips_through_json`:

```rust
        let child = Command::StartChild {
            seq: 3,
            workflow_type: "Ship".into(),
            input: b"{}".to_vec(),
        };
        let back: Command =
            serde_json::from_str(&serde_json::to_string(&child).unwrap()).unwrap();
        assert_eq!(child, back);
```

Run: `cargo test -p workflow --lib command::` — FAIL (`StartChild` undefined).

- [ ] **Step 4: Implement `Command::StartChild`**

In `crates/workflow/src/command.rs`, add the variant after `StartTimer` and update
the doc comment:

```rust
/// Issued by workflow futures, drained by the driver each turn (spec §3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    ScheduleActivity {
        seq: u64,
        activity_type: String,
        input: Vec<u8>,
        retry: RetryPolicy,
    },
    StartTimer {
        seq: u64,
        duration_ms: u64,
    },
    /// Start a child workflow (spec §5.4, §9). Allocates a `seq`; the driver records
    /// it as `Event::ChildScheduled` and creates the child execution.
    StartChild {
        seq: u64,
        workflow_type: String,
        input: Vec<u8>,
    },
}
```

Run: `cargo test -p workflow --lib command::` — PASS.

- [ ] **Step 5: Add `Event::ChildScheduled` / `ChildCompleted` (write the tests first)**

In `crates/workflow/src/event.rs`, add two assertions to `kind_matches_variant`
(before its closing brace):

```rust
        assert_eq!(
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Ship".into(),
                input: vec![],
            }
            .kind(),
            "ChildScheduled"
        );
        assert_eq!(
            Event::ChildCompleted {
                seq: 0,
                result: crate::ChildResult::Completed(vec![]),
            }
            .kind(),
            "ChildCompleted"
        );
```

And add a round-trip test after `signal_received_round_trips_through_json`:

```rust
    #[test]
    fn child_events_round_trip_through_json() {
        let s = Event::ChildScheduled {
            seq: 2,
            workflow_type: "Ship".into(),
            input: b"[1]".to_vec(),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(s, back);

        let c = Event::ChildCompleted {
            seq: 2,
            result: crate::ChildResult::Failed(crate::Error::new("nope")),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
    }
```

Run: `cargo test -p workflow --lib event::` — FAIL (variants undefined).

- [ ] **Step 6: Implement `Event::ChildScheduled` / `ChildCompleted` + `kind()`**

In `crates/workflow/src/event.rs`, add the variants after `SignalReceived` (inside
the `Event` enum) and update `kind()`:

```rust
    /// Parent-side echo that a child workflow was started (spec §5.4). The analog of
    /// `ActivityScheduled` / `TimerStarted`: it carries the command's `seq` for the
    /// divergence check and tells replay this child is already in flight.
    ChildScheduled {
        seq: u64,
        workflow_type: String,
        input: Vec<u8>,
    },
    /// A child workflow reached a terminal status; written into the PARENT's history
    /// (spec §5.4). `seq` is the parent's `StartChild` command seq.
    ChildCompleted {
        seq: u64,
        result: crate::ChildResult,
    },
```

Add to `kind()`:

```rust
            Event::ChildScheduled { .. } => "ChildScheduled",
            Event::ChildCompleted { .. } => "ChildCompleted",
```

Run: `cargo test -p workflow --lib event::` — PASS.

- [ ] **Step 7: Export `ChildResult` and (placeholder) `ChildFuture` slot**

In `crates/workflow/src/lib.rs`, update the `result` re-export line:

```rust
mod result;
pub use result::{ChildResult, CommandResult};
```

(The `future` re-export gains `ChildFuture` in Task 2.)

Run: `cargo build -p workflow` — compiles.

- [ ] **Step 8: Commit**

```bash
git add crates/workflow/src/command.rs crates/workflow/src/event.rs \
        crates/workflow/src/result.rs crates/workflow/src/lib.rs
git commit -m "feat(workflow): StartChild command, Child{Scheduled,Completed} events, ChildResult"
```

---

### Task 2: Workflow — `ctx.child_workflow` + `ChildFuture`

**Files:**
- Modify: `crates/workflow/src/context.rs`, `future.rs`, `state.rs`, `lib.rs`

- [ ] **Step 1: Add the `child_results` field to `ContextInner`**

In `crates/workflow/src/context.rs`, add the field to `ContextInner` (after
`signals`):

```rust
    // Child workflow outcomes, keyed by the parent's StartChild command `seq` (spec
    // §5.4). Resolves `ChildFuture` exactly like `results` resolves `ActivityFuture`.
    pub(crate) child_results: RefCell<HashMap<u64, Result<Vec<u8>, crate::Error>>>,
```

And initialize it in `Context::new` (after `signals: RefCell::new(HashMap::new()),`):

```rust
            child_results: RefCell::new(HashMap::new()),
```

- [ ] **Step 2: Add `child_workflow` and `apply_child_result` to `Context`**

In `crates/workflow/src/context.rs`, add these methods inside `impl Context` (place
`child_workflow` next to `activity`, and `apply_child_result` next to
`apply_result`):

```rust
    /// Start a child workflow (the `workflow.ExecuteChildWorkflow` analog, spec §9).
    /// `seq` is allocated HERE (creation time, Invariant 2). The returned future
    /// emits `StartChild` once and resolves to the child's typed output (or error).
    pub fn child_workflow<W: crate::Definition>(
        &self,
        input: W::Input,
    ) -> crate::future::ChildFuture<W> {
        let seq = self.inner.next_seq.get();
        self.inner.next_seq.set(seq + 1);
        let bytes = serde_json::to_vec(&input).expect("child input serializes");
        crate::future::ChildFuture::new(self.inner.clone(), seq, bytes)
    }

    /// Driver/replay applies one recorded child outcome before a poll (one event per
    /// turn, spec §4.1/§5.4): record it so the next poll resolves the `ChildFuture`.
    pub fn apply_child_result(&self, seq: u64, result: Result<Vec<u8>, crate::Error>) {
        self.inner.child_results.borrow_mut().insert(seq, result);
    }
```

- [ ] **Step 3: Write the failing `ChildFuture` tests**

In `crates/workflow/src/future.rs`, add tests to `mod tests` (after
`surfaces_activity_failure`). A `Definition`-implementing workflow type is needed as
the `W` tag; define a trivial one in the test module:

```rust
    struct Echo;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Echo {
        type Input = i64;
        type Output = i64;
        const TYPE: &'static str = "Echo";
        async fn run(_c: Context, i: i64) -> Result<i64, crate::Error> {
            Ok(i)
        }
    }

    fn poll_child(
        f: &mut crate::future::ChildFuture<Echo>,
    ) -> Poll<Result<i64, crate::Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        Pin::new(f).poll(&mut tcx)
    }

    #[test]
    fn child_first_poll_emits_one_start_child_then_pends() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7);
        assert!(poll_child(&mut f).is_pending());
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(matches!(&cmds[0],
            Command::StartChild { seq: 0, workflow_type, .. } if workflow_type == "Echo"));
    }

    #[test]
    fn child_re_poll_does_not_duplicate_the_command() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7);
        assert!(poll_child(&mut f).is_pending());
        let _ = ctx.drain_commands();
        assert!(poll_child(&mut f).is_pending());
        assert!(ctx.drain_commands().is_empty(), "in-flight child seq must not re-emit");
    }

    #[test]
    fn child_resolves_to_typed_output_when_result_applied() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7);
        assert!(poll_child(&mut f).is_pending());
        ctx.apply_child_result(0, Ok(serde_json::to_vec(&42i64).unwrap()));
        match poll_child(&mut f) {
            Poll::Ready(Ok(v)) => assert_eq!(v, 42),
            other => panic!("expected Ready(Ok(42)), got {other:?}"),
        }
    }

    #[test]
    fn child_surfaces_failure() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7);
        assert!(poll_child(&mut f).is_pending());
        ctx.apply_child_result(0, Err(crate::Error::new("child boom")));
        match poll_child(&mut f) {
            Poll::Ready(Err(e)) => assert_eq!(e.message, "child boom"),
            other => panic!("expected Ready(Err), got {other:?}"),
        }
    }
```

Run: `cargo test -p workflow --lib future::child` — FAIL (`ChildFuture` undefined).

- [ ] **Step 4: Implement `ChildFuture`**

In `crates/workflow/src/future.rs`, append the type after `TimerFuture` (the file
already imports `PhantomData`, `Pin`, `Rc`, `TaskContext`, `Poll`, `Command`,
`ContextInner`):

```rust
/// Awaitable handle for one child workflow (the `ExecuteChildWorkflow` analog, spec
/// §5.4, §9). Resolves to the child's typed Output or a `workflow::Error`. `seq`
/// identifies it in the parent's history; the shared `scheduled` set means it emits
/// `StartChild` exactly once across re-polls (Invariant 4).
pub struct ChildFuture<W: crate::Definition> {
    inner: Rc<ContextInner>,
    seq: u64,
    input: Vec<u8>,
    _marker: PhantomData<fn() -> W>,
}

impl<W: crate::Definition> ChildFuture<W> {
    pub(crate) fn new(inner: Rc<ContextInner>, seq: u64, input: Vec<u8>) -> Self {
        Self {
            inner,
            seq,
            input,
            _marker: PhantomData,
        }
    }
}

impl<W: crate::Definition> Future for ChildFuture<W> {
    type Output = Result<W::Output, crate::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();

        // 1. Replay path: child outcome already recorded -> resolve immediately.
        let recorded = me.inner.child_results.borrow().get(&me.seq).cloned();
        if let Some(recorded) = recorded {
            return Poll::Ready(recorded.and_then(|b| {
                serde_json::from_slice::<W::Output>(&b)
                    .map_err(|e| crate::Error::new(format!("child output deserialize: {e}")))
            }));
        }

        // 2. First arrival: emit StartChild exactly once, then park (Invariant 4).
        if me.inner.scheduled.borrow_mut().insert(me.seq) {
            me.inner.commands.borrow_mut().push(Command::StartChild {
                seq: me.seq,
                workflow_type: W::TYPE.to_string(),
                input: me.input.clone(),
            });
        }
        Poll::Pending
    }
}
```

Run: `cargo test -p workflow --lib future::child` — PASS.

- [ ] **Step 5: Add `WorkflowState::apply_child_result`**

In `crates/workflow/src/state.rs`, add the delegate inside `impl WorkflowState`
(after `apply_signal`):

```rust
    pub fn apply_child_result(&self, seq: u64, result: Result<Vec<u8>, crate::Error>) {
        self.ctx.apply_child_result(seq, result);
    }
```

- [ ] **Step 6: Export `ChildFuture`**

In `crates/workflow/src/lib.rs`, update the `future` re-export:

```rust
mod future;
pub use future::{ActivityFuture, ChildFuture, TimerFuture};
```

Run: `cargo test -p workflow` — all workflow lib tests PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/workflow/src/context.rs crates/workflow/src/future.rs \
        crates/workflow/src/state.rs crates/workflow/src/lib.rs
git commit -m "feat(workflow): ctx.child_workflow + ChildFuture + apply_child_result"
```

---

### Task 3: Workflow — child replay in `cold_replay` (pure determinism)

**Files:**
- Modify: `crates/workflow/src/replay.rs`

- [ ] **Step 1: Write the failing replay tests**

In `crates/workflow/src/replay.rs` `mod tests`, add a parent workflow definition and
three tests (after the signal tests at the end of the module). The `Child` type is
only a `Definition` *tag* — replay never executes it; the parent's `ChildFuture`
resolves from the recorded `ChildCompleted`.

```rust
    // --- Pass 4a: child workflows -----------------------------------------

    // A parent that starts one child with input 5 and returns (child_output + 1).
    struct Parent;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Parent {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Parent";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let v = ctx.child_workflow::<Child>(5).await?;
            Ok(v + 1)
        }
    }
    struct Child;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Child {
        type Input = i64;
        type Output = i64;
        const TYPE: &'static str = "Child";
        async fn run(_ctx: Context, i: i64) -> Result<i64, Error> {
            Ok(i * 2)
        }
    }

    fn parent_info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "Parent".into(),
        }
    }

    #[test]
    fn replays_child_completed_to_parent_output() {
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Child".into(),
                input: serde_json::to_vec(&5i64).unwrap(),
            },
            Event::ChildCompleted {
                seq: 0,
                result: crate::ChildResult::Completed(serde_json::to_vec(&10i64).unwrap()),
            },
        ];
        let outcome = cold_replay::<Parent>(parent_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 11, "child returned 10 (=5*2); parent adds 1");
        assert_eq!(outcome.commands.len(), 1);
        assert!(matches!(
            &outcome.commands[0],
            Command::StartChild { seq: 0, .. }
        ));
    }

    #[test]
    fn child_failure_propagates_to_parent_error() {
        // The child failed; the parent's `?` turns the ChildResult::Failed into a
        // workflow error and returns it — completion is Some(Err), NOT nondeterminism.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Child".into(),
                input: serde_json::to_vec(&5i64).unwrap(),
            },
            Event::ChildCompleted {
                seq: 0,
                result: crate::ChildResult::Failed(Error::new("child died")),
            },
        ];
        let outcome = cold_replay::<Parent>(parent_info(), &h).unwrap();
        match outcome.completion {
            Some(Err(e)) => assert_eq!(e.message, "child died"),
            other => panic!("expected Some(Err(child died)), got {other:?}"),
        }
    }

    #[test]
    fn detects_divergent_child_type() {
        // History recorded a child of type "Other" at seq 0, but Parent emits "Child".
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Other".into(),
                input: serde_json::to_vec(&5i64).unwrap(),
            },
        ];
        let err = cold_replay::<Parent>(parent_info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(err.detail.contains("Other"));
    }
```

Run: `cargo test -p workflow --lib replay::tests::replays_child` — FAIL (the new
events are not handled; `ChildFuture` never resolves and `StartChild` is unchecked).

- [ ] **Step 2: Wire children into `cold_replay`**

In `crates/workflow/src/replay.rs`, make three edits inside `cold_replay`.

(a) Extend the `Applied` enum (add a `Child` variant) and add a `recorded_child`
map. Replace the `enum Applied { … }` and the two `recorded_*` declarations with:

```rust
    enum Applied {
        Result(u64, CommandResult),
        Timer(u64),
        Signal(String, Vec<u8>),
        Child(u64, Result<Vec<u8>, crate::Error>),
    }
    let mut recorded_sched: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
    let mut recorded_timer: HashMap<u64, u64> = HashMap::new(); // seq -> duration_ms
    let mut recorded_child: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
    let mut applied: Vec<Applied> = Vec::new();
```

(b) In the `for ev in history` match, add arms for the child events (place them next
to the `SignalReceived` arm):

```rust
            Event::ChildScheduled {
                seq,
                workflow_type,
                input,
            } => {
                recorded_child.insert(*seq, (workflow_type.clone(), input.clone()));
            }
            Event::ChildCompleted { seq, result } => {
                applied.push(Applied::Child(*seq, result.clone().into()));
            }
```

(c) In the command-divergence `match &cmd` block, add a `StartChild` arm (after the
`StartTimer` arm):

```rust
                Command::StartChild {
                    seq,
                    workflow_type,
                    input,
                } => {
                    if let Some((rty, rin)) = recorded_child.get(seq) {
                        if rty != workflow_type || rin != input {
                            return Err(Nondeterminism {
                                seq: *seq,
                                detail: format!(
                                    "history recorded child {rty}, workflow emitted {workflow_type}"
                                ),
                            });
                        }
                    }
                }
```

(d) In the `Poll::Pending` arm's `match &applied[cursor]`, add the `Child` case:

```rust
                        Applied::Child(seq, r) => state.apply_child_result(*seq, r.clone()),
```

Run: `cargo test -p workflow --lib replay::` — all replay tests PASS (existing +
three new).

- [ ] **Step 3: Whole-crate green**

Run: `cargo test -p workflow` — all PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/workflow/src/replay.rs
git commit -m "feat(workflow): cold_replay child workflows — apply ChildCompleted + StartChild divergence check"
```

---

### Task 4: Engine — driver wiring for child workflows

**Files:**
- Modify: `crates/engine/src/types.rs`, `lib.rs`, `engine.rs`

- [ ] **Step 1: Add `NewChild`, `ParentNotify`, and grow `TurnCommit` / `RunMeta`**

In `crates/engine/src/types.rs`:

Add after `NewTimer`:

```rust
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
/// — in the same transaction, so completion-and-notification is atomic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParentNotify {
    pub parent_run_id: String,
    pub event: Event, // a ChildCompleted event
}
```

Grow `TurnCommit` (add the two fields after `new_timers`):

```rust
    pub new_children: Vec<NewChild>,
    pub parent_notify: Option<ParentNotify>,
```

Grow `RunMeta` (add after `status`):

```rust
    pub parent_run_id: Option<String>,
    pub parent_seq: Option<i64>,
```

- [ ] **Step 2: Export the new types**

In `crates/engine/src/lib.rs`, no change is needed — `pub use types::*;` already
re-exports `NewChild` and `ParentNotify`. Confirm by reading line 3.

- [ ] **Step 3: Update the engine's own `TurnCommit` literals**

In `crates/engine/src/engine.rs` there are three `TurnCommit { … }` literals (the
early-return terminal path ~line 224, the main decision turn ~line 333, and
`dead_letter` ~line 369). For the **early-return** and **dead_letter** literals, add
these two fields (after `new_timers: Vec::new(),`):

```rust
                new_children: Vec::new(),
                parent_notify: None,
```

(The main decision-turn literal is rewritten in Step 5; leave it for now — the engine
will not compile until Step 5, which is expected.)

- [ ] **Step 4: Thread child creation, parent notification, and `info.parent` into the driver**

In `crates/engine/src/engine.rs`, edit `process_one_runnable`.

(a) Update the imports at the top of the file to bring in the new types:

```rust
use crate::{
    ExecStatus, History, NewActivityTask, NewChild, NewTimer, ParentNotify, SignalOutcome,
    TaskQueue, TurnCommit,
};
```

(b) Extend the `recorded` set to include `ChildScheduled` seqs:

```rust
        let recorded: HashSet<u64> = events
            .iter()
            .filter_map(|e| match e {
                workflow::Event::ActivityScheduled { seq, .. }
                | workflow::Event::TimerStarted { seq, .. }
                | workflow::Event::ChildScheduled { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect();
```

(c) Replace the hard-coded `parent: None` `info` construction with a real parent
lookup. Replace the existing `let info = workflow::Info { … parent: None … };` block
with:

```rust
        // A child run records its parent's identity so `ctx.info().parent` is correct
        // (spec §9). The parent's workflow_id comes from its own execution row.
        let parent = match &meta.parent_run_id {
            Some(prid) => self
                .history
                .load_run(prid)
                .await?
                .map(|pm| workflow::Execution {
                    workflow_id: pm.workflow_id,
                    run_id: prid.clone(),
                }),
            None => None,
        };
        let info = workflow::Info {
            execution: workflow::Execution {
                workflow_id: meta.workflow_id.clone(),
                run_id: run_id.clone(),
            },
            parent,
            workflow_type: meta.workflow_type.clone(),
        };
```

(d) In the command loop, declare `new_children` alongside the other accumulators and
add a `StartChild` arm. Change:

```rust
        let mut new_events = Vec::new();
        let mut new_tasks = Vec::new();
        let mut new_timers = Vec::new();
```

to add:

```rust
        let mut new_children = Vec::new();
```

and add this arm to `match cmd` (after the `StartTimer` arm, before the closing
brace of the match):

```rust
                workflow::Command::StartChild {
                    seq,
                    workflow_type,
                    input,
                } => {
                    if recorded.contains(seq) {
                        continue;
                    }
                    new_events.push(workflow::Event::ChildScheduled {
                        seq: *seq,
                        workflow_type: workflow_type.clone(),
                        input: input.clone(),
                    });
                    new_children.push(NewChild {
                        seq: *seq as i64,
                        child_run_id: uuid::Uuid::new_v4().to_string(),
                        child_workflow_id: format!("{}::child::{}", meta.workflow_id, seq),
                        workflow_type: workflow_type.clone(),
                        input: input.clone(),
                    });
                }
```

(e) After the `(status, result)` computation, build the parent notification (only on
a terminal turn of a run that *has* a parent):

```rust
        // If this run is a child and just reached a terminal status, notify the parent
        // in the same transaction (spec §5.4) so completion+notification is atomic.
        let parent_notify = match (&meta.parent_run_id, meta.parent_seq, &outcome.completion) {
            (Some(prid), Some(pseq), Some(Ok(bytes))) => Some(ParentNotify {
                parent_run_id: prid.clone(),
                event: workflow::Event::ChildCompleted {
                    seq: pseq as u64,
                    result: workflow::ChildResult::Completed(bytes.clone()),
                },
            }),
            (Some(prid), Some(pseq), Some(Err(err))) => Some(ParentNotify {
                parent_run_id: prid.clone(),
                event: workflow::Event::ChildCompleted {
                    seq: pseq as u64,
                    result: workflow::ChildResult::Failed(err.clone()),
                },
            }),
            _ => None,
        };
```

(f) Add the two new fields to the main decision-turn `TurnCommit` literal:

```rust
        let commit = TurnCommit {
            events: new_events,
            new_tasks,
            new_timers,
            new_children,
            parent_notify,
            status,
            result: result.clone(),
        };
```

- [ ] **Step 5: Build the engine crate**

Run: `cargo build -p engine`
Expected: compiles. (`persist` is RED until Task 5 — its `commit_turn` ignores the
new `TurnCommit` fields, its `load_run` does not populate the new `RunMeta` fields,
and its test literals lack the new fields. Do not run workspace-wide `cargo test`
yet.)

- [ ] **Step 6: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): driver creates child workflows + notifies parent on terminal + info.parent"
```

---

### Task 5: Persist — child creation + parent notification in `commit_turn`, parent cols in `load_run`

**Files:**
- Modify: `crates/persist/src/history_impl.rs`

- [ ] **Step 1: Update existing `TurnCommit` / `RunMeta` literals so persist compiles**

`persist` does not compile until its existing `TurnCommit` literals carry the new
fields. Add these two lines (after each `new_timers: …,` field) to **every**
`TurnCommit { … }` literal in:

- `crates/persist/src/history_impl.rs` — 4 literals in `mod tests`
  (`commit_turn_appends_clears_runnable_and_sets_status`,
  `commit_turn_round_trips_signal_received_with_null_seq`,
  `append_signal_delivers_to_running_and_marks_runnable` (the `idle` literal),
  `append_signal_to_terminal_run_is_not_running` (the `done` literal)):

```rust
            new_children: vec![],
            parent_notify: None,
```

- `crates/persist/src/taskqueue_impl.rs` — 5 literals in `mod tests`
  (`db_with_task`, `lease_round_trips_the_scheduled_retry_policy`,
  `task_not_due_yet_is_not_leasable`, `fire_due_timer_appends_timer_fired_and_makes_runnable`,
  `timer_not_due_yet_does_not_fire`): same two lines after each `new_timers: …,`.

(These are mechanical. `grep -rn "new_timers" crates/persist` lists every site.)

- [ ] **Step 2: `encode()` — seq for the child events**

In `crates/persist/src/history_impl.rs`, update the `encode` match so both child
events expose their `seq`:

```rust
    let seq = match event {
        Event::ActivityScheduled { seq, .. }
        | Event::ActivityCompleted { seq, .. }
        | Event::ActivityFailed { seq, .. }
        | Event::TimerStarted { seq, .. }
        | Event::TimerFired { seq }
        | Event::ChildScheduled { seq, .. }
        | Event::ChildCompleted { seq, .. } => Some(*seq as i64),
        Event::WorkflowStarted { .. } | Event::SignalReceived { .. } => None,
    };
```

- [ ] **Step 3: `load_run` — read `parent_run_id` / `parent_seq`**

In `crates/persist/src/history_impl.rs`, replace the `load_run` body's query +
mapping with:

```rust
    async fn load_run(&self, run_id: &str) -> anyhow::Result<Option<RunMeta>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT workflow_id, workflow_type, status, parent_run_id, parent_seq \
                 FROM executions WHERE run_id = ?1",
                params![run_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .optional()?;
        Ok(
            row.map(|(workflow_id, workflow_type, status, parent_run_id, parent_seq)| RunMeta {
                run_id: run_id.to_string(),
                workflow_id,
                workflow_type,
                status: ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
                parent_run_id,
                parent_seq,
            }),
        )
    }
```

- [ ] **Step 4: Write the failing `commit_turn` child tests**

In `crates/persist/src/history_impl.rs` `mod tests`, add (after
`commit_turn_round_trips_signal_received_with_null_seq`). Update the test `use` if
needed — `NewChild` and `ParentNotify` come from `engine`:

```rust
    #[tokio::test]
    async fn commit_turn_creates_child_execution_and_marks_it_runnable() {
        use engine::NewChild;
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("parent-run", "parent-wf", "Parent", b"in")
            .await
            .unwrap();

        let commit = TurnCommit {
            events: vec![Event::ChildScheduled {
                seq: 0,
                workflow_type: "Child".into(),
                input: b"5".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![NewChild {
                seq: 0,
                child_run_id: "child-run".into(),
                child_workflow_id: "parent-wf::child::0".into(),
                workflow_type: "Child".into(),
                input: b"5".to_vec(),
            }],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("parent-run", &commit).await.unwrap();

        // The child execution exists, is running, and links back to the parent.
        let meta = db.load_run("child-run").await.unwrap().unwrap();
        assert_eq!(meta.workflow_type, "Child");
        assert_eq!(meta.status, ExecStatus::Running);
        assert_eq!(meta.parent_run_id.as_deref(), Some("parent-run"));
        assert_eq!(meta.parent_seq, Some(0));

        // The child has a WorkflowStarted event and is runnable.
        let h = db.read_history("child-run").await.unwrap();
        assert_eq!(h.len(), 1);
        assert!(matches!(h[0].event, Event::WorkflowStarted { .. }));
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            Some("child-run".into())
        );
    }

    #[tokio::test]
    async fn commit_turn_notifies_parent_with_child_completed() {
        use engine::ParentNotify;
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("parent-run", "parent-wf", "Parent", b"in")
            .await
            .unwrap();
        db.create_execution("child-run", "child-wf", "Child", b"5")
            .await
            .unwrap();
        // Drive both runnable flags away so we can observe the notify re-arm one.
        let idle = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("parent-run", &idle).await.unwrap();

        // The child's terminal turn: complete the child AND notify the parent.
        let commit = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![],
            parent_notify: Some(ParentNotify {
                parent_run_id: "parent-run".into(),
                event: Event::ChildCompleted {
                    seq: 0,
                    result: workflow::ChildResult::Completed(b"10".to_vec()),
                },
            }),
            status: ExecStatus::Completed,
            result: Some(b"10".to_vec()),
        };
        db.commit_turn("child-run", &commit).await.unwrap();

        // ChildCompleted landed in the PARENT's history (with the parent's seq).
        let h = db.read_history("parent-run").await.unwrap();
        match &h.last().unwrap().event {
            Event::ChildCompleted { seq: 0, result } => {
                assert_eq!(*result, workflow::ChildResult::Completed(b"10".to_vec()));
            }
            other => panic!("expected ChildCompleted, got {other:?}"),
        }
        // The parent is runnable again; the child is not.
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            Some("parent-run".into())
        );
    }
```

Run: `cargo test -p persist --lib commit_turn_creates_child` — FAIL (`commit_turn`
does not yet create children or notify parents).

- [ ] **Step 5: Implement child creation + parent notification in `commit_turn`**

In `crates/persist/src/history_impl.rs`, update the engine import to bring in the new
types:

```rust
use engine::{
    CreateOutcome, ExecStatus, History, NewChild, ParentNotify, RunMeta, SignalOutcome,
    StoredEvent, TurnCommit,
};
```

> If `cargo clippy` later flags `NewChild` / `ParentNotify` as unused imports (they
> are only referenced through `commit.new_children` / `commit.parent_notify` field
> access, not named), drop them from this `use` — keep only the names the code names.
> Re-add only what compiles cleanly.

In `commit_turn`, add the two blocks **after** the `new_timers` loop and **before**
the `UPDATE executions` statement:

```rust
        for child in &commit.new_children {
            tx.execute(
                "INSERT INTO executions \
                 (run_id, workflow_id, workflow_type, parent_run_id, parent_seq, input, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running')",
                params![
                    child.child_run_id,
                    child.child_workflow_id,
                    child.workflow_type,
                    run_id,
                    child.seq,
                    child.input
                ],
            )?;
            let (cseq, ckind, cpayload) = encode(&Event::WorkflowStarted {
                input: child.input.clone(),
            });
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, 1, ?2, ?3, ?4, ?5)",
                params![child.child_run_id, cseq, ckind, cpayload, now_ms()],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
                params![child.child_run_id, now_ms()],
            )?;
        }

        if let Some(notify) = &commit.parent_notify {
            let (pseq, pkind, ppayload) = encode(&notify.event);
            let pid = next_event_id(&tx, &notify.parent_run_id)?;
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![notify.parent_run_id, pid, pseq, pkind, ppayload, now_ms()],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
                params![notify.parent_run_id, now_ms()],
            )?;
        }
```

Run: `cargo test -p persist` — all PASS (the two new tests + every earlier test).

- [ ] **Step 6: Whole-workspace build + lib tests**

Run: `cargo test` — every crate's tests PASS again (workspace green).

- [ ] **Step 7: Commit**

```bash
git add crates/persist
git commit -m "feat(persist): commit_turn creates child executions + notifies parent; load_run reads parent cols"
```

---

### Task 6: Pass-4 acceptance — child workflow e2e

**Files:**
- Create: `crates/engine/tests/children.rs`

- [ ] **Step 1: Write the integration tests**

Create `crates/engine/tests/children.rs`:

```rust
use std::sync::Arc;

use engine::{Engine, ExecStatus, History, StartOptions};
use persist::Sqlite;

// Activity: Double(n) -> n * 2.
struct Double;
#[async_trait::async_trait]
impl activity::Definition for Double {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Double";
    async fn run(_c: activity::Context, n: i64) -> Result<i64, activity::Error> {
        Ok(n * 2)
    }
}

// Child: runs one activity (Double) so cold recovery of the child is exercised too.
struct Child;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Child {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Child";
    async fn run(ctx: workflow::Context, n: i64) -> Result<i64, workflow::Error> {
        let v = ctx.activity::<Double>(n).await?;
        Ok(v)
    }
}

// Parent: starts Child(input), returns child_output + 1.
struct Parent;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Parent {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Parent";
    async fn run(ctx: workflow::Context, n: i64) -> Result<i64, workflow::Error> {
        let v = ctx.child_workflow::<Child>(n).await?;
        Ok(v + 1)
    }
}

// Child that returns its parent's workflow_id, to prove info.parent is populated.
struct ParentIdChild;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for ParentIdChild {
    type Input = ();
    type Output = String;
    const TYPE: &'static str = "ParentIdChild";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<String, workflow::Error> {
        let parent = ctx
            .info()
            .parent
            .as_ref()
            .map(|p| p.workflow_id.clone())
            .unwrap_or_else(|| "<none>".into());
        Ok(parent)
    }
}
struct ParentOfIdChild;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for ParentOfIdChild {
    type Input = ();
    type Output = String;
    const TYPE: &'static str = "ParentOfIdChild";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<String, workflow::Error> {
        let id = ctx.child_workflow::<ParentIdChild>(()).await?;
        Ok(id)
    }
}

// Child that always fails; parent propagates via `?`.
struct FailChild;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for FailChild {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "FailChild";
    async fn run(_ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        Err(workflow::Error::new("child failed on purpose"))
    }
}
struct ParentOfFail;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for ParentOfFail {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "ParentOfFail";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let v = ctx.child_workflow::<FailChild>(()).await?;
        Ok(v)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Parent>();
    e.register_workflow::<Child>();
    e.register_workflow::<ParentOfIdChild>();
    e.register_workflow::<ParentIdChild>();
    e.register_workflow::<ParentOfFail>();
    e.register_workflow::<FailChild>();
    e.register_activity::<Double>();
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
async fn parent_completes_when_child_does() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(5, StartOptions { id: "p-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 11, "child Double(5)=10, parent adds 1");

    // The child execution exists, completed, and links back to the parent.
    let (child_run, status, _) = db.find_execution("p-1::child::0").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let meta = db.load_run(&child_run).await.unwrap().unwrap();
    assert_eq!(meta.parent_seq, Some(0));
}

#[tokio::test]
async fn child_info_parent_is_populated() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<ParentOfIdChild>((), StartOptions { id: "pid-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: String = handle.result().await.unwrap();
    assert_eq!(
        out, "pid-1",
        "the child observed its parent's workflow_id via ctx.info().parent"
    );
}

#[tokio::test]
async fn child_failure_propagates_to_parent() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<ParentOfFail>((), StartOptions { id: "pf-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    // The child failed; the parent's `?` turned that into a workflow failure.
    let (_, child_status, _) = db.find_execution("pf-1::child::0").await.unwrap().unwrap();
    assert_eq!(child_status, ExecStatus::Failed);
    let (_, parent_status, _) = db.find_execution("pf-1").await.unwrap().unwrap();
    assert_eq!(parent_status, ExecStatus::Failed);
}

#[tokio::test]
async fn cold_recovery_completes_parent_and_child() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start the parent, drive a few turns (parent starts the child, child
    // schedules its activity), then drop the engine — simulating a crash mid-flight.
    {
        let engine = build(&db);
        engine
            .start_workflow::<Parent>(5, StartOptions { id: "p-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // parent: StartChild, child created
        assert!(engine.process_one_runnable().await.unwrap()); // child: schedules Double activity
                                                               // engine dropped here; only the shared `db` survives.
    }

    // Phase 2: a fresh engine with no in-memory state cold-replays both runs and
    // finishes — child completes, notifies the parent, parent completes (spec §13).
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("p-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 11);
}
```

- [ ] **Step 2: Run the acceptance tests**

Run: `cargo test -p engine --test children`
Expected: all four PASS — `parent_completes_when_child_does`,
`child_info_parent_is_populated`, `child_failure_propagates_to_parent`,
`cold_recovery_completes_parent_and_child`.

> **If `cold_recovery_completes_parent_and_child` is flaky on turn counts:** the two
> `process_one_runnable` asserts in Phase 1 assume the parent's first turn creates the
> child and the child's first turn schedules its activity. Both are deterministic
> here (no activity completes in Phase 1), so the asserts hold; if a future change
> makes them brittle, drop the asserts and let `pump` in Phase 2 do all the work — the
> durability claim is unchanged.

- [ ] **Step 3: Commit**

```bash
git add crates/engine/tests/children.rs
git commit -m "test(engine): pass 4 acceptance — parent/child completion, info.parent, failure, cold recovery"
```

---

### Task 7: Whole-workspace green + clippy + fmt + roadmap update

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS (all earlier passes
  still green; the new code is additive).
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean. (If the
  `NewChild` / `ParentNotify` imports in `persist/src/history_impl.rs` are flagged
  unused, remove them per Task 5 Step 5's note and re-run.)
- [ ] **Step 3:** Run `cargo fmt --all -- --check` — no drift. (Run `cargo fmt --all`
  first if it reports changes, then re-check.)
- [ ] **Step 4:** In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
  - In the chunk table, set chunk `4a` status to `done` and its Plan file to
    `2026-06-14-pass-4a-child-workflows.md`.
  - In "Canonical types": add `Command::StartChild`; add `Event::ChildScheduled` /
    `Event::ChildCompleted`; add the `ChildResult` enum + its `From`; add
    `ChildFuture`, `Context::child_workflow`, `Context::apply_child_result`, the
    `ContextInner.child_results` field, and `WorkflowState::apply_child_result`; in
    the `engine` block add `NewChild`, `ParentNotify`, the two new `TurnCommit`
    fields, and the two new `RunMeta` fields. Mark each `// 4a (done)`.
- [ ] **Step 5:** Commit:

```bash
git add -A
git commit -m "chore: pass 4a complete — child workflows, roadmap + canonical types updated"
```

---

## Notes

- **Why child creation lives in `commit_turn`, not a separate call.** The parent's
  `ChildScheduled` echo and the child's creation must be one atomic unit (Invariant
  5): if they could tear, a crash would leave a `ChildScheduled` in the parent's
  history (so replay would *not* re-emit `StartChild`, per the `recorded` set) with no
  child row to ever complete it — a permanent deadlock. Threading `new_children`
  through `TurnCommit` keeps it in the same SQLite transaction. The mirror image —
  the child's terminal status and the parent's `ChildCompleted` — is atomic for the
  same reason, via `parent_notify`.

- **Child run_id is a fresh uuid; child workflow_id is derived.** `child_run_id` is a
  uuid minted in the driver, so a crash *before* the parent's commit simply rolls back
  and a later replay mints a new one (no orphan). `child_workflow_id =
  "{parent_workflow_id}::child::{seq}"` is deterministic and human-readable for the
  list/describe UI (spec §7.4); the plain `INSERT` (not `OR IGNORE`) is safe because
  the `recorded` set guarantees `StartChild` is emitted at most once per committed
  history, so the same child is never created twice.

- **`ChildCompleted` carries a `seq` and IS divergence-relevant, but only its
  schedule is checked.** `StartChild` is compared against the recorded
  `ChildScheduled` (type + input) exactly like `ScheduleActivity` vs
  `ActivityScheduled` (Invariant 9). The `ChildCompleted` payload is an applied
  outcome, recorded once and replayed in `event_id` order (like an activity result) —
  it is not re-derived by the workflow, so it needs no divergence check.

- **No new tables, no new queue.** Children reuse `executions` (with the existing
  `parent_run_id` / `parent_seq` columns), `history`, and `runnable`. This is the same
  "reuse the spine" discipline signals followed in Pass 3 (spec §11).

- **Cancellation stays deferred (spec §6.4).** Nothing here implements cooperative
  cancellation; the parent simply blocks on the child. Cancelling a parent would later
  propagate to children via the reserved `WorkflowCancelRequested` inbound-event kind,
  reusing this same parent/child linkage — not a refactor.

## Self-Review (completed during authoring)

- **Spec coverage:** §5.4 (child = `executions` row with `parent_run_id`/`parent_seq`;
  terminal child writes `ChildCompleted` into the parent's history and re-marks the
  parent runnable) — Tasks 4–5. §9 (`ctx.child_workflow::<W>(input).await`,
  `workflow::Info.parent` populated for children) — Tasks 2, 4, and the
  `child_info_parent_is_populated` acceptance test. §9.1 mapping
  (`ExecuteChildWorkflow` ↔ `child_workflow`) — Task 2. §11 (`ChildScheduled` /
  `ChildCompleted` history kinds, no new table) — Tasks 1, 5. §12 Invariants 2/4/5/9
  (seq at creation, emit-once, atomic turn, divergence check) — Tasks 2–5. §13 Pass-4
  acceptance gate ("a parent awaiting a child completes when the child does, across
  cold recovery of either") — `parent_completes_when_child_does` +
  `cold_recovery_completes_parent_and_child`.
- **Placeholders:** none — full code, exact commands, expected outcomes throughout.
- **Type consistency:** `Command::StartChild { seq: u64, workflow_type: String, input:
  Vec<u8> }`, `Event::ChildScheduled { seq, workflow_type, input }`,
  `Event::ChildCompleted { seq, result: ChildResult }`, `ChildResult::{Completed(Vec<u8>),
  Failed(workflow::Error)}` with `From<ChildResult> for Result<Vec<u8>, Error>`,
  `ChildFuture<W>::Output = Result<W::Output, workflow::Error>`,
  `Context::child_workflow::<W>(input) -> ChildFuture<W>`,
  `Context::apply_child_result(&self, u64, Result<Vec<u8>, Error>)`,
  `WorkflowState::apply_child_result` (same signature), `NewChild { seq: i64,
  child_run_id, child_workflow_id, workflow_type, input }`, `ParentNotify {
  parent_run_id: String, event: Event }`, `TurnCommit.new_children: Vec<NewChild>` +
  `.parent_notify: Option<ParentNotify>`, `RunMeta.parent_run_id: Option<String>` +
  `.parent_seq: Option<i64>` — all used identically across workflow, engine, and
  persist tasks. `encode()` and the driver's `recorded` set both treat
  `ChildScheduled` like `ActivityScheduled`/`TimerStarted`.
