# Pass 1b — Replay Core — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a workflow function durably replayable: `workflow::Context` with
`seq`/`results`/`scheduled`/`commands`, the `ActivityFuture`, a `WorkflowState`
harness to step a workflow, and a pure `cold_replay` that reproduces the command
stream from history and detects nondeterminism (spec §3, §4.1, §12).

**Architecture:** Everything here is **pure** — no tokio, no backend. A workflow
future is polled with a no-op waker; the driver (chunk 1d) will reuse these
primitives. `Context` shares `Rc<ContextInner>` so the future the user awaits and
the harness that feeds results see the same state. Single-threaded by design, so
the future is `!Send` (matches `workflow::Definition(?Send)` from 1a).

**Tech Stack:** Rust 2021, `Rc`/`RefCell`/`Cell`, `futures::task::noop_waker`,
serde_json.

**Depends on:** chunk 1a (all protocol types).

---

## File Structure

```
/crates/workflow/src/context.rs   # REWRITE: Rc<ContextInner> + replay state
/crates/workflow/src/future.rs    # NEW: ActivityFuture<A>
/crates/workflow/src/state.rs     # NEW: WorkflowState harness
/crates/workflow/src/replay.rs    # NEW: cold_replay + Nondeterminism
/crates/workflow/src/error.rs     # MODIFY: add From<activity::Error>
/crates/workflow/src/lib.rs       # MODIFY: wire new modules
/crates/workflow/Cargo.toml       # MODIFY: serde_json -> deps; add futures
```

---

### Task 1: Cargo deps + `Context` rewrite to shared replay state

**Files:**
- Modify: `crates/workflow/Cargo.toml`
- Modify: `crates/workflow/src/context.rs`

- [ ] **Step 1: Move serde_json into deps and add futures**

Edit `crates/workflow/Cargo.toml` so the `[dependencies]` section reads:

```toml
[dependencies]
activity    = { path = "../activity" }
serde       = { workspace = true }
serde_json  = { workspace = true }
thiserror   = { workspace = true }
async-trait = { workspace = true }
futures     = { workspace = true }

[dev-dependencies]
tokio = { workspace = true }
```

- [ ] **Step 2: Rewrite `context.rs` (replace the whole file)**

```rust
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use crate::future::ActivityFuture;
use crate::{Command, CommandResult, Info, RetryPolicy};

/// Shared, single-threaded replay state. `Context` is a cheap handle to this.
pub(crate) struct ContextInner {
    pub(crate) info: Info,
    pub(crate) next_seq: Cell<u64>,
    pub(crate) results: RefCell<HashMap<u64, CommandResult>>, // seq -> recorded outcome
    pub(crate) scheduled: RefCell<HashSet<u64>>,              // seqs emitted this life
    pub(crate) commands: RefCell<Vec<Command>>,               // emitted this turn
}

#[derive(Clone)]
pub struct Context {
    inner: Rc<ContextInner>,
}

impl Context {
    pub fn new(info: Info) -> Self {
        Self {
            inner: Rc::new(ContextInner {
                info,
                next_seq: Cell::new(0),
                results: RefCell::new(HashMap::new()),
                scheduled: RefCell::new(HashSet::new()),
                commands: RefCell::new(Vec::new()),
            }),
        }
    }

    pub fn info(&self) -> &Info {
        &self.inner.info
    }

    /// Schedule an activity. `seq` is allocated HERE (creation time, spec §3/Inv 2).
    pub fn activity<A: activity::Definition>(&self, input: A::Input) -> ActivityFuture<A> {
        let seq = self.inner.next_seq.get();
        self.inner.next_seq.set(seq + 1);
        let bytes = serde_json::to_vec(&input).expect("activity input serializes");
        ActivityFuture::new(self.inner.clone(), seq, bytes, RetryPolicy::none())
    }

    /// Driver applies exactly one recorded outcome before each poll (spec §4.1).
    pub fn apply_result(&self, seq: u64, result: CommandResult) {
        self.inner.results.borrow_mut().insert(seq, result);
    }

    /// Driver drains commands emitted during the poll it just ran.
    pub fn drain_commands(&self) -> Vec<Command> {
        self.inner.commands.borrow_mut().drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::Execution;

    fn info() -> Info {
        Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "T".into(),
        }
    }

    #[test]
    fn apply_then_drain_are_independent() {
        let ctx = Context::new(info());
        ctx.apply_result(0, CommandResult::ActivityCompleted(b"x".to_vec()));
        // applying a result does not by itself emit a command
        assert!(ctx.drain_commands().is_empty());
        assert_eq!(ctx.info().run_id_str(), "r");
    }
}
```

> The test calls a helper `run_id_str()` that does not exist — replace that line
> with `assert_eq!(ctx.info().execution.run_id, "r");`. (Kept here to force you to
> read the assertion; fix it before running.)

- [ ] **Step 3: Run the test (expect a compile error first)**

Run: `cargo test -p workflow context`
Expected: FAILS to compile because `crate::future::ActivityFuture` does not exist
yet, and because of the deliberate `run_id_str()` line. Fix the assertion line as
noted; the `ActivityFuture` error resolves in Task 2.

- [ ] **Step 4: Commit (after Task 2 makes it compile)**

Defer the commit for this file until Task 2; they compile together.

---

### Task 2: `ActivityFuture<A>` + `From<activity::Error>` for workflow error

**Files:**
- Create: `crates/workflow/src/future.rs`
- Modify: `crates/workflow/src/error.rs`, `crates/workflow/src/lib.rs`

- [ ] **Step 1: Add the error conversion (ergonomic `?`)**

Append to `crates/workflow/src/error.rs` (inside the file, after the `impl Error`):

```rust
impl From<activity::Error> for Error {
    fn from(e: activity::Error) -> Self {
        Error { message: e.message }
    }
}
```

- [ ] **Step 2: Create `future.rs`**

```rust
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

use crate::context::ContextInner;
use crate::{Command, RetryPolicy};

/// Awaitable handle for one activity call. Resolves to the activity's typed
/// Output (deserialized) or its Error. `seq` identifies it in history (spec §3).
pub struct ActivityFuture<A: activity::Definition> {
    inner: Rc<ContextInner>,
    seq: u64,
    input: Vec<u8>,
    retry: RetryPolicy,
    _marker: PhantomData<A>,
}

impl<A: activity::Definition> ActivityFuture<A> {
    pub(crate) fn new(inner: Rc<ContextInner>, seq: u64, input: Vec<u8>, retry: RetryPolicy) -> Self {
        Self { inner, seq, input, retry, _marker: PhantomData }
    }

    /// Builder: attach a retry policy (spec §7). Default is single-attempt.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }
}

impl<A: activity::Definition> Future for ActivityFuture<A> {
    type Output = Result<A::Output, activity::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();

        // 1. Replay path: outcome already recorded -> resolve immediately.
        let recorded = me.inner.results.borrow().get(&me.seq).cloned();
        if let Some(recorded) = recorded {
            let bytes: Result<Vec<u8>, activity::Error> = recorded.into();
            return Poll::Ready(bytes.and_then(|b| {
                serde_json::from_slice::<A::Output>(&b).map_err(|e| {
                    activity::Error::fatal(format!("activity output deserialize: {e}"))
                })
            }));
        }

        // 2. First arrival: emit the command exactly once, then park (spec §3/Inv 4).
        if me.inner.scheduled.borrow_mut().insert(me.seq) {
            me.inner.commands.borrow_mut().push(Command::ScheduleActivity {
                seq: me.seq,
                activity_type: A::TYPE.to_string(),
                input: me.input.clone(),
                retry: me.retry.clone(),
            });
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, CommandResult, Context, Info};
    use activity::{Definition, Error, Execution};

    struct Add;
    #[async_trait::async_trait]
    impl Definition for Add {
        type Input = (i64, i64);
        type Output = i64;
        const TYPE: &'static str = "Add";
        async fn run(_c: activity::Context, i: (i64, i64)) -> Result<i64, Error> {
            Ok(i.0 + i.1)
        }
    }

    fn ctx() -> Context {
        Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "T".into(),
        })
    }

    fn poll<A: Definition>(f: &mut ActivityFuture<A>) -> Poll<Result<A::Output, Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        // ActivityFuture is Unpin (no self-referential fields).
        Pin::new(f).poll(&mut tcx)
    }

    #[test]
    fn first_poll_emits_one_schedule_command_then_pends() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(matches!(&cmds[0],
            Command::ScheduleActivity { seq: 0, activity_type, .. } if activity_type == "Add"));
    }

    #[test]
    fn re_poll_does_not_duplicate_the_command() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        let _ = ctx.drain_commands();
        assert!(poll(&mut f).is_pending());
        assert!(ctx.drain_commands().is_empty(), "in-flight seq must not re-emit");
    }

    #[test]
    fn resolves_to_typed_output_when_result_applied() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        ctx.apply_result(0, CommandResult::ActivityCompleted(serde_json::to_vec(&5i64).unwrap()));
        match poll(&mut f) {
            Poll::Ready(Ok(v)) => assert_eq!(v, 5),
            other => panic!("expected Ready(Ok(5)), got {other:?}"),
        }
    }

    #[test]
    fn surfaces_activity_failure() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        ctx.apply_result(0, CommandResult::ActivityFailed(Error::fatal("nope")));
        match poll(&mut f) {
            Poll::Ready(Err(e)) => assert_eq!(e.message, "nope"),
            other => panic!("expected Ready(Err), got {other:?}"),
        }
    }
}
```

- [ ] **Step 3: Wire `future` into the crate**

Add to `crates/workflow/src/lib.rs`:

```rust
mod future;
pub use future::ActivityFuture;
```

- [ ] **Step 4: Run the future + context tests**

Run: `cargo test -p workflow future`
Then: `cargo test -p workflow context`
Expected: all PASS (the Task-1 context test now compiles and passes too).

- [ ] **Step 5: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): replay-aware Context + ActivityFuture<A>"
```

---

### Task 3: `WorkflowState` harness (step a whole workflow)

**Files:**
- Create: `crates/workflow/src/state.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Create `state.rs`**

```rust
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use crate::{Command, CommandResult, Context, Info};

/// A live workflow future plus its shared `Context`. The driver (chunk 1d) holds
/// one of these per cached run; here it is also the unit-test harness.
pub struct WorkflowState {
    ctx: Context,
    // Output is the JSON-encoded workflow result. !Send by construction.
    main: Pin<Box<dyn Future<Output = Result<Vec<u8>, crate::Error>>>>,
}

impl WorkflowState {
    /// Build a fresh future for workflow `W` with typed input.
    pub fn start<W: crate::Definition>(info: Info, input: W::Input) -> Self {
        let ctx = Context::new(info);
        let run_ctx = ctx.clone();
        let main = Box::pin(async move {
            let out = W::run(run_ctx, input).await?;
            serde_json::to_vec(&out).map_err(|e| crate::Error::new(e.to_string()))
        });
        Self { ctx, main }
    }

    pub fn context(&self) -> &Context {
        &self.ctx
    }

    /// Poll the workflow once with a no-op waker (spec §4.4 single poll/turn).
    pub fn poll_turn(&mut self) -> Poll<Result<Vec<u8>, crate::Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        self.main.as_mut().poll(&mut tcx)
    }

    pub fn drain_commands(&self) -> Vec<Command> {
        self.ctx.drain_commands()
    }

    pub fn apply_result(&self, seq: u64, result: CommandResult) {
        self.ctx.apply_result(seq, result);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Error};
    use activity::Execution;

    // Workflow: b = Add(Add(1,2), 10) == 13, via two sequential activities.
    struct Sum;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Sum {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Sum";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let a = ctx.activity::<Add>((1, 2)).await?;
            let b = ctx.activity::<Add>((a, 10)).await?;
            Ok(b)
        }
    }

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

    fn info() -> Info {
        Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "Sum".into(),
        }
    }

    #[test]
    fn drives_two_sequential_activities_to_completion() {
        let mut s = WorkflowState::start::<Sum>(info(), ());

        // Turn 1: schedules activity seq 0.
        assert!(s.poll_turn().is_pending());
        let c0 = s.drain_commands();
        assert!(matches!(&c0[0], Command::ScheduleActivity { seq: 0, .. }));

        // Feed result of seq 0 (=3), one event this turn.
        s.apply_result(0, CommandResult::ActivityCompleted(serde_json::to_vec(&3i64).unwrap()));
        assert!(s.poll_turn().is_pending());
        let c1 = s.drain_commands();
        assert!(matches!(&c1[0], Command::ScheduleActivity { seq: 1, .. }));

        // Feed result of seq 1 (=13).
        s.apply_result(1, CommandResult::ActivityCompleted(serde_json::to_vec(&13i64).unwrap()));
        match s.poll_turn() {
            Poll::Ready(Ok(bytes)) => {
                let out: i64 = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(out, 13);
            }
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }
}
```

- [ ] **Step 2: Wire into the crate**

Add to `crates/workflow/src/lib.rs`:

```rust
mod state;
pub use state::WorkflowState;
```

- [ ] **Step 3: Run the test**

Run: `cargo test -p workflow state`
Expected: `drives_two_sequential_activities_to_completion ... ok`.

- [ ] **Step 4: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): WorkflowState harness to step a workflow"
```

---

### Task 4: `cold_replay` + nondeterminism detection

**Files:**
- Create: `crates/workflow/src/replay.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Create `replay.rs`**

```rust
use std::collections::HashMap;
use std::task::Poll;

use crate::{Command, CommandResult, Event, Info, WorkflowState};

/// Result of replaying a full history: the command stream the workflow produced
/// and, if it completed within the history, its JSON-encoded output.
#[derive(Debug, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub commands: Vec<Command>,
    /// Some(Ok) = completed with output, Some(Err) = workflow returned an error
    /// (Failed), None = still running (history ended mid-flight). A *Failed*
    /// workflow is NOT nondeterminism — only schedule mismatches are.
    pub completion: Option<Result<Vec<u8>, crate::Error>>,
}

/// Replay diverged from recorded history (spec §12, Invariant 9).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("nondeterminism at seq {seq}: {detail}")]
pub struct Nondeterminism {
    pub seq: u64,
    pub detail: String,
}

/// Re-execute workflow `W` from `history`, replaying recorded outcomes one per
/// turn (spec §3, §4.1). Used by the driver's cold path and the equivalence
/// guard (chunk 5a).
pub fn cold_replay<W: crate::Definition>(
    info: Info,
    history: &[Event],
) -> Result<ReplayOutcome, Nondeterminism> {
    // 1. Recover input from the first event.
    let input_bytes = match history.first() {
        Some(Event::WorkflowStarted { input }) => input.clone(),
        _ => return Err(Nondeterminism { seq: 0, detail: "history must start with WorkflowStarted".into() }),
    };
    let input: W::Input = serde_json::from_slice(&input_bytes)
        .map_err(|e| Nondeterminism { seq: 0, detail: format!("input deserialize: {e}") })?;

    // 2. Index recorded schedules (for the divergence check) and ordered results.
    let mut recorded_sched: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
    let mut results: Vec<(u64, CommandResult)> = Vec::new();
    for ev in history {
        match ev {
            Event::ActivityScheduled { seq, activity_type, input, .. } => {
                recorded_sched.insert(*seq, (activity_type.clone(), input.clone()));
            }
            Event::ActivityCompleted { seq, output } => {
                results.push((*seq, CommandResult::ActivityCompleted(output.clone())));
            }
            Event::ActivityFailed { seq, error } => {
                results.push((*seq, CommandResult::ActivityFailed(error.clone())));
            }
            Event::WorkflowStarted { .. } => {}
        }
    }

    // 3. Drive the workflow, applying one result per turn.
    let mut state = WorkflowState::start::<W>(info, input);
    let mut commands = Vec::new();
    let mut cursor = 0usize;
    loop {
        let poll = state.poll_turn();
        for cmd in state.drain_commands() {
            let Command::ScheduleActivity { seq, activity_type, input, .. } = &cmd;
            if let Some((rty, rin)) = recorded_sched.get(seq) {
                if rty != activity_type || rin != input {
                    return Err(Nondeterminism {
                        seq: *seq,
                        detail: format!("history recorded schedule of {rty}, workflow emitted {activity_type}"),
                    });
                }
            }
            commands.push(cmd);
        }
        match poll {
            Poll::Ready(result) => {
                return Ok(ReplayOutcome { commands, completion: Some(result) });
            }
            Poll::Pending => {
                if cursor < results.len() {
                    let (seq, r) = results[cursor].clone();
                    state.apply_result(seq, r);
                    cursor += 1;
                } else {
                    return Ok(ReplayOutcome { commands, completion: None });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Error, RetryPolicy};
    use activity::Execution;

    struct Sum;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Sum {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Sum";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let a = ctx.activity::<Add>((1, 2)).await?;
            let b = ctx.activity::<Add>((a, 10)).await?;
            Ok(b)
        }
    }
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

    fn info() -> Info {
        Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "Sum".into(),
        }
    }

    fn add_input(a: i64, b: i64) -> Vec<u8> {
        serde_json::to_vec(&(a, b)).unwrap()
    }

    fn full_history() -> Vec<Event> {
        vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::ActivityScheduled { seq: 0, activity_type: "Add".into(), input: add_input(1, 2), retry: RetryPolicy::none() },
            Event::ActivityCompleted { seq: 0, output: serde_json::to_vec(&3i64).unwrap() },
            Event::ActivityScheduled { seq: 1, activity_type: "Add".into(), input: add_input(3, 10), retry: RetryPolicy::none() },
            Event::ActivityCompleted { seq: 1, output: serde_json::to_vec(&13i64).unwrap() },
        ]
    }

    #[test]
    fn replays_full_history_to_same_output_and_commands() {
        let outcome = cold_replay::<Sum>(info(), &full_history()).unwrap();
        let bytes = outcome.completion.unwrap().unwrap();
        let out: i64 = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out, 13);
        assert_eq!(outcome.commands.len(), 2);
        assert!(matches!(&outcome.commands[0], Command::ScheduleActivity { seq: 0, .. }));
        assert!(matches!(&outcome.commands[1], Command::ScheduleActivity { seq: 1, .. }));
    }

    #[test]
    fn detects_divergent_activity_type() {
        // History claims seq 0 scheduled "Charge", but Sum schedules "Add".
        let mut h = full_history();
        h[1] = Event::ActivityScheduled { seq: 0, activity_type: "Charge".into(), input: add_input(1, 2), retry: RetryPolicy::none() };
        let err = cold_replay::<Sum>(info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(err.detail.contains("Charge"));
    }
}
```

- [ ] **Step 2: Wire into the crate**

Add to `crates/workflow/src/lib.rs`:

```rust
mod replay;
pub use replay::{cold_replay, Nondeterminism, ReplayOutcome};
```

- [ ] **Step 3: Run the tests**

Run: `cargo test -p workflow replay`
Expected: `replays_full_history_to_same_output_and_commands ... ok` and
`detects_divergent_activity_type ... ok`.

- [ ] **Step 4: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): cold_replay with nondeterminism detection"
```

---

### Task 5: Whole-crate green + clippy

- [ ] **Step 1: Test the crate**

Run: `cargo test -p workflow`
Expected: all tests across context/future/state/replay/event/etc. PASS.

- [ ] **Step 2: Lint**

Run: `cargo clippy -p workflow --all-targets -- -D warnings`
Expected: clean. The single-variant `let Command::ScheduleActivity { .. } = &cmd`
may trigger `clippy::infallible_destructuring_match` once more variants exist;
for now it is fine. Fix any unused imports.

- [ ] **Step 3: Commit (if needed)**

```bash
git add -A
git commit -m "chore: clippy-clean pass 1b"
```

---

## Notes for chunk 1d (driver)

- The driver holds a `WorkflowState` per cached run on a **single thread** (the
  future is `!Send`); use a dedicated thread or `tokio::task::LocalSet`.
- The driver's live loop mirrors `cold_replay`'s loop but sources results from
  the `TaskQueue`/history instead of a pre-built vec, and **persists** each new
  `ScheduleActivity` command as an `ActivityScheduled` event, running the same
  divergence check against already-recorded schedules.
- `ctx.now()` / `ctx.random()` are deferred (they need recorded marker events);
  add them in a later pass before any workflow needs wall-clock/RNG.

## Self-Review (completed during authoring)

- **Spec coverage:** §3 (seq at creation, command-once, replay), §4.1
  (one-result-per-turn loop in `cold_replay`), §12 Inv 2/3/4/9 (seq, one-per-turn,
  emit-once, divergence) — all have tasks/tests. Persistence (§11) and the live
  driver (§5) are 1c/1d.
- **Placeholders:** none, except the intentional `run_id_str()` line in Task 1
  Step 2, which is called out with its fix.
- **Type consistency:** `Context::{new,info,activity,apply_result,drain_commands}`,
  `ActivityFuture<A>`, `WorkflowState::{start,poll_turn,drain_commands,apply_result}`,
  `cold_replay`, `ReplayOutcome`, `Nondeterminism` are used consistently here and
  referenced by the 1d notes.
