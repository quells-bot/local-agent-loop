# Pass 3a — Inbound-event pipeline + signal channel — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the replay-pure half of signals — the generic **inbound-event** pipeline: a
`SignalReceived` history event (carrying **no `seq`**), a per-name signal buffer on
`workflow::Context`, the idempotent-by-name `ctx.signal_channel::<T>(name)` whose
`recv()` pops one buffered payload, `cold_replay` support for applying signals one
per turn, and the `persist` encoding for the new event kind — so a workflow can
block on `recv()` and resume deterministically on replay (spec §6.1–6.3, §11, §12).

**Architecture:** A signal is an **inbound event**, not a workflow-issued command: it
is appended to history by a host entrypoint (Pass 3b), carries no `seq`, and is
**exempt from the divergence check** (Invariant 9 only compares emitted commands).
Applying a `SignalReceived { name, payload }` pushes the payload onto a per-name
`VecDeque` buffer in `ContextInner` (`signals: HashMap<String, VecDeque<Vec<u8>>>`).
`ctx.signal_channel::<T>(name)` returns a cheap handle keyed by name (allocating no
command, no `seq`); its `recv()` future resolves by popping the front of that buffer
and deserializing to `T`, parking while empty. Because history replays in
`event_id` order, the per-name buffer is rebuilt identically every replay, so the
Nth `recv()` of a name deterministically observes the Nth buffered signal — no `seq`
needed (Invariant 10). This mirrors the activity/timer pattern (`apply_result` /
`apply_timer_fired`) with a new `apply_signal`, except inbound events have no `seq`
and no command. **Host delivery (`engine.signal_workflow`), `SignalError`, and the
signal-or-timeout acceptance tests are Pass 3b** — this chunk is proven entirely by
the pure `cold_replay` tests inside the `workflow` crate plus a `persist` encode test.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow.

**Depends on:** Passes 1 and 2 (chunks 1a–2c), all merged on `main`.

---

## Canonical type additions (update the ROADMAP "Canonical types" list)

```rust
// crate `workflow`
pub enum Event {
    WorkflowStarted   { input: Vec<u8> },
    ActivityScheduled { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    ActivityCompleted { seq: u64, output: Vec<u8> },
    ActivityFailed    { seq: u64, error: activity::Error },
    TimerStarted      { seq: u64, duration_ms: u64 },
    TimerFired        { seq: u64 },
    SignalReceived    { name: String, payload: Vec<u8> },   // NEW (Pass 3a) — inbound, no seq
}

// workflow::Context gains:
//   fn signal_channel<T>(&self, name: &str) -> SignalChannel<T>   // idempotent by name, §6.3
//   fn apply_signal(&self, name: String, payload: Vec<u8>)        // driver/replay applies one/turn

// ContextInner gains:
//   signals: RefCell<HashMap<String, VecDeque<Vec<u8>>>>          // name -> buffered payloads

pub struct SignalChannel<T> { /* Rc<ContextInner> + name; no seq, no command */ }   // NEW
pub struct SignalRecv<T>    { /* Future<Output = Result<T, workflow::Error>> */ }    // NEW
// WorkflowState::apply_signal(name, payload) — passthrough to ctx (like apply_result).
```

**No `Command` variant** is added — signals are inbound, not workflow-issued (the
ROADMAP's `Command` comment already says "Pass 3 adds nothing (signals are
inbound)"). `CommandResult` is also unchanged: signals resolve through the `signals`
buffer, not the `results` map.

---

## File Structure

```
/crates/workflow/src/event.rs    # MODIFY: add Event::SignalReceived + kind()
/crates/workflow/src/context.rs   # MODIFY: signals buffer field, signal_channel(), apply_signal()
/crates/workflow/src/signal.rs    # NEW: SignalChannel<T> + SignalRecv<T>
/crates/workflow/src/state.rs      # MODIFY: WorkflowState::apply_signal
/crates/workflow/src/replay.rs     # MODIFY: apply SignalReceived one-per-turn (Applied::Signal)
/crates/workflow/src/lib.rs        # MODIFY: declare/export signal module
/crates/persist/src/history_impl.rs# MODIFY: encode() handles SignalReceived (seq = NULL)
```

> **Build-order note:** Task 1 adds `Event::SignalReceived`, which makes the
> exhaustive `match` in `persist`'s `encode()` non-exhaustive — so **after Task 1 the
> `persist` crate does not compile**. The `engine` crate keeps compiling (its only
> `Event` matches use a `_` wildcard). Task 1 therefore verifies with
> `cargo test -p workflow` only; Task 2 makes the workspace green again.

---

### Task 1: Workflow-crate signal slice (event + buffer + channel + replay)

One cohesive change to the `workflow` crate: the new inbound event, the per-name
buffer, the `SignalChannel`/`SignalRecv` futures, and `cold_replay` support land
together because adding the `Event` variant forces the `kind()` and `cold_replay`
matches to update.

**Files:**
- Modify: `crates/workflow/src/event.rs`, `context.rs`, `state.rs`, `replay.rs`, `lib.rs`
- Create: `crates/workflow/src/signal.rs`

- [ ] **Step 1: Write the failing pure replay tests**

Append to `crates/workflow/src/replay.rs` `mod tests` (after the existing tests, inside the `mod tests { ... }` block):

```rust
    // --- Pass 3a: signals -------------------------------------------------

    // Workflow that blocks on a single signal, returning its bool payload.
    struct WaitApprove;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for WaitApprove {
        type Input = ();
        type Output = bool;
        const TYPE: &'static str = "WaitApprove";
        async fn run(ctx: Context, _i: ()) -> Result<bool, Error> {
            let approvals = ctx.signal_channel::<bool>("approve");
            let v = approvals.recv().await?;
            Ok(v)
        }
    }

    fn wait_info() -> Info {
        Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "WaitApprove".into(),
        }
    }

    #[test]
    fn replays_signal_received() {
        let h = vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::SignalReceived {
                name: "approve".into(),
                payload: serde_json::to_vec(&true).unwrap(),
            },
        ];
        let outcome = cold_replay::<WaitApprove>(wait_info(), &h).unwrap();
        let out: bool = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert!(out);
        assert!(
            outcome.commands.is_empty(),
            "signals are inbound: they allocate no command and no seq"
        );
    }

    #[test]
    fn signal_for_other_name_leaves_recv_pending() {
        // A signal for a DIFFERENT name must not resolve the "approve" recv.
        let h = vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::SignalReceived {
                name: "other".into(),
                payload: serde_json::to_vec(&true).unwrap(),
            },
        ];
        let outcome = cold_replay::<WaitApprove>(wait_info(), &h).unwrap();
        assert!(
            outcome.completion.is_none(),
            "a signal for a different name does not wake recv()"
        );
    }

    // Workflow that receives TWO signals of the same name, in order.
    struct TwoRecv;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for TwoRecv {
        type Input = ();
        type Output = (i64, i64);
        const TYPE: &'static str = "TwoRecv";
        async fn run(ctx: Context, _i: ()) -> Result<(i64, i64), Error> {
            let ch = ctx.signal_channel::<i64>("n");
            let a = ch.recv().await?;
            let b = ch.recv().await?;
            Ok((a, b))
        }
    }

    #[test]
    fn two_signals_resolve_in_order_one_per_turn() {
        let info = Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "TwoRecv".into(),
        };
        let h = vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::SignalReceived { name: "n".into(), payload: serde_json::to_vec(&1i64).unwrap() },
            Event::SignalReceived { name: "n".into(), payload: serde_json::to_vec(&2i64).unwrap() },
        ];
        let outcome = cold_replay::<TwoRecv>(info, &h).unwrap();
        let out: (i64, i64) =
            serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, (1, 2), "the Nth recv pops the Nth buffered signal of that name");
    }
```

Run: `cargo test -p workflow --lib replay`
Expected: FAIL — `ctx.signal_channel`, `Event::SignalReceived` do not exist yet (compile errors).

- [ ] **Step 2: Add `Event::SignalReceived` + `kind()`**

In `crates/workflow/src/event.rs`, add the variant to the enum (after `TimerFired`):

```rust
    TimerFired {
        seq: u64,
    },
    /// Inbound event (spec §6): externally-injected, carries NO `seq`. Its payload
    /// is recorded once and replayed in `event_id` order (Invariant 10).
    SignalReceived {
        name: String,
        payload: Vec<u8>,
    },
```

And add the `kind()` arm (after the `TimerFired` arm):

```rust
            Event::TimerFired { .. } => "TimerFired",
            Event::SignalReceived { .. } => "SignalReceived",
```

- [ ] **Step 3: Add the per-name buffer + signal methods to `Context`**

In `crates/workflow/src/context.rs`, extend the `use` for collections at the top:

```rust
use std::collections::{HashMap, HashSet, VecDeque};
```

Add the field to `ContextInner` (after `new_spawns`):

```rust
    // Futures spawned this turn, awaiting absorption by WorkflowState (spec §4.4).
    pub(crate) new_spawns: RefCell<Vec<Pin<Box<dyn Future<Output = ()>>>>>,
    // Inbound signal payloads, buffered per name (spec §6.2). Rebuilt identically on
    // every replay because history replays in event_id order (Invariant 10).
    pub(crate) signals: RefCell<HashMap<String, VecDeque<Vec<u8>>>>,
```

Initialize it in `Context::new` (after `new_spawns: RefCell::new(Vec::new()),`):

```rust
                new_spawns: RefCell::new(Vec::new()),
                signals: RefCell::new(HashMap::new()),
```

Add the methods to `impl Context` (after `spawn`, before `drain_new_spawns`):

```rust
    /// Get the signal channel for `name` (the `workflow.GetSignalChannel` analog,
    /// spec §6.3). Idempotent by name: every call returns a handle onto the same
    /// per-name buffer. Allocates no command and consumes no `seq`.
    pub fn signal_channel<T>(&self, name: &str) -> crate::SignalChannel<T> {
        crate::SignalChannel::new(self.inner.clone(), name.to_string())
    }

    /// Driver/replay applies one recorded inbound signal before a poll (one event per
    /// turn, spec §4.1/§6.2): push its payload onto the per-name buffer.
    pub fn apply_signal(&self, name: String, payload: Vec<u8>) {
        self.inner
            .signals
            .borrow_mut()
            .entry(name)
            .or_default()
            .push_back(payload);
    }
```

- [ ] **Step 4: Create the `SignalChannel` / `SignalRecv` futures**

Create `crates/workflow/src/signal.rs`:

```rust
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

use serde::de::DeserializeOwned;

use crate::context::ContextInner;

/// Idempotent-by-name signal channel (the `workflow.GetSignalChannel` analog,
/// spec §6.3). Holds no buffer of its own — it reads/writes the shared per-name
/// buffer in `ContextInner`, so two channels for the same name are the same logical
/// channel. Allocates no command and consumes no `seq`.
pub struct SignalChannel<T> {
    inner: Rc<ContextInner>,
    name: String,
    // `fn() -> T` keeps the handle `Send`-agnostic and `Unpin` regardless of `T`;
    // `T` is only a type tag here, never stored.
    _marker: PhantomData<fn() -> T>,
}

impl<T> SignalChannel<T> {
    pub(crate) fn new(inner: Rc<ContextInner>, name: String) -> Self {
        Self {
            inner,
            name,
            _marker: PhantomData,
        }
    }

    /// Await one buffered signal of this name (`ReceiveChannel.Receive` analog).
    /// Resolves by popping the front of the per-name buffer; the Nth `recv()`
    /// deterministically pops the Nth buffered signal, so it is replay-stable
    /// without a `seq` (spec §6.3).
    pub fn recv(&self) -> SignalRecv<T> {
        SignalRecv {
            inner: self.inner.clone(),
            name: self.name.clone(),
            _marker: PhantomData,
        }
    }
}

/// Future returned by [`SignalChannel::recv`]. Pops one payload off the per-name
/// buffer and deserializes it to `T`; parks (Pending) while the buffer is empty.
pub struct SignalRecv<T> {
    inner: Rc<ContextInner>,
    name: String,
    _marker: PhantomData<fn() -> T>,
}

impl<T: DeserializeOwned> Future for SignalRecv<T> {
    type Output = Result<T, crate::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        // SignalRecv is Unpin (Rc/String/PhantomData hold no self-referential data).
        let me = self.get_mut();
        let popped = me
            .inner
            .signals
            .borrow_mut()
            .get_mut(&me.name)
            .and_then(|buf| buf.pop_front());
        match popped {
            Some(bytes) => Poll::Ready(serde_json::from_slice::<T>(&bytes).map_err(|e| {
                crate::Error::new(format!("signal '{}' deserialize: {e}", me.name))
            })),
            None => Poll::Pending,
        }
    }
}
```

- [ ] **Step 5: Add `WorkflowState::apply_signal`**

In `crates/workflow/src/state.rs`, add to `impl WorkflowState` (after `apply_timer_fired`):

```rust
    pub fn apply_signal(&self, name: String, payload: Vec<u8>) {
        self.ctx.apply_signal(name, payload);
    }
```

- [ ] **Step 6: Apply signals one-per-turn in `cold_replay`**

In `crates/workflow/src/replay.rs`, the `enum Applied { ... }` inside `cold_replay`
gains a `Signal` variant. Replace the `enum Applied` declaration:

```rust
    enum Applied {
        Result(u64, CommandResult),
        Timer(u64),
        Signal(String, Vec<u8>),
    }
```

In the `for ev in history { match ev { ... } }` block, add an arm for the new event
(after the `Event::TimerFired { seq }` arm, before `Event::WorkflowStarted { .. }`):

```rust
            Event::TimerFired { seq } => {
                applied.push(Applied::Timer(*seq));
            }
            Event::SignalReceived { name, payload } => {
                applied.push(Applied::Signal(name.clone(), payload.clone()));
            }
            Event::WorkflowStarted { .. } => {}
```

In the drive loop's `Poll::Pending` arm, extend the `match &applied[cursor]` to
apply signals:

```rust
                if cursor < applied.len() {
                    match &applied[cursor] {
                        Applied::Result(seq, r) => state.apply_result(*seq, r.clone()),
                        Applied::Timer(seq) => state.apply_timer_fired(*seq),
                        Applied::Signal(name, payload) => {
                            state.apply_signal(name.clone(), payload.clone())
                        }
                    }
                    cursor += 1;
                } else {
```

- [ ] **Step 7: Declare + export the `signal` module**

In `crates/workflow/src/lib.rs`, after the `spawn` module block, add:

```rust
mod spawn;
pub use spawn::SpawnHandle;

mod signal;
pub use signal::{SignalChannel, SignalRecv};
```

- [ ] **Step 8: Run the workflow-crate tests**

Run: `cargo test -p workflow`
Expected: PASS — the three new `replay` tests plus all Pass 1/2 tests still green.

(The `persist` crate will NOT build yet — expected per the build-order note; Task 2
fixes it. `engine` still builds.)

- [ ] **Step 9: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): signals — SignalReceived event, per-name buffer, signal_channel/recv, replay"
```

---

### Task 2: Persist — encode the inbound event (seq = NULL)

**Files:**
- Modify: `crates/persist/src/history_impl.rs`

- [ ] **Step 1: Write the failing encode round-trip test**

Add to `crates/persist/src/history_impl.rs` `mod tests` (after the last test):

```rust
    #[tokio::test]
    async fn commit_turn_round_trips_signal_received_with_null_seq() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();

        let commit = TurnCommit {
            events: vec![Event::SignalReceived {
                name: "approve".into(),
                payload: b"true".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();

        let h = db.read_history("run-1").await.unwrap();
        match &h.last().unwrap().event {
            Event::SignalReceived { name, payload } => {
                assert_eq!(name, "approve");
                assert_eq!(payload, b"true");
            }
            other => panic!("expected SignalReceived, got {other:?}"),
        }

        // Inbound events carry no `seq`: the history.seq column must be NULL.
        let seq: Option<i64> = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT seq FROM history WHERE run_id = 'run-1' ORDER BY event_id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(seq, None, "inbound events carry no seq (spec §6, §11)");
    }
```

Run: `cargo test -p persist --lib commit_turn_round_trips_signal_received`
Expected: FAIL — `persist` does not compile because `encode()`'s match is not
exhaustive over `Event` (missing `SignalReceived`).

- [ ] **Step 2: Teach `encode()` the inbound event**

In `crates/persist/src/history_impl.rs`, update the `encode` `seq` match to map
`SignalReceived` to `None` (inbound events have no `seq`). Replace the match:

```rust
fn encode(event: &Event) -> (Option<i64>, &'static str, Vec<u8>) {
    let seq = match event {
        Event::ActivityScheduled { seq, .. }
        | Event::ActivityCompleted { seq, .. }
        | Event::ActivityFailed { seq, .. }
        | Event::TimerStarted { seq, .. }
        | Event::TimerFired { seq } => Some(*seq as i64),
        // Inbound events (spec §6) carry no seq; WfStarted predates any command.
        Event::WorkflowStarted { .. } | Event::SignalReceived { .. } => None,
    };
    let payload = serde_json::to_vec(event).expect("event serializes");
    (seq, event.kind(), payload)
}
```

- [ ] **Step 3: Run persist tests + whole-workspace build**

Run: `cargo test -p persist`
Expected: PASS — the new signal round-trip test plus all Pass 1/2 persist tests.

Run: `cargo build`
Expected: the whole workspace compiles green again.

- [ ] **Step 4: Commit**

```bash
git add crates/persist
git commit -m "feat(persist): encode SignalReceived inbound events with NULL seq"
```

---

### Task 3: Whole-workspace green + clippy + roadmap update

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS (Pass 1/2 integration
  tests still green; none schedule signals, so they are unaffected).
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean.
- [ ] **Step 3:** In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
  - In the chunk table, set chunk `3a` status to `done` and its Plan file to
    `2026-06-14-pass-3a-inbound-events-and-signal-channel.md`.
  - In the "Canonical types" section, update the `workflow` `Event` enum to include
    `SignalReceived { name: String, payload: Vec<u8> }` (and drop the
    `// Pass 3: SignalReceived` reservation comment, now realized), add the
    `signals` field note to `ContextInner` if listed, and add `signal_channel` /
    `apply_signal` to the `Context` method list plus `SignalChannel`/`SignalRecv`.
- [ ] **Step 4:** Commit:

```bash
git add -A
git commit -m "chore: pass 3a complete — inbound-event pipeline + signal channel, roadmap + canonical types updated"
```

---

## Notes for Pass 3b

- **Host delivery is next.** 3b adds `engine.signal_workflow(workflow_id, name,
  &payload)` / `handle.signal(...)` returning a typed `SignalError`, backed by a new
  `History::append_signal` trait method whose `persist` impl appends a
  `SignalReceived` row (seq NULL) and marks the run runnable in **one transaction**
  (the durable-before-return boundary, spec §6.1). The encode path built here is what
  that transaction reuses.
- **Determinism is already proven** by the pure `cold_replay` tests in Task 1; 3b's
  integration tests exercise the live driver + the signal-or-timeout `select_biased!`
  composition end-to-end (including before-a-crash delivery and `NotRunning`).
- **`recv()` resolves to `Result<T, workflow::Error>`** so a malformed payload
  surfaces as a workflow error (the spec's §6.3 `proceed(a?)` shape); the `?` in a
  workflow body that returns `Result<_, workflow::Error>` propagates it.

## Self-Review (completed during authoring)

- **Spec coverage:** §6.1 (inbound event appended to history, no `seq`), §6.2
  (`SignalReceived` consumed one-per-turn → per-name buffer; exempt from the
  command-divergence check), §6.3 (`signal_channel` idempotent by name, `recv()`
  pops front, replay-stable without `seq`), §11 (`SignalReceived` in `history.kind`,
  `seq` NULL), §12 Invariants 3/9/10 (one event per turn; inbound exempt from
  divergence; recorded-once-and-replayed). Host delivery + the §13 Pass-3 acceptance
  gate are deliberately deferred to 3b.
- **Placeholders:** none — full code, exact commands, expected outcomes throughout.
- **Type consistency:** `Event::SignalReceived { name: String, payload: Vec<u8> }`,
  `Context::signal_channel<T>(&self, &str) -> SignalChannel<T>`,
  `Context::apply_signal(&self, String, Vec<u8>)`,
  `WorkflowState::apply_signal(&self, String, Vec<u8>)`,
  `SignalChannel::recv(&self) -> SignalRecv<T>`,
  `SignalRecv<T>: Future<Output = Result<T, workflow::Error>>`, and
  `encode()`'s `SignalReceived => None` are used identically across the workflow and
  persist tasks. No `Command` variant and no `CommandResult` change, by design.
</content>
</invoke>
