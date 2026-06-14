# Pass 2a — Timers (`ctx.sleep` / `ctx.timer` + timer service) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add durable timers — `ctx.sleep(dur)` / `ctx.timer(dur)` in workflow code, a
`StartTimer` command, `TimerStarted` / `TimerFired` history events, the `timers`
queue table wiring, and a timer-service loop that fires due timers — so a workflow
can block on time and recover correctly across a crash (spec §4, §5.3, §11).

**Architecture:** Timers mirror activities, minus retries and side effects. A
`TimerFuture` allocates a `seq` at creation time (Invariant 2) and emits
`Command::StartTimer { seq, duration_ms }` exactly once (the shared `scheduled`
set guards re-emits). The **duration** is the deterministic, replay-checked datum;
the engine converts it to an absolute `fire_at = now + duration` when it commits
the `TimerStarted` event and inserts a `timers` row. A separate timer-service loop
atomically pops a due `timers` row, appends `TimerFired { seq }`, and marks the run
runnable. On replay, the recorded `TimerStarted` is divergence-checked against the
re-emitted command and the recorded `TimerFired` resolves the future — exactly the
activity pattern, with a `fired: HashSet<u64>` instead of the `results` map (timers
carry no payload). This also folds in the Pass-1-review **observer double-fire**
guard, which becomes a real bug once Pass 2 introduces concurrency.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow.

**Depends on:** Pass 1 (chunks 1a–1d), all merged on `main`.

---

## Canonical type additions (update the ROADMAP "Canonical types" list)

```rust
// crate `workflow`
pub enum Command {
    ScheduleActivity { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    StartTimer { seq: u64, duration_ms: u64 },        // NEW (Pass 2a)
}

pub enum Event {
    WorkflowStarted   { input: Vec<u8> },
    ActivityScheduled { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    ActivityCompleted { seq: u64, output: Vec<u8> },
    ActivityFailed    { seq: u64, error: activity::Error },
    TimerStarted      { seq: u64, duration_ms: u64 }, // NEW (Pass 2a)
    TimerFired        { seq: u64 },                    // NEW (Pass 2a)
}

// workflow::Context gains:
//   fn timer(&self, dur: Duration) -> TimerFuture
//   fn sleep(&self, dur: Duration) -> TimerFuture   (alias of timer)
//   fn apply_timer_fired(&self, seq: u64)
// TimerFuture: Future<Output = ()>

// crate `engine`
pub struct NewTimer { pub seq: i64, pub fire_at: i64 }   // NEW (Pass 2a)
pub struct TurnCommit { /* …existing… */ pub new_timers: Vec<NewTimer> } // field added
// TaskQueue trait gains:  async fn fire_due_timer(&self) -> anyhow::Result<bool>;
```

`CommandResult` is **unchanged** — timers resolve through a dedicated `fired` set,
not the `results` map, so the activity resolution path and the
`From<CommandResult> for Result<Vec<u8>, activity::Error>` impl stay untouched.

---

## File Structure

```
/crates/workflow/src/command.rs    # MODIFY: add Command::StartTimer
/crates/workflow/src/event.rs       # MODIFY: add Event::TimerStarted / TimerFired + kind()
/crates/workflow/src/context.rs     # MODIFY: fired set, timer()/sleep()/apply_timer_fired()
/crates/workflow/src/future.rs       # MODIFY: add TimerFuture
/crates/workflow/src/state.rs        # MODIFY: WorkflowState::apply_timer_fired
/crates/workflow/src/replay.rs       # MODIFY: timer divergence check + unified apply stream
/crates/workflow/src/lib.rs          # MODIFY: export TimerFuture
/crates/engine/src/types.rs          # MODIFY: NewTimer + TurnCommit.new_timers
/crates/engine/src/traits.rs         # MODIFY: TaskQueue::fire_due_timer
/crates/engine/src/engine.rs         # MODIFY: driver StartTimer arm, observer guard, process_one_timer, start() loop
/crates/persist/src/history_impl.rs  # MODIFY: encode() timers, commit_turn inserts new_timers
/crates/persist/src/taskqueue_impl.rs# MODIFY: fire_due_timer impl
/crates/engine/tests/timers.rs       # NEW: Pass-2a integration tests (sleep + cold recovery)
```

> **Build-order note:** Task 1 adds `Command::StartTimer`, which turns the
> irrefutable `let Command::ScheduleActivity { .. } = cmd;` destructures in
> `replay.rs` (fixed in Task 1) and `engine.rs` (fixed in Task 2) into refutable
> patterns. Therefore **after Task 1 the `engine`/`persist` crates do not compile**;
> Task 1 verifies with `cargo test -p workflow` only. The workspace is green again
> at the end of Task 3.

---

### Task 1: Workflow-crate timer slice (protocol + future + replay)

This is one cohesive change to the `workflow` crate: the new command/events, the
`TimerFuture`, and replay support land together because adding the `Command`
variant forces every `match`/destructure over `Command` and `Event` to update.

**Files:**
- Modify: `crates/workflow/src/command.rs`, `event.rs`, `context.rs`, `future.rs`, `state.rs`, `replay.rs`, `lib.rs`

- [ ] **Step 1: Write the failing pure replay test**

Append to `crates/workflow/src/replay.rs` `mod tests` (after the existing tests):

```rust
    // Workflow that sleeps, then runs one activity. Exercises a timer interleaved
    // with an activity under the one-event-per-turn rule.
    struct Nap;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Nap {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Nap";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            ctx.sleep(std::time::Duration::from_millis(500)).await;
            let a = ctx.activity::<Add>((1, 2)).await?;
            Ok(a)
        }
    }

    fn nap_info() -> Info {
        Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "Nap".into(),
        }
    }

    #[test]
    fn replays_timer_then_activity() {
        let h = vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::TimerStarted { seq: 0, duration_ms: 500 },
            Event::TimerFired { seq: 0 },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted { seq: 1, output: serde_json::to_vec(&3i64).unwrap() },
        ];
        let outcome = cold_replay::<Nap>(nap_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 3);
        // First command is the timer (seq 0), then the activity (seq 1).
        assert!(matches!(&outcome.commands[0], Command::StartTimer { seq: 0, duration_ms: 500 }));
        assert!(matches!(&outcome.commands[1], Command::ScheduleActivity { seq: 1, .. }));
    }

    #[test]
    fn detects_divergent_timer_duration() {
        // History recorded a 500ms timer at seq 0; Nap emits 500ms, so mutate the
        // record to 999ms and expect a nondeterminism error at seq 0.
        let h = vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::TimerStarted { seq: 0, duration_ms: 999 },
            Event::TimerFired { seq: 0 },
        ];
        let err = cold_replay::<Nap>(nap_info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(err.detail.contains("timer"));
    }
```

Run: `cargo test -p workflow --lib replay`
Expected: FAIL — `ctx.sleep`, `Command::StartTimer`, `Event::TimerStarted`/`TimerFired` do not exist yet (compile errors).

- [ ] **Step 2: Add `Command::StartTimer`**

In `crates/workflow/src/command.rs`, replace the enum body:

```rust
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
}
```

- [ ] **Step 3: Add `Event::TimerStarted` / `TimerFired` + `kind()`**

In `crates/workflow/src/event.rs`, extend the enum and `kind()`:

```rust
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    WorkflowStarted { input: Vec<u8> },
    ActivityScheduled { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    ActivityCompleted { seq: u64, output: Vec<u8> },
    ActivityFailed { seq: u64, error: activity::Error },
    TimerStarted { seq: u64, duration_ms: u64 },
    TimerFired { seq: u64 },
}

impl Event {
    /// Discriminant string stored in `history.kind`.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::WorkflowStarted { .. } => "WorkflowStarted",
            Event::ActivityScheduled { .. } => "ActivityScheduled",
            Event::ActivityCompleted { .. } => "ActivityCompleted",
            Event::ActivityFailed { .. } => "ActivityFailed",
            Event::TimerStarted { .. } => "TimerStarted",
            Event::TimerFired { .. } => "TimerFired",
        }
    }
}
```

- [ ] **Step 4: Add the `fired` set + timer methods to `Context`**

In `crates/workflow/src/context.rs`, add the field to `ContextInner` and the
methods to `Context`. First the field (inside `ContextInner`, after `commands`):

```rust
    pub(crate) commands: RefCell<Vec<Command>>,               // emitted this turn
    pub(crate) fired: RefCell<HashSet<u64>>,                   // timer seqs fired (no payload)
```

Initialize it in `Context::new` (after `commands: RefCell::new(Vec::new()),`):

```rust
                commands: RefCell::new(Vec::new()),
                fired: RefCell::new(HashSet::new()),
```

Add the methods to `impl Context` (after `activity`), and the `Duration` import at
the top of the file (`use std::time::Duration;`):

```rust
    /// Start a timer. `seq` is allocated HERE (creation time, Invariant 2). The
    /// duration is the deterministic, replay-checked datum; the engine converts it
    /// to an absolute fire time when it commits the TimerStarted event (spec §5.3).
    pub fn timer(&self, dur: Duration) -> crate::future::TimerFuture {
        let seq = self.inner.next_seq.get();
        self.inner.next_seq.set(seq + 1);
        crate::future::TimerFuture::new(self.inner.clone(), seq, dur.as_millis() as u64)
    }

    /// `workflow.Sleep` analog — await a timer for `dur`.
    pub fn sleep(&self, dur: Duration) -> crate::future::TimerFuture {
        self.timer(dur)
    }

    /// Driver/replay applies a recorded TimerFired before a poll (one event/turn).
    pub fn apply_timer_fired(&self, seq: u64) {
        self.inner.fired.borrow_mut().insert(seq);
    }
```

- [ ] **Step 5: Add `TimerFuture`**

Append to `crates/workflow/src/future.rs`:

```rust
/// Awaitable handle for one timer. Resolves to `()` once its TimerFired event has
/// been applied. `seq` identifies it in history; the shared `scheduled` set means
/// it emits `StartTimer` exactly once across re-polls (spec §3, §5.3).
pub struct TimerFuture {
    inner: Rc<ContextInner>,
    seq: u64,
    duration_ms: u64,
}

impl TimerFuture {
    pub(crate) fn new(inner: Rc<ContextInner>, seq: u64, duration_ms: u64) -> Self {
        Self { inner, seq, duration_ms }
    }
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<()> {
        let me = self.get_mut();

        // 1. Replay path: this timer's TimerFired has been applied -> resolve.
        if me.inner.fired.borrow().contains(&me.seq) {
            return Poll::Ready(());
        }

        // 2. First arrival: emit StartTimer exactly once, then park (Invariant 4).
        if me.inner.scheduled.borrow_mut().insert(me.seq) {
            me.inner.commands.borrow_mut().push(Command::StartTimer {
                seq: me.seq,
                duration_ms: me.duration_ms,
            });
        }
        Poll::Pending
    }
}
```

- [ ] **Step 6: Add `WorkflowState::apply_timer_fired`**

In `crates/workflow/src/state.rs`, add to `impl WorkflowState` (after `apply_result`):

```rust
    pub fn apply_timer_fired(&self, seq: u64) {
        self.ctx.apply_timer_fired(seq);
    }
```

- [ ] **Step 7: Update `cold_replay` — timer divergence + unified apply stream**

Replace the body of `cold_replay` in `crates/workflow/src/replay.rs` from the
"// 2. Index recorded schedules" comment through the end of the `loop` with:

```rust
    // 2. Index recorded schedules (for divergence checks) and the ordered stream of
    //    things to apply one-per-turn (activity outcomes AND timer fires), in
    //    event_id order. Timers resolve with no payload, so they get their own
    //    apply variant rather than riding the CommandResult map.
    enum Applied {
        Result(u64, CommandResult),
        Timer(u64),
    }
    let mut recorded_sched: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
    let mut recorded_timer: HashMap<u64, u64> = HashMap::new(); // seq -> duration_ms
    let mut applied: Vec<Applied> = Vec::new();
    for ev in history {
        match ev {
            Event::ActivityScheduled { seq, activity_type, input, .. } => {
                recorded_sched.insert(*seq, (activity_type.clone(), input.clone()));
            }
            Event::ActivityCompleted { seq, output } => {
                applied.push(Applied::Result(*seq, CommandResult::ActivityCompleted(output.clone())));
            }
            Event::ActivityFailed { seq, error } => {
                applied.push(Applied::Result(*seq, CommandResult::ActivityFailed(error.clone())));
            }
            Event::TimerStarted { seq, duration_ms } => {
                recorded_timer.insert(*seq, *duration_ms);
            }
            Event::TimerFired { seq } => {
                applied.push(Applied::Timer(*seq));
            }
            Event::WorkflowStarted { .. } => {}
        }
    }

    // 3. Drive the workflow, applying one item per turn.
    let mut state = WorkflowState::start::<W>(info, input);
    let mut commands = Vec::new();
    let mut cursor = 0usize;
    loop {
        let poll = state.poll_turn();
        for cmd in state.drain_commands() {
            match &cmd {
                Command::ScheduleActivity { seq, activity_type, input, .. } => {
                    if let Some((rty, rin)) = recorded_sched.get(seq) {
                        if rty != activity_type || rin != input {
                            return Err(Nondeterminism {
                                seq: *seq,
                                detail: format!(
                                    "history recorded schedule of {rty}, workflow emitted {activity_type}"
                                ),
                            });
                        }
                    }
                }
                Command::StartTimer { seq, duration_ms } => {
                    if let Some(rdur) = recorded_timer.get(seq) {
                        if rdur != duration_ms {
                            return Err(Nondeterminism {
                                seq: *seq,
                                detail: format!(
                                    "history recorded timer of {rdur}ms, workflow emitted {duration_ms}ms"
                                ),
                            });
                        }
                    }
                }
            }
            commands.push(cmd);
        }
        match poll {
            Poll::Ready(result) => {
                return Ok(ReplayOutcome { commands, completion: Some(result) });
            }
            Poll::Pending => {
                if cursor < applied.len() {
                    match &applied[cursor] {
                        Applied::Result(seq, r) => state.apply_result(*seq, r.clone()),
                        Applied::Timer(seq) => state.apply_timer_fired(*seq),
                    }
                    cursor += 1;
                } else {
                    return Ok(ReplayOutcome { commands, completion: None });
                }
            }
        }
    }
```

- [ ] **Step 8: Export `TimerFuture`**

In `crates/workflow/src/lib.rs`, change the `future` re-export line:

```rust
mod future;
pub use future::{ActivityFuture, TimerFuture};
```

- [ ] **Step 9: Run the workflow-crate tests**

Run: `cargo test -p workflow`
Expected: PASS — including the new `replays_timer_then_activity` and
`detects_divergent_timer_duration`, and all Pass 1 tests still green.

(The `engine` and `persist` crates will NOT build yet — that is expected per the
build-order note; Tasks 2–3 fix them.)

- [ ] **Step 10: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): timers — StartTimer command, TimerStarted/Fired events, TimerFuture, replay"
```

---

### Task 2: Engine — driver timer branch, observer guard, timer service

**Files:**
- Modify: `crates/engine/src/types.rs`, `traits.rs`, `engine.rs`

- [ ] **Step 1: Add `NewTimer` + `TurnCommit.new_timers`**

In `crates/engine/src/types.rs`, add `NewTimer` (after `NewActivityTask`):

```rust
/// A timer to enqueue this turn. `fire_at` is the absolute epoch-ms deadline the
/// driver computes from the StartTimer command's duration (spec §5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTimer {
    pub seq: i64,
    pub fire_at: i64,
}
```

Add the field to `TurnCommit`:

```rust
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnCommit {
    pub events: Vec<Event>,            // new history events emitted this turn
    pub new_tasks: Vec<NewActivityTask>,
    pub new_timers: Vec<NewTimer>,     // timers to enqueue this turn
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>,       // Some iff status != Running
}
```

- [ ] **Step 2: Add `fire_due_timer` to the `TaskQueue` trait**

In `crates/engine/src/traits.rs`, add to the `TaskQueue` trait (after
`reschedule_activity`):

```rust
    /// Atomically fire one timer whose `fire_at <= now`: append `TimerFired`,
    /// delete the timer row, and mark the run runnable (spec §5.3). Returns false
    /// if no timer is due. Single combined method (no lease/retry — timers carry no
    /// side effect) so two service iterations cannot double-fire the same timer.
    async fn fire_due_timer(&self) -> anyhow::Result<bool>;
```

- [ ] **Step 3: Driver — match `Command`, emit timers, add the observer guard**

In `crates/engine/src/engine.rs`, in `process_one_runnable`:

(a) Extend the `recorded` set to also cover already-recorded timer seqs so a
re-emitted `StartTimer` is not persisted twice. Replace the `recorded` builder:

```rust
        let recorded: HashSet<u64> = events
            .iter()
            .filter_map(|e| match e {
                workflow::Event::ActivityScheduled { seq, .. }
                | workflow::Event::TimerStarted { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect();
```

(b) Add the terminal-state guard at the very top of `process_one_runnable`, right
after `load_run` resolves `meta` (folds in the Pass-1-review observer double-fire
fix — under Pass 2 a straggler `complete_activity` can re-mark an already-terminal
run runnable; without this guard the driver re-drives it and re-fires the observer):

```rust
        let meta = self
            .history
            .load_run(&run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("runnable run {run_id} has no execution row"))?;

        // Already terminal: a late completion re-marked it runnable. Clear the
        // runnable flag without re-driving or re-firing the observer (Inv 5).
        if meta.status != ExecStatus::Running {
            let commit = TurnCommit {
                events: Vec::new(),
                new_tasks: Vec::new(),
                new_timers: Vec::new(),
                status: meta.status,
                result: None,
            };
            self.history.commit_turn(&run_id, &commit).await?;
            return Ok(true);
        }
```

(Note: `commit_turn` overwrites `result` with `None` here, so the early-return path
must not clobber a stored result. To avoid that, pass the existing result through —
see step 3c.)

(c) `RunMeta` does not currently carry the stored result, so the guard above would
null it. Keep it correct by NOT touching `result` in this path: change the guard's
commit to reuse a dedicated clear. Replace the guard body with a direct runnable
clear via a new `History` call is overkill; instead make `commit_turn`'s status
write preserve result when `result` is `None` AND status is terminal would be
surprising. Simplest correct fix: have the guard read the stored result first.
Replace the guard with:

```rust
        if meta.status != ExecStatus::Running {
            // Preserve the stored result; only clear the stale runnable flag.
            let existing = self.history.find_execution(&meta.workflow_id).await?;
            let result = existing.and_then(|(_, _, r)| r);
            let commit = TurnCommit {
                events: Vec::new(),
                new_tasks: Vec::new(),
                new_timers: Vec::new(),
                status: meta.status,
                result,
            };
            self.history.commit_turn(&run_id, &commit).await?;
            return Ok(true);
        }
```

(d) Replace the command-diff loop (the `for cmd in &outcome.commands { … }` block)
with a `match` that handles both variants and builds `new_timers`:

```rust
        // Persist only commands not already recorded in history.
        let mut new_events = Vec::new();
        let mut new_tasks = Vec::new();
        let mut new_timers = Vec::new();
        for cmd in &outcome.commands {
            match cmd {
                workflow::Command::ScheduleActivity { seq, activity_type, input, retry } => {
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
                workflow::Command::StartTimer { seq, duration_ms } => {
                    if recorded.contains(seq) {
                        continue;
                    }
                    new_events.push(workflow::Event::TimerStarted {
                        seq: *seq,
                        duration_ms: *duration_ms,
                    });
                    new_timers.push(NewTimer {
                        seq: *seq as i64,
                        fire_at: now_ms() + *duration_ms as i64,
                    });
                }
            }
        }
```

(e) Add `new_timers` to the `TurnCommit` constructed lower down:

```rust
        let commit = TurnCommit { events: new_events, new_tasks, new_timers, status, result: result.clone() };
```

(f) Import `NewTimer` — update the `use crate::{…}` line at the top of `engine.rs`:

```rust
use crate::{ExecStatus, History, NewActivityTask, NewTimer, TaskQueue, TurnCommit};
```

- [ ] **Step 4: Add `process_one_timer` + the timer-service loop**

In `crates/engine/src/engine.rs`, add a new `impl Engine` block (next to the other
`process_one_*` methods):

```rust
impl Engine {
    /// Fire one due timer, if any (spec §5.3). Returns false if none was due.
    pub async fn process_one_timer(&self) -> anyhow::Result<bool> {
        self.queue.fire_due_timer().await
    }
}
```

In `start()`, add a third spawned loop after the worker loop (before `engine`):

```rust
        let timers = engine.clone();
        tokio::spawn(async move {
            loop {
                match timers.process_one_timer().await {
                    Ok(true) => {}
                    Ok(false) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(err) => {
                        eprintln!("timer error: {err:#}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });
```

- [ ] **Step 5: Build the engine crate**

Run: `cargo build -p engine`
Expected: compiles. (`persist` still red until Task 3 — do not run `cargo test`
across the workspace yet.)

- [ ] **Step 6: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): timer driver branch, fire_due_timer trait, observer double-fire guard"
```

---

### Task 3: Persist — encode timers, commit timers, fire due timers

**Files:**
- Modify: `crates/persist/src/history_impl.rs`, `taskqueue_impl.rs`

- [ ] **Step 1: Teach `encode()` the timer events**

In `crates/persist/src/history_impl.rs`, update the `encode` `seq` match to cover
the new variants:

```rust
fn encode(event: &Event) -> (Option<i64>, &'static str, Vec<u8>) {
    let seq = match event {
        Event::ActivityScheduled { seq, .. }
        | Event::ActivityCompleted { seq, .. }
        | Event::ActivityFailed { seq, .. }
        | Event::TimerStarted { seq, .. }
        | Event::TimerFired { seq } => Some(*seq as i64),
        Event::WorkflowStarted { .. } => None,
    };
    let payload = serde_json::to_vec(event).expect("event serializes");
    (seq, event.kind(), payload)
}
```

- [ ] **Step 2: Insert `new_timers` in `commit_turn`**

In `commit_turn`, after the `for task in &commit.new_tasks { … }` loop, add:

```rust
        for timer in &commit.new_timers {
            tx.execute(
                "INSERT OR REPLACE INTO timers (run_id, seq, fire_at) VALUES (?1, ?2, ?3)",
                params![run_id, timer.seq, timer.fire_at],
            )?;
        }
```

- [ ] **Step 3: Fix the existing `commit_turn` tests for the new field**

The two `TurnCommit { … }` literals in `history_impl.rs` `mod tests` now need
`new_timers: vec![]`. Add the field to each (in `commit_turn_appends_clears_runnable_and_sets_status`):

```rust
        let commit = TurnCommit {
            events: vec![Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: b"[1,2]".to_vec(),
                retry: RetryPolicy::none(),
            }],
            new_tasks: vec![NewActivityTask {
                seq: 0,
                activity_type: "Add".into(),
                input: b"[1,2]".to_vec(),
                next_run_at: 0,
            }],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
```

- [ ] **Step 4: Write the failing `fire_due_timer` test**

Add to `crates/persist/src/taskqueue_impl.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn fire_due_timer_appends_timer_fired_and_makes_runnable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in").await.unwrap();

        // Commit a TimerStarted event + a timer row already due (fire_at = 0).
        let commit = TurnCommit {
            events: vec![Event::TimerStarted { seq: 0, duration_ms: 500 }],
            new_tasks: vec![],
            new_timers: vec![engine::NewTimer { seq: 0, fire_at: 0 }],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();
        // commit_turn cleared runnable; a due timer must re-arm it.
        assert_eq!(db.next_runnable().await.unwrap(), None);

        assert!(db.fire_due_timer().await.unwrap(), "a due timer should fire");
        assert!(!db.fire_due_timer().await.unwrap(), "timer row consumed -> nothing due");

        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(h.last().unwrap().event, Event::TimerFired { seq: 0 }));
        assert_eq!(db.next_runnable().await.unwrap(), Some("run-1".into()));
    }

    #[tokio::test]
    async fn timer_not_due_yet_does_not_fire() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in").await.unwrap();
        let commit = TurnCommit {
            events: vec![Event::TimerStarted { seq: 0, duration_ms: 60_000 }],
            new_tasks: vec![],
            new_timers: vec![engine::NewTimer { seq: 0, fire_at: now_ms() + 60_000 }],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();
        assert!(!db.fire_due_timer().await.unwrap(), "future timer must not fire");
    }
```

The existing `db_with_task` helper builds a `TurnCommit`; add `new_timers: vec![]`
to it as well, plus the two literals in
`lease_round_trips_the_scheduled_retry_policy` and `task_not_due_yet_is_not_leasable`.

Run: `cargo test -p persist --lib fire_due_timer`
Expected: FAIL — `fire_due_timer` is not implemented (compile error on the trait
method).

- [ ] **Step 5: Implement `fire_due_timer`**

In `crates/persist/src/taskqueue_impl.rs`, add to `impl TaskQueue for Sqlite`
(after `reschedule_activity`):

```rust
    async fn fire_due_timer(&self) -> anyhow::Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = now_ms();

        let row: Option<(String, i64)> = tx
            .query_row(
                "SELECT run_id, seq FROM timers WHERE fire_at <= ?1 ORDER BY fire_at LIMIT 1",
                params![now],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((run_id, seq)) = row else {
            tx.commit()?;
            return Ok(false);
        };

        let event = Event::TimerFired { seq: seq as u64 };
        let payload = serde_json::to_vec(&event)?;
        let next_id: i64 = tx.query_row(
            "SELECT COALESCE(MAX(event_id), 0) + 1 FROM history WHERE run_id = ?1",
            params![run_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![run_id, next_id, seq, event.kind(), payload, now_ms()],
        )?;
        tx.execute(
            "DELETE FROM timers WHERE run_id = ?1 AND seq = ?2",
            params![run_id, seq],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
            params![run_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(true)
    }
```

- [ ] **Step 6: Run persist tests + whole-workspace build**

Run: `cargo test -p persist`
Expected: PASS — new timer tests plus all Pass 1 persist tests.

Run: `cargo build`
Expected: the whole workspace compiles green again.

- [ ] **Step 7: Commit**

```bash
git add crates/persist
git commit -m "feat(persist): timers — encode events, commit_turn rows, fire_due_timer"
```

---

### Task 4: Pass-2a acceptance — sleep workflow runs + cold recovery

**Files:**
- Create: `crates/engine/tests/timers.rs`

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

// Workflow: sleep, then Add(1, 2) == 3. A 0ms timer is due immediately, so the
// timer service fires it on the first pass — the test stays deterministic while
// still exercising the full StartTimer -> TimerStarted -> TimerFired path.
struct Nap;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Nap {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Nap";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        ctx.sleep(std::time::Duration::from_millis(0)).await;
        let a = ctx.activity::<Add>((1, 2)).await?;
        Ok(a)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Nap>();
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
async fn timer_workflow_runs_to_completion() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Nap>((), StartOptions { id: "nap-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 3);
}

#[tokio::test]
async fn timer_cold_recovery_completes_identically() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn (schedules the timer), then crash.
    {
        let engine = build(&db);
        engine
            .start_workflow::<Nap>((), StartOptions { id: "nap-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // emits StartTimer seq 0
        // engine dropped here; only the shared `db` survives. The timer row and the
        // TimerStarted event are durable; TimerFired has not been written yet.
    }

    // Phase 2: a fresh engine fires the timer and completes by cold replay.
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("nap-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 3);
}
```

- [ ] **Step 2: Run the acceptance tests**

Run: `cargo test -p engine --test timers`
Expected: both PASS — `timer_workflow_runs_to_completion`,
`timer_cold_recovery_completes_identically`.

- [ ] **Step 3: Commit**

```bash
git add crates/engine/tests/timers.rs
git commit -m "test(engine): pass 2a acceptance — timer workflow + cold recovery"
```

---

### Task 5: Whole-workspace green + clippy + roadmap update

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS (Pass 1 e2e still green:
  the existing `end_to_end.rs` `pump` does not call `process_one_timer`, which is
  fine — those workflows schedule no timers).
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean.
- [ ] **Step 3:** In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
  - Mark chunk `2a` status `done` and set its Plan file to
    `2026-06-14-pass-2a-timers.md`.
  - Update the "Canonical types" section: add `Command::StartTimer`,
    `Event::TimerStarted`/`TimerFired`, `NewTimer`, `TurnCommit.new_timers`, and
    `TaskQueue::fire_due_timer` (mirror the "Canonical type additions" block above).
- [ ] **Step 4:** Commit:

```bash
git add -A
git commit -m "chore: pass 2a complete — timers, clippy clean, roadmap + canonical types updated"
```

---

## Notes for Pass 2b and later

- **`join!` / `select_biased!` already work** with the current driver: they live
  inside `main`, the driver drains all commands emitted in a turn, and `cold_replay`
  applies one result per turn. Pass 2b adds `ctx.spawn`'s ordered scheduler (the one
  hand-built piece) plus the concurrency acceptance tests and the banned-combinator
  docs.
- **`ctx.now()` / `ctx.random()`** remain deferred (spec §9). When a workflow needs
  them, add recorded marker events analogous to `TimerStarted` so replay returns the
  recorded value.
- **Hardening folded in here:** the observer double-fire / re-drive-of-terminal-run
  guard (Task 2, Step 3b–c). **Still deferred to Pass 5 hardening** (not gating Pass
  2 acceptance, tracked in the `pass2-hardening-backlog` memory): activity
  lease-expiry/heartbeat (needs an `activity_tasks.lease_expires_at` column),
  unregistered-workflow poison-spin dead-lettering, and replacing the busy-poll loops
  with notify/channel wakeups.
- **Timer determinism is in replay, not wall-clock.** Integration tests use a 0ms
  timer (due immediately) so they need no real waiting; the blocking/ordering
  semantics are proven by the pure `cold_replay` tests in Task 1. Production uses real
  durations; the timer service fires when `fire_at <= now`.

## Self-Review (completed during authoring)

- **Spec coverage:** §4 (timer as a `seq`-bearing command under one-event-per-turn),
  §5.3 (timer service: `fire_due_timer` appends `TimerFired`, marks runnable), §11
  (`timers` table now populated/consumed; `TimerStarted`/`TimerFired` in `history.kind`),
  §12 Invariant 9 (timer-duration divergence check in `cold_replay`), Pass 2 timer
  half of the §13 acceptance gate (sleep workflow + cold recovery).
- **Placeholders:** none — full code, exact commands, expected outcomes throughout.
- **Type consistency:** `Command::StartTimer{seq,duration_ms}`,
  `Event::TimerStarted{seq,duration_ms}` / `TimerFired{seq}`,
  `NewTimer{seq:i64,fire_at:i64}`, `TurnCommit.new_timers`,
  `TaskQueue::fire_due_timer(&self)->Result<bool>`, `Context::timer/sleep/apply_timer_fired`,
  `WorkflowState::apply_timer_fired`, `TimerFuture: Future<Output=()>` are used
  identically across the workflow/engine/persist tasks. `CommandResult` is
  deliberately unchanged.
```
