# In-Process Durable Workflow Engine — Design Spec

A single-node, embeddable workflow orchestration engine in Rust with a
Temporal-like API (activities, workflows, child workflows, **signals**), backed
by SQLite, running entirely in-process. Target host: a Tauri desktop app used to
iterate on workflow/tool/skill shape before lifting the same workflow code to a
cloud backend.

This document is the implementation spec. Treat the **Invariants** section
(§12) as load-bearing — most of the correctness of the engine reduces to those
rules. The work is sequenced into ordered **implementation passes** (§13); each
pass is independently buildable and testable.

**Guiding principle — Temporal Go SDK parity.** The workflow-facing API is
shaped to mirror Temporal's Go SDK so that, once workflow logic is nailed down
on desktop, translating it to the Go SDK is mechanical. Where a Rust idiom and a
Go SDK idiom diverge, §11 records the intended mapping rather than forcing the
surface to match exactly.

---

## 1. Goals and non-goals

**Goals**

- Temporal-style programming model: workflows as deterministic `async fn`s that
  orchestrate activities and child workflows.
- Durable and resumable: a process crash at any point recovers to the exact
  logical position, with no lost or double-applied workflow decisions.
- Retries and a clear idempotency story for activities (side-effecting work).
- Concurrency *within* a workflow (concurrent activity branches, races,
  detached tasks), even though we run with parallelism = 1 across workflows.
- **Signals**: externally-injected input (driven by Tauri IPC from the
  frontend) delivered durably to a running workflow and consumed through a
  Go-SDK-style signal channel.
- A trait boundary clean enough that the workflow code written during desktop
  iteration carries over unchanged to a cloud backend.

**Non-goals (v1)**

- Multiple namespaces. Single namespace only.
- Scale-out / multi-node. Single process, single SQLite file.
- Network-distributed workers. Activities run in-process on a tokio pool.
- **Queries** (Temporal `QueryWorkflow`). The frontend observes progress through
  a completion observer hook (§7.3), not a query handler. Hooks left where
  cheap.
- **Real cancellation / terminate.** `RequestCancellation` is deferred, but the
  inbound-event pipeline that signals introduce (§6) is designed so cooperative
  cancellation slots in as a second inbound-event kind, not a refactor (§6.4).
- Search attributes. Deferred; hooks left where cheap.

---

## 2. Core insight

Workflow code is **never serialized**. The only durable state is an append-only
**event history**. A running workflow is a pure, deterministic function of its
history. Recovery is *not* a stack snapshot — it is re-executing the workflow
function from the top and replaying the recorded history into it, which
fast-forwards it to where it left off.

Consequences:

- The live Rust `Future` is a **cache**, not the source of truth. We never have
  to serialize a continuation (which Rust can't do anyway). Cold recovery
  rebuilds the future by replay.
- Determinism is only required **on replay**. The original execution is allowed
  to observe real nondeterminism (race outcomes, wall-clock time, RNG, **signal
  arrivals**) **as long as every nondeterministic observation is recorded the
  first time and replayed thereafter.**

---

## 3. The replay mechanism

A workflow is `async fn(WfContext, Input) -> Output`. Every interaction with the
outside world goes through `ctx`. Each such call is assigned a monotonic
**command sequence number (`seq`)** *at creation time, not poll time*. Because
the workflow body is deterministic and single-threaded, the Nth `ctx.activity()`
call is always the same logical operation across replays, so `seq` is a stable
identifier into history.

```rust
struct WfContextInner {
    next_seq:   Cell<u64>,
    results:    RefCell<HashMap<u64, CommandResult>>, // seq -> recorded outcome
    scheduled:  RefCell<HashSet<u64>>,                // seqs already emitted this life
    commands:   RefCell<Vec<Command>>,                // emitted this turn, drained by driver
    now:        Cell<SystemTime>,                      // deterministic clock (recorded)
    signals:    RefCell<HashMap<String, VecDeque<Vec<u8>>>>, // name -> buffered signal payloads
}
```

(The `signals` field is introduced by pass 3; see §6.)

A command future (activity shown; timers / child workflows are identical in
shape):

```rust
impl Future for ActivityFuture {
    type Output = Result<Vec<u8>, ActivityError>;
    fn poll(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Self::Output> {
        // 1. Replay path: result already known -> resolve immediately.
        if let Some(r) = self.ctx.results.borrow().get(&self.seq) {
            return Poll::Ready(r.clone().into());
        }
        // 2. First arrival at this point: emit the command exactly once, then park.
        if self.ctx.scheduled.borrow_mut().insert(self.seq) {
            self.ctx.commands.borrow_mut().push(Command::ScheduleActivity {
                seq: self.seq,
                activity_type: self.ty.clone(),
                input: self.input.clone(),
                retry: self.retry.clone(),
            });
        }
        Poll::Pending
    }
}
```

The `scheduled` set is what prevents a re-poll (e.g. when a sibling branch in a
`join!` wakes the whole future) from emitting a duplicate `ScheduleActivity` for
a `seq` that is in flight but not yet resolved.

`ctx.now()` and `ctx.random()` follow the same pattern: the first execution
records a value into history; replay returns the recorded value.

---

## 4. Concurrency model

Intra-workflow concurrency is unavoidable: the Temporal-style API exposes
goroutine-like concurrent branches (`join!`, races, `ctx.spawn`). Parallelism = 1
across workflows does **not** remove this — concurrency ≠ parallelism. The
branches still have to make progress concurrently within one run.

The model is made deterministic by **one discipline plus one ban list**.

### 4.1 Invariant: one new event applied per turn

The driver applies **exactly one** new event into `ctx` (a completion **or** an
inbound event such as a signal), then polls the workflow future once, then
applies the next event, and so on.

Activities run in parallel and finish in wall-clock order, but each completion is
committed to history one at a time, so **history is a total order over
observations**. Because the workflow can never observe two new results in the
same poll, it always sees completions in recorded sequence — so any combinator
that is itself deterministic replays identically. This is the single rule that
makes concurrent branches safe.

### 4.2 Allowed vs banned combinators

- **Allowed:** `join!`, `try_join!`, ordered `join_all`, and `select_biased!`.
  Their poll/branch order is deterministic, so with the one-event-per-turn rule
  they replay exactly. `select_biased!` is the Rust analog of Temporal's
  `workflow.Selector` (deterministic by registration order).
- **Banned (footguns):**
  - `futures::select!` — randomizes branch order by default. Use
    `select_biased!`.
  - bare `FuturesUnordered` — reorders by wakeup order, which is wall-clock
    dependent.

These bans are lintable and should eventually be enforced by a `#[workflow]`
macro or a clippy lint. Until then they are a documented contract.

### 4.3 Cancellation of losing branches (v1 semantics)

When `select_biased!` resolves, losing branches drop. In v1 the activity those
branches scheduled keeps running to completion; its result lands in `results`
and is simply never consumed. Real cancellation requests are deferred (§6.4) —
they are not a correctness prerequisite.

### 4.4 Detached spawn — the only hand-built scheduler

`ctx.spawn(future)` (the `workflow.Go` analog) returns an awaitable handle for a
branch that is not awaited inline by a combinator. Those futures must be polled
every turn even when nothing awaits them, so `ctx` keeps an **ordered**
collection of spawned tasks keyed by spawn `seq`, and the turn loop polls them in
creation order. This ordered vec is the entire scheduler — `async`/`await` does
the stack-suspension work, so there is no dispatcher loop or yield primitive to
write.

```rust
fn run_turn(wf: &mut WorkflowState) -> Vec<Command> {
    // Precondition: caller has applied AT MOST ONE new event (completion or
    // inbound) into wf.ctx before calling run_turn.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    // Poll main line, then every spawned task, in deterministic (seq) order.
    let _ = wf.main.as_mut().poll(&mut cx);
    for task in wf.spawned.iter_mut() {          // ordered by spawn seq
        let _ = task.future.as_mut().poll(&mut cx);
    }

    wf.ctx.commands.borrow_mut().drain(..).collect()
}
```

---

## 5. Engine architecture

Three concerns, all on tokio, coordinated through SQLite tables. Workflow
**decisions** are serialized globally (parallelism = 1 across workflows);
activity **execution** runs in parallel.

### 5.1 Workflow driver (single-threaded decision loop)

Pulls runnable workflows, and for each:

1. Load the cached future for the `run_id`, or cold-replay from `history` if not
   cached.
2. Apply new events **one at a time** — completions and inbound events (signals)
   alike — polling via `run_turn` between each (§4.1).
3. In a **single transaction**: append the new history events, upsert the
   resulting `activity_tasks` / `timers`, update execution status, and clear the
   run from `runnable`.
4. If the run reached a terminal status this turn, invoke the **completion
   observer hook** (§7.3) after the transaction commits.

That transaction is the **exactly-once boundary on the workflow side**: a
completion is consumed and its resulting commands are committed atomically, so a
crash mid-turn just re-runs the turn from the last committed history.

Running the decision loop single-threaded means one writer on history, no
concurrent SQLite write transactions, and no lock discipline around the per-run
results map. This is a deliberate simplification that desktop scale permits.

### 5.2 Activity workers (parallel)

Poll `activity_tasks WHERE status='pending' AND next_run_at <= now`, run the
registered async fn (real IO, retries via policy), and on completion append
`ActivityCompleted` / `ActivityFailed` to history and mark the run `runnable`.

Activities are **at-least-once**: if the process dies after the side effect but
before the result is recorded, the task is re-dispatched. See §9.

### 5.3 Timer service

`timers WHERE fire_at <= now` → append `TimerFired`, mark runnable.

### 5.4 Child workflows

A child workflow is an ordinary `executions` row with `parent_run_id` /
`parent_seq` set. Its terminal status writes a `ChildCompleted` event into the
**parent's** history, marking the parent runnable.

---

## 6. Inbound events and signals

Signals are the first member of a small, general category: **inbound events** —
externally-injected facts that are appended to a run's history and applied to
`ctx` state, as opposed to *commands* the workflow itself issues. Building this
category generically (rather than as a signal-specific feature) is what keeps
cooperative cancellation (§6.4) a later addition instead of a refactor.

An inbound event:

- is appended to `history` by a host-side entrypoint (§7), not by `run_turn`;
- carries **no `seq`** — it is unsolicited, not a workflow-issued command, so it
  lives outside the command / `scheduled` / divergence-check machinery
  (Invariant 9 still only checks *emitted commands*);
- is consumed under the **one-event-per-turn** rule (§4.1) exactly like a
  completion: applying it mutates `ctx` state, then the workflow is polled once;
- has its payload recorded once and replayed in `history.event_id` order, so
  replay reproduces the same observations.

### 6.1 Delivery (caller side)

Signals originate from a Tauri command handler calling the engine **in-process**,
against the same SQLite file — there is no network. The entrypoint:

```rust
engine.signal_workflow(workflow_id, "approve", &payload).await
    -> Result<(), SignalError>;
// or, by run, via a handle:
handle.signal("approve", &payload).await -> Result<(), SignalError>;
```

It performs a **single transaction**: append `SignalReceived { name, payload }`
to the target run's history and mark it `runnable`. By the time the call returns
`Ok(())`, the signal is **durably committed** — so the Tauri command can give the
frontend synchronous confirmation (button press → "recorded").

Errors are typed so the IPC layer can propagate a meaningful result to the
frontend:

```rust
enum SignalError {
    WorkflowNotFound,   // no execution with that workflow_id
    NotRunning,         // execution is completed/failed — signaling it errors
                        // (matches Temporal; do not buffer for a run that will
                        // never consume it)
}
```

**No deduplication, no inbound table.** Because delivery is in-process and
committed-before-return, the only way to get a duplicate is for the frontend to
send twice deliberately, which is the frontend's call. Signals are therefore
**at-least-once** in contract (Go-SDK-faithful) and effectively exactly-once in
practice. Workflow code must not *assume* exactly-once — this keeps Go SDK
portability — but in this host it gets it.

### 6.2 Determinism

`SignalReceived` is just another history event consumed under one-event-per-turn
(Invariant 3). Applying it pushes its payload onto a per-name buffer in `ctx`
(`signals: HashMap<String, VecDeque<Vec<u8>>>`); the next poll lets a parked
`recv()` resolve. On replay, the same events replay in `event_id` order →
identical buffer contents → identical receive outcomes. No `seq` is involved, so
signals are exempt from the command-divergence check (§12, Invariant 9), which
only ever compares *emitted commands*.

### 6.3 API (consumption) — Go-SDK-faithful

```rust
let approvals = ctx.signal_channel::<Approval>("approve"); // GetSignalChannel analog
select_biased! {
    a = approvals.recv() => proceed(a?),                   // ReceiveChannel.Receive
    _ = ctx.sleep(Duration::from_secs(86_400)) => escalate(),
}
```

- `ctx.signal_channel::<T>(name)` returns the same logical channel for a given
  name (**idempotent by name**, like `workflow.GetSignalChannel`). It allocates
  no command and consumes no `seq`.
- `recv()` is a future that resolves when the per-name buffer is non-empty,
  popping the front; empty → `Pending`. The Nth `recv()` on a channel
  deterministically pops the Nth buffered signal of that name, so it is
  replay-stable **without** a `seq`.
- Composes with `select_biased!` (the Selector analog) for the motivating
  "signal **or** timeout" pattern, which is why signals land *after* the
  timer + concurrency pass (§13).

### 6.4 Reserved extension: cooperative cancellation

`RequestCancellation` is deferred, but reserved as the **second inbound-event
kind** so it is not a redesign later:

- A `WorkflowCancelRequested` event kind is reserved in the `history.kind` enum
  (§11).
- A host entrypoint `engine.cancel_workflow(workflow_id)` (not implemented in the
  early passes) would append it like a signal.
- Applying it flips a `cancelled` flag on `ctx`; combinators and `ctx.activity`
  observe it cooperatively (Temporal's `ctx.Done()` / `workflow.ErrCanceled`
  model). Forceful **terminate** is a separate, simpler status write, also
  deferred.

Nothing in the early passes implements cancellation; they implement the inbound
pipeline that makes adding it cheap.

---

## 7. Host / IPC API surface

This is the host→engine surface the Tauri frontend drives over IPC. It is
distinct from the workflow-authoring surface (§10). All entrypoints are ordinary
in-process async calls returning typed `Result`s the IPC layer can forward.

### 7.1 StartWorkflow

```rust
let handle = engine.start_workflow::<OrderWorkflow>(
    input,
    StartOptions { id: "order-123".into(), ..Default::default() },
).await?;          // returns a handle carrying run_id
```

The client-supplied workflow `id` is the **top-level dedup / idempotency** story
(Temporal's `WorkflowIdReusePolicy`): starting the same `id` twice returns a
handle to the existing run rather than spawning a second. Over IPC: takes
workflow type + input + `workflow_id`, returns `run_id`.

### 7.2 SignalWorkflow

`engine.signal_workflow(workflow_id, name, &payload)` / `handle.signal(...)` —
see §6.1.

### 7.3 Completion observer hook

A Tauri frontend cannot `.await` a Rust future across IPC, so completion is
**pushed**, not awaited. The driver exposes an observer seam invoked after the
decision-turn transaction commits when a run reaches a terminal status:

```rust
engine.on_run_completed(|event: RunCompleted| { /* Tauri re-emits to frontend */ });
// RunCompleted { run_id, workflow_id, status, result }
```

In-process, `handle.result().await` remains the primitive (it is implemented on
top of the same signal that the run is terminal). The observer hook is
deliberately generic — a future progress-stream or query feature (both non-goals
now) can extend the same seam rather than introduce a new one.

### 7.4 List / describe (trivial reads)

`engine.list_workflows()` / `engine.describe_workflow(workflow_id)` are read-only
SELECTs over `executions` (status, result). No engine machinery; included for UI
completeness.

---

## 8. Idempotency and delivery guarantees

The contract is the same split Temporal gives:

- **Workflow decisions: exactly-once.** Guaranteed by the atomic per-turn
  transaction (§5.1).
- **Activity execution: at-least-once.** Guaranteed nothing — the activity may
  run more than once across a crash.
- **Signal delivery: at-least-once in contract, exactly-once into history.** The
  `SignalReceived` append is the durable boundary; workflow code tolerates
  duplicates (§6.1).

Idempotency is therefore the activity author's responsibility, and the engine
makes it tractable by handing each activity a **stable idempotency key** that is
identical across retries and re-deliveries:

```
idempotency_key = "{run_id}:{seq}"
```

Activities use it at their side-effect boundary (payment provider idempotency
key, `INSERT ... ON CONFLICT`, dedupe table, etc.).

```rust
async fn charge(ctx: ActivityContext, input: ChargeInput) -> Result<Receipt, ActivityError> {
    let key = ctx.idempotency_key(); // stable across attempts/redeliveries
    stripe.charge(&input, idempotency = key).await
}
```

Retries live in the `activity_tasks` table (attempt count + `next_run_at`
backoff). Only terminal outcomes (`ActivityCompleted` / `ActivityFailed` after
the policy is exhausted) are written to history; intermediate retries stay in the
task table.

---

## 9. Activity workers and at-least-once execution

(See §5.2.) Activities run on a parallel tokio pool. The at-least-once guarantee
means a crash between side effect and result append re-dispatches the task; the
stable idempotency key (§8) is how the activity author makes that safe.

---

## 10. Workflow / activity API surface

This is the part that must remain stable across the desktop→cloud migration.

```rust
// --- Workflow: deterministic, only touches the world through `ctx` ---
async fn order_workflow(ctx: WfContext, input: OrderInput) -> Result<OrderResult, WfError> {
    let receipt = ctx.activity::<Charge>(ChargeInput { card: input.card, cents: input.total })
        .retry(RetryPolicy::exponential(5))
        .await?;

    // Wait for human approval, or escalate after a day.
    let approvals = ctx.signal_channel::<Approval>("approve");
    select_biased! {
        a = approvals.recv() => { a?; }
        _ = ctx.sleep(Duration::from_secs(86_400)) => return Err(WfError::ApprovalTimeout),
    }

    let shipment = ctx.child_workflow::<ShipWorkflow>(ShipInput::from(&input)).await?;
    Ok(OrderResult { receipt, shipment })
}

// --- Activity: ordinary async fn, real IO and nondeterminism allowed ---
async fn charge(ctx: ActivityContext, input: ChargeInput) -> Result<Receipt, ActivityError> {
    let key = ctx.idempotency_key();
    stripe.charge(&input, idempotency = key).await
}

// --- Engine setup / start ---
let engine = Engine::open("app.db").await?;
engine.register_workflow::<OrderWorkflow>();
engine.register_workflow::<ShipWorkflow>();
engine.register_activity::<Charge>();
engine.on_run_completed(|e| { /* Tauri emit */ });
engine.start().await?;

let handle = engine
    .start_workflow::<OrderWorkflow>(
        input,
        StartOptions { id: "order-123".into(), ..Default::default() },
    )
    .await?;

let result: OrderResult = handle.result().await?; // awaits durable completion (in-process)
```

### Concurrency helpers exposed on `ctx`

- `ctx.activity::<A>(input)` → awaitable, with `.retry(..)`.
- `ctx.child_workflow::<W>(input)` → awaitable.
- `ctx.sleep(dur)` / `ctx.timer(dur)`.
- `ctx.now()` / `ctx.random()` — deterministic, recorded.
- `ctx.signal_channel::<T>(name)` → idempotent-by-name channel; `.recv()` awaits
  one buffered signal (§6.3).
- `ctx.spawn(future) -> SpawnHandle` — detached branch (§4.4).
- Use `join!` / `try_join!` / `select_biased!` over these; never `select!` or
  `FuturesUnordered`.

### 10.1 Rust ↔ Temporal Go SDK mapping

The Rust surface is kept idiomatic; this table records the intended translation
so workflow logic ports mechanically to the Go SDK.

| Rust (this engine) | Temporal Go SDK |
| --- | --- |
| `ctx.activity::<A>(input).retry(p).await` | `workflow.ExecuteActivity(ctx, A, input).Get(ctx, &res)` (+ `RetryPolicy` on `ActivityOptions`) |
| `ctx.child_workflow::<W>(input).await` | `workflow.ExecuteChildWorkflow(ctx, W, input).Get(ctx, &res)` |
| `ctx.sleep(dur).await` / `ctx.timer(dur)` | `workflow.Sleep(ctx, dur)` / `workflow.NewTimer(ctx, dur)` |
| `ctx.now()` | `workflow.Now(ctx)` |
| `ctx.random()` | `workflow.SideEffect` / `workflow.Now`-seeded RNG (recorded) |
| `ctx.signal_channel::<T>(name)` | `workflow.GetSignalChannel(ctx, name)` |
| `channel.recv().await` | `ch.Receive(ctx, &v)` |
| `select_biased! { ... }` | `workflow.NewSelector(ctx)` + `AddReceive` / `AddFuture` (deterministic by registration order) |
| `ctx.spawn(fut)` | `workflow.Go(ctx, fn)` |
| `ctx.idempotency_key()` (activity) | `activity.GetInfo(ctx)` identity (`WorkflowID:ActivityID`) |
| `ctx.patched("id")` (reserved, §14) | `workflow.GetVersion(ctx, "id", min, max)` |
| `engine.start_workflow::<W>(..)` | `Client.ExecuteWorkflow` |
| `engine.signal_workflow(id, name, p)` | `Client.SignalWorkflow` |
| `engine.cancel_workflow(id)` (reserved, §6.4) | `Client.CancelWorkflow` |

---

## 11. SQLite schema

Event-sourced, with derived task tables so the engine has a cheap "what's
runnable" query instead of scanning history.

```sql
CREATE TABLE executions (
  run_id        TEXT PRIMARY KEY,
  workflow_id   TEXT NOT NULL,          -- user-facing id, for dedup
  workflow_type TEXT NOT NULL,
  parent_run_id TEXT,                   -- set for child workflows
  parent_seq    INTEGER,               -- command seq in parent that spawned this
  input         BLOB,
  status        TEXT NOT NULL,          -- running | completed | failed
  result        BLOB,
  UNIQUE(workflow_id)                   -- enforces start dedup
);

CREATE TABLE history (
  run_id   TEXT NOT NULL,
  event_id INTEGER NOT NULL,            -- per-run monotonic, defines replay order
  seq      INTEGER,                     -- command seq this event resolves (NULL for
                                        --   WfStarted and for inbound events)
  kind     TEXT NOT NULL,               -- WfStarted | ActivityScheduled
                                        -- | ActivityCompleted | ActivityFailed
                                        -- | TimerFired | ChildCompleted
                                        -- | SignalReceived
                                        -- | WorkflowCancelRequested (reserved, §6.4)
  payload  BLOB,
  ts       INTEGER NOT NULL,
  PRIMARY KEY (run_id, event_id)
);

-- Actionable work queue (derivable from history, kept in sync transactionally).
CREATE TABLE activity_tasks (
  run_id        TEXT NOT NULL,
  seq           INTEGER NOT NULL,
  activity_type TEXT NOT NULL,
  input         BLOB,
  attempt       INTEGER NOT NULL DEFAULT 0,
  next_run_at   INTEGER NOT NULL,       -- retry backoff
  status        TEXT NOT NULL,          -- pending | running | done
  PRIMARY KEY (run_id, seq)
);

CREATE TABLE timers (
  run_id  TEXT NOT NULL,
  seq     INTEGER NOT NULL,
  fire_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, seq)
);

-- Which workflows have new events to process (the "workflow task" queue).
CREATE TABLE runnable (
  run_id TEXT PRIMARY KEY,
  since  INTEGER NOT NULL
);
```

`history.event_id` is the canonical replay order. The driver's one-event-per-turn
rule consumes new events (completions and inbound events) strictly in `event_id`
order. **Signals need no table of their own** — they are `history` rows with
`kind='SignalReceived'`, `seq=NULL`, payload = encoded signal; the existing
`runnable` queue carries the wake-up.

---

## 12. Invariants (do not break these)

1. **History is the only source of truth.** Never persist or serialize a
   workflow future. Cold recovery = replay history into a fresh future.
2. **`seq` is assigned at command-creation time**, in deterministic order. The
   Nth `ctx` command call is the same logical op on every replay. Inbound events
   (signals) carry no `seq`.
3. **Apply exactly one new event per poll turn** — a completion *or* an inbound
   event — in `history.event_id` order. This is what makes concurrent branches
   deterministic.
4. **Emit each command exactly once** per workflow life (the `scheduled` set);
   re-polls of an in-flight `seq` must not re-emit.
5. **One atomic transaction per decision turn**: append events + upsert tasks +
   update status + clear `runnable`, together. This is the exactly-once boundary.
   The completion observer hook fires only *after* this commits.
6. **Workflow code is pure modulo `ctx`.** No `SystemTime::now()`, no direct IO,
   no `reqwest`, no `rand` — all of that goes through `ctx` or lives in an
   activity.
7. **Only deterministic combinators** in workflow code: `join!`, `try_join!`,
   `select_biased!`. Never `select!` or `FuturesUnordered`.
8. **Activities are at-least-once**; they must be idempotent via
   `ctx.idempotency_key()`.
9. **Nondeterminism check on replay:** if the *commands* emitted on a turn
   diverge from what history records for those seqs, fail loudly (nondeterminism
   error) — do not silently continue. Inbound events carry no `seq` and are
   **exempt** from this check; their replay stability comes from recorded
   payloads + deterministic poll order (§6.2).
10. **Inbound events are recorded once and replayed.** A signal's payload is
    written to history exactly once and replayed in `event_id` order; the
    per-name buffer is rebuilt identically on every replay.

---

## 13. Implementation passes

The engine is built as five ordered passes. Each is independently buildable and
has its own acceptance tests. Signals land in **pass 3** — the earliest slot that
isn't artificial, because the motivating "signal or timeout" pattern needs the
timer + concurrency machinery from pass 2.

### Pass 1 — Replay core (activities only)

The spine: `WfContext` + `seq`, `history`, cold replay, the one-event-per-turn
loop (degenerate/sequential here), the atomic decision-turn transaction, activity
scheduling/completion/failure, retries + `idempotency_key`, and the
nondeterminism divergence check. Single workflow, sequential `await`.
StartWorkflow (§7.1) and the completion observer hook (§7.3) land here, the hook
firing on terminal status.

*Acceptance:* an activity-only workflow runs to completion; killing the process
mid-run cold-replays to the same logical position; a forced cache-evict
reproduces an identical command stream.

### Pass 2 — Time & concurrency

`ctx.sleep`/`ctx.timer` + the timer service; the real concurrency model
(`join!`, `try_join!`, ordered `join_all`, `select_biased!`, `ctx.spawn`'s
ordered scheduler); the banned-combinator contract. This exercises
one-event-per-turn non-degenerately.

*Acceptance:* concurrent activity branches and a `select_biased!` race replay
deterministically across cold recovery; losing branches behave per §4.3.

### Pass 3 — Signals (and the inbound-event pipeline)

The generic inbound-event pipeline (§6): `SignalReceived` events, the per-name
`ctx` buffer, `ctx.signal_channel`/`recv`, `engine.signal_workflow` /
`handle.signal` with typed `SignalError`, and Selector composition
(signal-or-timeout). Builds on pass 2's combinators.

*Acceptance:* a workflow blocked on `recv()` resumes when signaled; the
signal-or-timeout pattern resolves each way deterministically; signals delivered
before/after a crash replay identically; signaling a completed run returns
`NotRunning`.

### Pass 4 — Child workflows

Parent/child executions, `ChildCompleted` into the parent's history, parent
re-marked runnable.

*Acceptance:* a parent awaiting a child completes when the child does, across
cold recovery of either.

### Pass 5 — Durability hardening & migration seam

Sticky-cache vs cold-replay equivalence test as a standing guard; the
`ctx.patched("change-id")` hook (§14); the `History` / `TaskQueue` trait boundary
made explicit (§15); divergence-check hardening.

*Acceptance:* the cache/cold-replay equivalence test passes as a CI guard; the
two traits compile as the only seam the SQLite backend implements.

---

## 14. Sharp edges and future work

- **Determinism enforcement** can't be fully achieved at compile time in Rust.
  Rely on convention + the runtime divergence check (Invariant 9) + an eventual
  `#[workflow]` lint. Document the contract hard.
- **Versioning / code change vs in-flight histories.** Changing a workflow's
  shape can break replay of running instances. For desktop iteration it's
  acceptable to drain or abandon running workflows on code change, but leave a
  `ctx.patched("change-id")` hook in from day one (the `GetVersion` analog) —
  retrofitting it later is painful, and the cloud version will need it.
- **Sticky cache vs cold replay equivalence.** Keeping live futures cached makes
  steady state cheap; cold replay is the correctness fallback. Periodically
  force-evict and assert the command stream is identical — that test (pass 5) is
  the real guard on the determinism contract.
- **Cooperative cancellation & terminate** — deferred; the inbound-event pipeline
  (§6.4) and the reserved `WorkflowCancelRequested` kind keep it a cheap
  addition.
- **Queries** — deferred; the completion observer hook (§7.3) covers the desktop
  UI's immediate need and is generic enough to extend.
- **Race-branch result cleanup** — losing `select_biased!` branches leave
  unconsumed results in `results`; fine for correctness, revisit if it bloats
  history.

---

## 15. Cloud migration boundary

Keep two traits clean and the desktop↔cloud swap is "reimplement two traits,"
not a rewrite:

- `History` — append events (including inbound events), read history for a run,
  atomic decision-turn commit.
- `TaskQueue` — enqueue/lease/complete activity tasks and timers; mark runnable.

The SQLite engine is one implementation of these; a server-backed engine is
another. **Workflow and activity code (§10) imports neither** — it only sees
`WfContext` / `ActivityContext`, so it carries over unchanged. The host/IPC
surface (§7) is desktop-specific glue and is expected to be re-implemented per
host.
