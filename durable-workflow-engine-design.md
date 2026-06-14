# In-Process Durable Workflow Engine — Design Spec

A single-node, embeddable workflow orchestration engine in Rust with a
Temporal-like API (activities, workflows, child workflows), backed by SQLite,
running entirely in-process. Target host: a Tauri desktop app used to iterate on
workflow/tool/skill shape before lifting the same workflow code to a cloud
backend.

This document is the implementation spec. Treat the **Invariants** section as
load-bearing — most of the correctness of the engine reduces to those rules.

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
- A trait boundary clean enough that the workflow code written during desktop
  iteration carries over unchanged to a cloud backend.

**Non-goals (v1)**

- Multiple namespaces. Single namespace only.
- Scale-out / multi-node. Single process, single SQLite file.
- Network-distributed workers. Activities run in-process on a tokio pool.
- Real activity cancellation, signals/queries, search attributes. Hooks left in
  place where cheap; full implementations deferred.

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
  to observe real nondeterminism (race outcomes, wall-clock time, RNG) **as long
  as every nondeterministic observation is recorded the first time and replayed
  thereafter.**

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
}
```

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

### 4.1 Invariant: one completion event applied per turn

The driver applies **exactly one** new completion event into the `results` map,
then polls the workflow future once, then applies the next event, and so on.

Activities run in parallel and finish in wall-clock order, but each completion is
committed to history one at a time, so **history is a total order over
observations**. Because the workflow can never observe two new results in the
same poll, it always sees completions in recorded sequence — so any combinator
that is itself deterministic replays identically. This is the single rule that
makes concurrent branches safe.

### 4.2 Allowed vs banned combinators

- **Allowed:** `join!`, `try_join!`, ordered `join_all`, and `select_biased!`.
  Their poll/branch order is deterministic, so with the one-event-per-turn rule
  they replay exactly.
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
and is simply never consumed. Real cancellation requests are deferred — they are
not a correctness prerequisite.

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
    // Precondition: caller has applied AT MOST ONE new completion event
    // into wf.ctx.results before calling run_turn.
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
2. Apply new completion events **one at a time**, polling via `run_turn` between
   each (Section 4.1).
3. In a **single transaction**: append the new history events, upsert the
   resulting `activity_tasks` / `timers`, update execution status, and clear the
   run from `runnable`.

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
before the result is recorded, the task is re-dispatched. See Section 6.

### 5.3 Timer service

`timers WHERE fire_at <= now` → append `TimerFired`, mark runnable.

### 5.4 Child workflows

A child workflow is an ordinary `executions` row with `parent_run_id` /
`parent_seq` set. Its terminal status writes a `ChildCompleted` event into the
**parent's** history, marking the parent runnable.

---

## 6. Idempotency and delivery guarantees

The contract is the same split Temporal gives:

- **Workflow decisions: exactly-once.** Guaranteed by the atomic per-turn
  transaction (Section 5.1).
- **Activity execution: at-least-once.** Guaranteed nothing — the activity may
  run more than once across a crash.

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

## 7. API surface

This is the part that must remain stable across the desktop→cloud migration.

```rust
// --- Workflow: deterministic, only touches the world through `ctx` ---
async fn order_workflow(ctx: WfContext, input: OrderInput) -> Result<OrderResult, WfError> {
    let receipt = ctx.activity::<Charge>(ChargeInput { card: input.card, cents: input.total })
        .retry(RetryPolicy::exponential(5))
        .await?;

    ctx.sleep(Duration::from_secs(30)).await;

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
engine.start().await?;

let handle = engine
    .start_workflow::<OrderWorkflow>(
        input,
        StartOptions { id: "order-123".into(), ..Default::default() },
    )
    .await?;

let result: OrderResult = handle.result().await?; // awaits durable completion
```

The client-supplied workflow `id` is the **top-level dedup / idempotency** story
(Temporal's `WorkflowIdReusePolicy`): starting the same `id` twice returns a
handle to the existing run rather than spawning a second.

### Concurrency helpers exposed on `ctx`

- `ctx.activity::<A>(input)` → awaitable, with `.retry(..)`.
- `ctx.child_workflow::<W>(input)` → awaitable.
- `ctx.sleep(dur)` / `ctx.timer(dur)`.
- `ctx.now()` / `ctx.random()` — deterministic, recorded.
- `ctx.spawn(future) -> SpawnHandle` — detached branch (Section 4.4).
- Use `join!` / `try_join!` / `select_biased!` over these; never `select!` or
  `FuturesUnordered`.

---

## 8. SQLite schema

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
  seq      INTEGER,                     -- command seq this event resolves (nullable)
  kind     TEXT NOT NULL,               -- WfStarted | ActivityScheduled
                                        -- | ActivityCompleted | ActivityFailed
                                        -- | TimerFired | ChildCompleted | ...
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
rule consumes completion events strictly in `event_id` order.

---

## 9. Invariants (do not break these)

1. **History is the only source of truth.** Never persist or serialize a
   workflow future. Cold recovery = replay history into a fresh future.
2. **`seq` is assigned at command-creation time**, in deterministic order. The
   Nth `ctx` call is the same logical op on every replay.
3. **Apply exactly one completion event per poll turn**, in `history.event_id`
   order. This is what makes concurrent branches deterministic.
4. **Emit each command exactly once** per workflow life (the `scheduled` set);
   re-polls of an in-flight `seq` must not re-emit.
5. **One atomic transaction per decision turn**: append events + upsert tasks +
   update status + clear `runnable`, together. This is the exactly-once boundary.
6. **Workflow code is pure modulo `ctx`.** No `SystemTime::now()`, no direct IO,
   no `reqwest`, no `rand` — all of that goes through `ctx` or lives in an
   activity.
7. **Only deterministic combinators** in workflow code: `join!`, `try_join!`,
   `select_biased!`. Never `select!` or `FuturesUnordered`.
8. **Activities are at-least-once**; they must be idempotent via
   `ctx.idempotency_key()`.
9. **Nondeterminism check on replay:** if the commands emitted on a turn diverge
   from what history records for those seqs, fail loudly (nondeterminism error) —
   do not silently continue.

---

## 10. Sharp edges and future work

- **Determinism enforcement** can't be fully achieved at compile time in Rust.
  Rely on convention + the runtime divergence check (Invariant 9) + an eventual
  `#[workflow]` lint. Document the contract hard.
- **Versioning / code change vs in-flight histories.** Changing a workflow's
  shape can break replay of running instances. For desktop iteration it's
  acceptable to drain or abandon running workflows on code change, but leave a
  `ctx.patched("change-id")` hook in from day one — retrofitting it later is
  painful, and the cloud version will need it.
- **Sticky cache vs cold replay equivalence.** Keeping live futures cached makes
  steady state cheap; cold replay is the correctness fallback. Periodically
  force-evict and assert the command stream is identical — that test is the real
  guard on the determinism contract.
- **Real activity cancellation, signals, queries** — deferred; leave context
  hooks where cheap.
- **Race-branch result cleanup** — losing `select_biased!` branches leave
  unconsumed results in `results`; fine for correctness, revisit if it bloats
  history.

---

## 11. Cloud migration boundary

Keep two traits clean and the desktop↔cloud swap is "reimplement two traits,"
not a rewrite:

- `History` — append events, read history for a run, atomic decision-turn commit.
- `TaskQueue` — enqueue/lease/complete activity tasks and timers; mark runnable.

The SQLite engine is one implementation of these; a server-backed engine is
another. **Workflow and activity code (Section 7) imports neither** — it only
sees `WfContext` / `ActivityContext`, so it carries over unchanged.
