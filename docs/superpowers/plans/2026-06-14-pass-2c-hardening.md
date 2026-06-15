# Pass 2c — Robustness Hardening (lease-expiry + dead-lettering) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close two robustness gaps that Pass 1 left as forward-looking risks (the
`pass2-hardening-backlog`): (1) a worker that crashes mid-activity strands its task
in `status='running'` forever — add a lease TTL and a reclaim sweep; (2) an
unregistered workflow type or a nondeterminism error leaves the run in `runnable`,
so the driver loop retries it forever — dead-letter it to a terminal `Failed`
instead.

**Architecture:** Activities are at-least-once (spec §5.2, §8); the missing piece is
crash recovery for an *in-flight* lease. We add `activity_tasks.lease_expires_at`
(set when a task is leased, with a fixed TTL) and a `reclaim_expired_activities`
sweep that returns expired `'running'` tasks to `'pending'` — re-leasable, at a
bumped attempt count. Separately, the decision driver currently propagates an `Err`
for an unregistered workflow type or a replay divergence, which leaves `runnable`
set and spins the loop; instead the driver **dead-letters** the run — commits a
terminal `Failed` turn (clearing `runnable`) and fires the completion observer — so
the failure surfaces to the frontend and the loop moves on. No workflow-crate
changes; this is engine + persist only.

**Tech Stack:** Rust 2021, rusqlite, tokio, serde_json, anyhow.

**Depends on:** Pass 2a (timers) — `TurnCommit` now carries `new_timers`, used in the
dead-letter commit. (Independent of Pass 2b; can be sequenced before or after it.)

---

## Canonical type additions (update the ROADMAP "Canonical types" list)

```rust
// crate `engine` — TaskQueue trait gains:
//   /// Return expired in-flight leases ('running' past their TTL) to 'pending'.
//   /// Returns the number reclaimed (spec §5.2 crash recovery).
//   async fn reclaim_expired_activities(&self) -> anyhow::Result<u64>;

// crate `persist` — schema: activity_tasks gains
//   lease_expires_at INTEGER   -- NULL when pending; epoch-ms deadline when leased
```

No changes to `Command`, `Event`, `CommandResult`, `TurnCommit`, or the `workflow`
crate.

---

## File Structure

```
/crates/persist/src/schema.rs        # MODIFY: activity_tasks gains lease_expires_at
/crates/persist/src/sqlite.rs         # MODIFY: idempotent migrate() for existing DBs
/crates/persist/src/taskqueue_impl.rs # MODIFY: set lease_expires_at on lease; reclaim impl; null it on reschedule
/crates/engine/src/traits.rs          # MODIFY: TaskQueue::reclaim_expired_activities
/crates/engine/src/engine.rs          # MODIFY: reclaim wrapper + sweep loop; dead-letter path
/crates/engine/tests/hardening.rs     # NEW: lease reclaim + dead-letter integration tests
```

---

### Task 1: Activity lease-expiry + reclaim sweep

**Files:**
- Modify: `crates/persist/src/schema.rs`, `sqlite.rs`, `taskqueue_impl.rs`,
  `crates/engine/src/traits.rs`, `crates/engine/src/engine.rs`

- [ ] **Step 1: Add the `lease_expires_at` column to the schema**

In `crates/persist/src/schema.rs`, in the `activity_tasks` table, add the column
after `next_run_at`:

```sql
CREATE TABLE IF NOT EXISTS activity_tasks (
  run_id        TEXT NOT NULL,
  seq           INTEGER NOT NULL,
  activity_type TEXT NOT NULL,
  input         BLOB,
  attempt       INTEGER NOT NULL DEFAULT 0,
  next_run_at   INTEGER NOT NULL,
  lease_expires_at INTEGER,             -- NULL when pending; epoch-ms deadline when leased
  status        TEXT NOT NULL,
  PRIMARY KEY (run_id, seq)
);
```

- [ ] **Step 2: Add an idempotent migration for pre-existing DBs**

A persisted desktop DB created before this change won't have the new column
(`CREATE TABLE IF NOT EXISTS` no-ops on the existing table). Add a guarded
`ALTER TABLE` that ignores the "already there" case.

In `crates/persist/src/sqlite.rs`, add a `migrate` fn and call it from both
constructors. Replace the `impl Sqlite` block and add the fn:

```rust
impl Sqlite {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }
}

/// Idempotent schema evolution for DBs created before a column existed. SQLite has
/// no `ADD COLUMN IF NOT EXISTS`, so we add and swallow the duplicate-column error.
fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    match conn.execute("ALTER TABLE activity_tasks ADD COLUMN lease_expires_at INTEGER", []) {
        Ok(_) => Ok(()),
        // Fresh DBs already have the column (from SCHEMA); old DBs get it added above.
        Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}
```

- [ ] **Step 3: Set `lease_expires_at` when leasing; null it on reschedule**

In `crates/persist/src/taskqueue_impl.rs`, add a TTL constant near the top of the
file (after the `use` lines):

```rust
/// How long a leased activity may run before the reclaim sweep considers it dead
/// and returns it to pending. Generous for desktop scale (spec §5.2).
const LEASE_TTL_MS: i64 = 30_000;
```

Update the `lease_activity` UPDATE to stamp the deadline (`now` is already bound at
the top of the method):

```rust
        let new_attempt = attempt + 1;
        tx.execute(
            "UPDATE activity_tasks SET status = 'running', attempt = ?3, lease_expires_at = ?4 \
             WHERE run_id = ?1 AND seq = ?2",
            params![run_id, seq, new_attempt, now + LEASE_TTL_MS],
        )?;
```

Update `reschedule_activity` to clear the deadline when a task returns to pending:

```rust
    async fn reschedule_activity(
        &self,
        lease: &ActivityLease,
        next_run_at: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE activity_tasks SET status = 'pending', next_run_at = ?3, lease_expires_at = NULL \
             WHERE run_id = ?1 AND seq = ?2",
            params![lease.run_id, lease.seq, next_run_at],
        )?;
        Ok(())
    }
```

- [ ] **Step 4: Add `reclaim_expired_activities` to the `TaskQueue` trait**

In `crates/engine/src/traits.rs`, add to the `TaskQueue` trait (after
`reschedule_activity`):

```rust
    /// Crash recovery: return in-flight leases whose TTL has elapsed (`status =
    /// 'running'` with `lease_expires_at <= now`) to `pending`, so a fresh worker
    /// can re-lease them. Returns the number reclaimed (spec §5.2 — at-least-once).
    async fn reclaim_expired_activities(&self) -> anyhow::Result<u64>;
```

- [ ] **Step 5: Write the failing reclaim tests**

Add to `crates/persist/src/taskqueue_impl.rs` `mod tests`:

```rust
    #[tokio::test]
    async fn expired_lease_is_reclaimed_and_releasable() {
        let db = db_with_task().await;
        let lease = db.lease_activity().await.unwrap().unwrap();
        assert_eq!(lease.attempt, 1);
        assert!(db.lease_activity().await.unwrap().is_none(), "a running task is not leasable");

        // Simulate the worker crashing and the lease TTL elapsing.
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE activity_tasks SET lease_expires_at = 0 WHERE run_id = ?1 AND seq = ?2",
                params![lease.run_id, lease.seq],
            )
            .unwrap();

        assert_eq!(db.reclaim_expired_activities().await.unwrap(), 1);
        let released = db
            .lease_activity()
            .await
            .unwrap()
            .expect("reclaimed task is leasable again");
        assert_eq!(released.attempt, 2, "re-lease after a crash counts as another attempt");
    }

    #[tokio::test]
    async fn live_lease_is_not_reclaimed() {
        let db = db_with_task().await;
        let _lease = db.lease_activity().await.unwrap().unwrap(); // TTL is in the future
        assert_eq!(
            db.reclaim_expired_activities().await.unwrap(),
            0,
            "a live lease must not be reclaimed"
        );
    }
```

Run: `cargo test -p persist --lib reclaim`
Expected: FAIL — `reclaim_expired_activities` is not implemented (compile error on
the trait method).

- [ ] **Step 6: Implement `reclaim_expired_activities`**

In `crates/persist/src/taskqueue_impl.rs`, add to `impl TaskQueue for Sqlite`
(after `fire_due_timer` from Pass 2a, or after `reschedule_activity`):

```rust
    async fn reclaim_expired_activities(&self) -> anyhow::Result<u64> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE activity_tasks SET status = 'pending', lease_expires_at = NULL \
             WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?1",
            params![now_ms()],
        )?;
        Ok(n as u64)
    }
```

- [ ] **Step 7: Add the engine wrapper + sweep loop**

In `crates/engine/src/engine.rs`, add an `impl Engine` block:

```rust
impl Engine {
    /// Reclaim expired in-flight activity leases (spec §5.2). Returns the count.
    pub async fn reclaim_expired_activities(&self) -> anyhow::Result<u64> {
        self.queue.reclaim_expired_activities().await
    }
}
```

In `start()`, add a sweep loop after the timer loop (before `engine`):

```rust
        let sweeper = engine.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = sweeper.reclaim_expired_activities().await {
                    eprintln!("lease sweep error: {err:#}");
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
```

- [ ] **Step 8: Build + test**

Run: `cargo test -p persist`
Expected: PASS — the two reclaim tests plus all prior persist tests.

Run: `cargo build -p engine`
Expected: compiles.

- [ ] **Step 9: Commit**

```bash
git add crates/persist crates/engine
git commit -m "feat(engine,persist): activity lease-expiry + reclaim sweep (crash recovery)"
```

---

### Task 2: Dead-letter unregistered workflows + nondeterminism

**Files:**
- Modify: `crates/engine/src/engine.rs`

- [ ] **Step 1: Add the `dead_letter` helper**

In `crates/engine/src/engine.rs`, add an `impl Engine` block (near
`process_one_runnable`):

```rust
impl Engine {
    /// Terminally fail a run that cannot make progress (unregistered type, replay
    /// divergence). Commits a `Failed` turn — which clears `runnable`, so the driver
    /// stops retrying — and fires the completion observer (spec §5.1, §14). Returns
    /// `Ok(true)` so the caller's loop continues without the error backoff.
    async fn dead_letter(
        &self,
        run_id: &str,
        workflow_id: &str,
        message: String,
    ) -> anyhow::Result<bool> {
        let err = workflow::Error::new(message);
        let result = Some(serde_json::to_vec(&err)?);
        let commit = TurnCommit {
            events: Vec::new(),
            new_tasks: Vec::new(),
            new_timers: Vec::new(),
            status: ExecStatus::Failed,
            result: result.clone(),
        };
        self.history.commit_turn(run_id, &commit).await?;
        if let Some(obs) = &self.observer {
            obs(RunCompleted {
                run_id: run_id.to_string(),
                workflow_id: workflow_id.to_string(),
                status: ExecStatus::Failed,
                result,
            });
        }
        Ok(true)
    }
}
```

- [ ] **Step 2: Route the two error sites through `dead_letter`**

In `process_one_runnable`, replace the unregistered-workflow lookup:

```rust
        let replay = match self.workflows.get(&meta.workflow_type) {
            Some(r) => r.clone(),
            None => {
                return self
                    .dead_letter(
                        &run_id,
                        &meta.workflow_id,
                        format!("unregistered workflow {}", meta.workflow_type),
                    )
                    .await;
            }
        };
```

…and replace the replay call that currently does `.map_err(…)?`:

```rust
        let outcome = match replay(info, &events) {
            Ok(o) => o,
            Err(e) => {
                return self
                    .dead_letter(
                        &run_id,
                        &meta.workflow_id,
                        format!("nondeterminism in {}: {e}", meta.workflow_type),
                    )
                    .await;
            }
        };
```

> The `meta.workflow_id` is moved into the `RunCompleted` only inside
> `dead_letter`; the borrow here is fine because both arms `return` before the
> later use of `meta.workflow_id` in the success path. If the borrow checker
> complains, clone it: `&meta.workflow_id.clone()`.

- [ ] **Step 3: Build the engine crate**

Run: `cargo build -p engine`
Expected: compiles.

- [ ] **Step 4: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): dead-letter unregistered-workflow + nondeterminism instead of spinning"
```

---

### Task 3: Pass-2c acceptance — reclaim + dead-letter integration tests

**Files:**
- Create: `crates/engine/tests/hardening.rs`

- [ ] **Step 1: Write the integration tests**

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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

// Two structs sharing one workflow TYPE but emitting different command streams.
// V1 schedules Add; V2 (registered on the "restart") schedules nothing before
// returning, so cold replay diverges from the recorded ActivityScheduled at seq 0.
struct SumV1;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for SumV1 {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "VersionedSum";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let a = ctx.activity::<Add>((1, 2)).await?;
        Ok(a)
    }
}
struct SumV2;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for SumV2 {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "VersionedSum";
    async fn run(_ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        Ok(99) // emits NO ScheduleActivity -> diverges from history's recorded seq 0
    }
}

fn engine_with<W: workflow::Definition>(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<W>();
    e.register_activity::<Add>();
    e
}

#[tokio::test]
async fn unregistered_workflow_is_dead_lettered() {
    let db = Sqlite::open_in_memory().unwrap();
    // Start a run of type "VersionedSum" but build an engine that registers NO
    // workflow of that type.
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut engine = Engine::new(h, q);
    engine.register_activity::<Add>(); // deliberately no register_workflow

    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    engine.on_run_completed(move |ev| {
        if matches!(ev.status, ExecStatus::Failed) {
            f.store(true, Ordering::SeqCst);
        }
    });

    // Use the History trait directly to create the run (start_workflow needs the
    // type only for serialization, which still works), then drive one turn.
    engine
        .start_workflow::<SumV1>((), StartOptions { id: "dead-1".into() })
        .await
        .unwrap();

    // One driver turn dead-letters instead of erroring/spinning.
    assert!(engine.process_one_runnable().await.unwrap());
    // The run is no longer runnable (would-be spin is gone).
    assert!(!engine.process_one_runnable().await.unwrap());

    let (_, status, _) = db.find_execution("dead-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Failed);
    assert!(fired.load(Ordering::SeqCst), "observer fires on dead-letter");
}

#[tokio::test]
async fn nondeterminism_is_dead_lettered_not_spun() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: run V1 one turn + one activity so history records ActivityScheduled
    // and ActivityCompleted at seq 0.
    {
        let engine = engine_with::<SumV1>(&db);
        engine
            .start_workflow::<SumV1>((), StartOptions { id: "nd-1".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // schedules Add seq 0
        assert!(engine.process_one_activity().await.unwrap()); // completes Add seq 0
    }

    // Phase 2: "restart" with V2 registered under the same TYPE. Cold replay emits
    // no command at seq 0 while history recorded one -> divergence -> dead-letter.
    let engine2 = engine_with::<SumV2>(&db);
    assert!(engine2.process_one_runnable().await.unwrap()); // dead-letters
    assert!(!engine2.process_one_runnable().await.unwrap(), "no spin: run cleared from runnable");

    let (_, status, _) = db.find_execution("nd-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Failed);
}

#[tokio::test]
async fn crashed_activity_lease_is_reclaimed_and_completes() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = engine_with::<SumV1>(&db);
    engine
        .start_workflow::<SumV1>((), StartOptions { id: "lease-1".into() })
        .await
        .unwrap();
    assert!(engine.process_one_runnable().await.unwrap()); // schedules Add seq 0

    // Simulate a worker that leased the task and then crashed: lease it, do NOT
    // complete it, and force its lease to expire.
    let lease = engine_lease(&db).await;
    db.conn_execute_expire(&lease).await;

    // Sweep reclaims it; a fresh worker run then completes the workflow.
    assert_eq!(engine.reclaim_expired_activities().await.unwrap(), 1);
    assert!(engine.process_one_activity().await.unwrap()); // re-leases + runs Add
    assert!(engine.process_one_runnable().await.unwrap()); // drives to completion

    let (_, status, result) = db.find_execution("lease-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 3);
}

// Test helpers: lease a task and expire its lease via the shared connection.
async fn engine_lease(db: &Sqlite) -> engine::ActivityLease {
    use engine::TaskQueue;
    db.lease_activity().await.unwrap().expect("a task is due")
}

trait ExpireExt {
    async fn conn_execute_expire(&self, lease: &engine::ActivityLease);
}
impl ExpireExt for Sqlite {
    async fn conn_execute_expire(&self, lease: &engine::ActivityLease) {
        // `Sqlite::conn` is pub(crate); from an integration test we instead reuse a
        // tiny pending->expired update through a fresh in-test connection is not
        // possible, so expire via reschedule semantics: set the lease into the past.
        // The persist crate exposes no setter, so this test drives expiry through the
        // public path below instead.
        let _ = lease;
        unreachable!("replaced in Step 2");
    }
}
```

> **Step 1 caveat:** the `crashed_activity_lease_is_reclaimed_and_completes` test
> needs to force a lease into the past, but `Sqlite::conn` is `pub(crate)` and an
> integration test (`tests/`) is a separate crate, so it cannot reach it. Step 2
> fixes this by exposing a tiny test-only seam on the engine. Do not try to run
> Step 1 as written — finish Step 2 first.

- [ ] **Step 2: Expose a minimal test seam for forcing lease expiry**

The integration test can't reach the private connection, and we don't want a
production "expire this lease" API. Add a **test-only** TTL override instead: make
the lease TTL a `0` so the lease is already expired the moment it's taken, by
adding a constructor used only in tests.

Simplest clean approach — add a `persist`-level helper guarded by a public method
that sets the lease deadline. In `crates/persist/src/sqlite.rs`, add:

```rust
impl Sqlite {
    /// Test/diagnostic helper: force a leased task's TTL into the past so the next
    /// `reclaim_expired_activities` reclaims it. Not used by the engine in
    /// production.
    pub fn expire_lease_for_test(&self, run_id: &str, seq: i64) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE activity_tasks SET lease_expires_at = 0 WHERE run_id = ?1 AND seq = ?2",
            rusqlite::params![run_id, seq],
        )?;
        Ok(())
    }
}
```

Then replace the helper block at the bottom of `hardening.rs` (the `engine_lease`
fn and `ExpireExt` trait) with:

```rust
async fn lease_one(db: &Sqlite) -> engine::ActivityLease {
    use engine::TaskQueue;
    db.lease_activity().await.unwrap().expect("a task is due")
}
```

…and rewrite the body of `crashed_activity_lease_is_reclaimed_and_completes` to use
it:

```rust
    let lease = lease_one(&db).await; // worker leases the task...
    db.expire_lease_for_test(&lease.run_id, lease.seq).unwrap(); // ...then "crashes"
```

(Remove the `engine_lease` fn, the `ExpireExt` trait, and its impl from Step 1 —
they were placeholders flagged for replacement.)

- [ ] **Step 3: Run the acceptance tests**

Run: `cargo test -p engine --test hardening`
Expected: all three PASS:
- `unregistered_workflow_is_dead_lettered`
- `nondeterminism_is_dead_lettered_not_spun`
- `crashed_activity_lease_is_reclaimed_and_completes`

- [ ] **Step 4: Commit**

```bash
git add crates/engine crates/persist
git commit -m "test(engine): pass 2c acceptance — dead-letter + lease reclaim; add expire_lease_for_test seam"
```

---

### Task 4: Whole-workspace green + clippy + roadmap update

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS (Pass 1, 2a, 2b, 2c).
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean. (If clippy
  flags the `msg.contains("duplicate column name")` string match, leave it — it is
  the standard SQLite duplicate-column probe; add `#[allow(clippy::...)]` only if a
  specific lint fires.)
- [ ] **Step 3:** In `docs/superpowers/plans/2026-06-13-durable-workflow-engine-ROADMAP.md`:
  - Mark chunk `2c` status `done`, Plan file `2026-06-14-pass-2c-hardening.md`.
  - Update the "Canonical types" section: add `TaskQueue::reclaim_expired_activities`
    and the `activity_tasks.lease_expires_at` column.
- [ ] **Step 4:** Commit:

```bash
git add -A
git commit -m "chore: pass 2c complete — hardening, clippy clean, roadmap updated"
```

---

## Notes / decisions

- **Why dead-letter an unregistered workflow** (rather than retry until it is
  registered): in this single-process desktop host, workflows are registered
  deterministically at startup. A run referencing a type the running binary does not
  register is a code/version mismatch, which spec §14 explicitly allows to "drain or
  abandon" on code change. Dead-lettering surfaces it via the observer instead of
  silently spinning. A richer recoverable/blocked status is future work.
- **Lease TTL is fixed at 30s**, ample for desktop activities. Activity heartbeating
  (extending a lease for long-running work) is **not** implemented here — it is the
  natural follow-on if/when an activity legitimately runs longer than the TTL.
- **Still deferred to Pass 5** (the last open item in `pass2-hardening-backlog`):
  replacing the `start()` busy-poll loops and `Handle::result` polling with
  `Notify`/channel wakeups. It is a pure-throughput optimization, and a clean
  implementation crosses the `History`/`TaskQueue` migration seam (§15) — a
  server-backed backend would signal differently — so it belongs with the Pass 5
  durability/seam work, not here.
- **Crash-loop bounding:** a task that crashes the worker process on every attempt is
  reclaimed and re-leased indefinitely (each crash bumps `attempt`, but the worker
  dies before it can evaluate exhaustion). Bounding this needs the worker to record a
  failed attempt before running the side effect, or a max-reclaim count — out of
  scope here; noted for Pass 5.

## Self-Review (completed during authoring)

- **Spec coverage:** §5.2 (activity at-least-once crash recovery — lease TTL +
  `reclaim_expired_activities`), §5.1 (dead-letter commits a terminal turn, clears
  `runnable`, fires the observer), §14 (abandon-on-code-change semantics for the
  unregistered/divergent cases). Folds in `pass2-hardening-backlog` items #2 (lease
  expiry) and #3 (poison-spin dead-lettering); #1 (observer double-fire) landed in
  Pass 2a; #4 (busy-poll) is explicitly deferred to Pass 5.
- **Placeholders:** the Step-1 test helpers in Task 3 are *intentional* throwaways
  flagged with an inline caveat and replaced in Step 2 (so the engineer reads them in
  order); no `TODO`/`unimplemented` survives into the committed test.
- **Type consistency:** `TaskQueue::reclaim_expired_activities(&self) -> Result<u64>`,
  `Sqlite::expire_lease_for_test(&self, &str, i64)`, `LEASE_TTL_MS: i64`,
  `lease_expires_at` column, `Engine::dead_letter(&self, &str, &str, String) ->
  Result<bool>`, and the existing `TurnCommit { …, new_timers, status, result }`
  (from Pass 2a) are used consistently across the persist/engine tasks.
```
