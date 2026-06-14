# Pass 1c — Backend Traits + SQLite Persist — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Define the migration-boundary traits (`engine::History`,
`engine::TaskQueue`, spec §15) and implement them over SQLite in the `persist`
crate, including the schema (spec §11) and the atomic decision-turn commit
(spec §5.1).

**Architecture:** One concrete `persist::Sqlite` type implements **both** traits
over a single `Arc<Mutex<rusqlite::Connection>>`, so transactions span all tables
even though the trait split is conceptual. Trait methods are `#[async_trait]` (for
a future async cloud backend) but do synchronous `rusqlite` work inside — fine at
desktop scale (parallelism = 1 on decisions). No driver yet; tests drive the
trait methods directly against a temp database.

**Tech Stack:** rusqlite (bundled), serde_json, anyhow, async-trait, tempfile (dev).

**Depends on:** chunks 1a, 1b (uses `workflow::{Event, CommandResult, RetryPolicy}`).

---

## File Structure

```
/crates/engine/Cargo.toml          # MODIFY: add async-trait, anyhow
/crates/engine/src/lib.rs          # MODIFY: module wiring
/crates/engine/src/types.rs        # NEW: ExecStatus, StoredEvent, NewActivityTask,
                                    #      TurnCommit, ActivityLease, CreateOutcome
/crates/engine/src/traits.rs       # NEW: History, TaskQueue
/crates/persist/Cargo.toml         # MODIFY: rusqlite, serde_json, anyhow, async-trait, tempfile(dev)
/crates/persist/src/lib.rs         # MODIFY: re-export Sqlite
/crates/persist/src/schema.rs      # NEW: CREATE TABLE batch
/crates/persist/src/sqlite.rs      # NEW: Sqlite struct + open
/crates/persist/src/history_impl.rs    # NEW: impl History for Sqlite
/crates/persist/src/taskqueue_impl.rs  # NEW: impl TaskQueue for Sqlite
```

---

### Task 1: `engine` shared types

**Files:**
- Modify: `crates/engine/Cargo.toml`, `crates/engine/src/lib.rs`
- Create: `crates/engine/src/types.rs`

- [ ] **Step 1: Add deps**

Set `crates/engine/Cargo.toml` `[dependencies]`:

```toml
[dependencies]
workflow    = { path = "../workflow" }
activity    = { path = "../activity" }
async-trait = { workspace = true }
anyhow      = "1"
```

(Add `anyhow = "1"` to `[workspace.dependencies]` too, and use
`anyhow = { workspace = true }` if you prefer; either is fine.)

- [ ] **Step 2: Create `types.rs`**

```rust
use workflow::{Event, RetryPolicy};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecStatus {
    Running,
    Completed,
    Failed,
}

impl ExecStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecStatus::Running => "running",
            ExecStatus::Completed => "completed",
            ExecStatus::Failed => "failed",
        }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "running" => Some(ExecStatus::Running),
            "completed" => Some(ExecStatus::Completed),
            "failed" => Some(ExecStatus::Failed),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredEvent {
    pub event_id: i64,
    pub event: Event,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewActivityTask {
    pub seq: i64,
    pub activity_type: String,
    pub input: Vec<u8>,
    pub next_run_at: i64, // epoch ms; <= now means runnable immediately
}

/// Everything a single decision turn commits atomically (spec §5.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnCommit {
    pub events: Vec<Event>,            // new history events emitted this turn
    pub new_tasks: Vec<NewActivityTask>,
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>,       // Some iff status != Running
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivityLease {
    pub run_id: String,
    pub workflow_id: String,
    pub seq: i64,
    pub activity_type: String,
    pub input: Vec<u8>,
    pub attempt: u32, // 1-based; this is the current attempt number
    pub retry: RetryPolicy, // read from the ActivityScheduled event by the queue
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CreateOutcome {
    Created,
    AlreadyExists,
}

/// Metadata for one run, resolved by `run_id` (driver needs this to build
/// `workflow::Info` and pick the replay closure).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunMeta {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub status: ExecStatus,
}
```

- [ ] **Step 3: Wire module**

Set `crates/engine/src/lib.rs`:

```rust
//! Backend-agnostic engine surface (traits + driver). Spec §5, §15.
mod types;
pub use types::*;
```

- [ ] **Step 4: Build**

Run: `cargo build -p engine`
Expected: compiles.

- [ ] **Step 5: Commit**

```bash
git add crates/engine Cargo.toml
git commit -m "feat(engine): shared persistence types"
```

---

### Task 2: `engine` traits

**Files:**
- Create: `crates/engine/src/traits.rs`
- Modify: `crates/engine/src/lib.rs`

- [ ] **Step 1: Create `traits.rs`**

```rust
use crate::{ActivityLease, CreateOutcome, ExecStatus, RunMeta, StoredEvent, TurnCommit};
use workflow::CommandResult;

/// History store + atomic decision-turn commit (spec §15).
#[async_trait::async_trait]
pub trait History: Send + Sync {
    /// Idempotent by workflow_id (start dedup, spec §7.1). On first creation,
    /// also appends `WorkflowStarted` and marks the run runnable. Returns the
    /// effective run_id (the new one, or the pre-existing one).
    async fn create_execution(
        &self,
        candidate_run_id: &str,
        workflow_id: &str,
        workflow_type: &str,
        input: &[u8],
    ) -> anyhow::Result<(CreateOutcome, String)>;

    async fn read_history(&self, run_id: &str) -> anyhow::Result<Vec<StoredEvent>>;

    /// Resolve a run_id to its metadata (workflow id/type/status).
    async fn load_run(&self, run_id: &str) -> anyhow::Result<Option<RunMeta>>;

    /// Atomically: append events, enqueue tasks, set status/result, clear
    /// runnable for this run (spec §5.1 — the exactly-once boundary).
    async fn commit_turn(&self, run_id: &str, commit: &TurnCommit) -> anyhow::Result<()>;

    /// (run_id, status, result) for a workflow_id, if it exists.
    async fn find_execution(
        &self,
        workflow_id: &str,
    ) -> anyhow::Result<Option<(String, ExecStatus, Option<Vec<u8>>)>>;
}

/// Work queue: activity tasks, timers (later), and the runnable set (spec §15).
#[async_trait::async_trait]
pub trait TaskQueue: Send + Sync {
    /// A run with unprocessed events, if any.
    async fn next_runnable(&self) -> anyhow::Result<Option<String>>;

    /// Lease one due pending activity task, marking it running and bumping its
    /// attempt count. Returns None if nothing is due.
    async fn lease_activity(&self) -> anyhow::Result<Option<ActivityLease>>;

    /// Terminal outcome: mark the task done, append the completion event, mark
    /// the run runnable — all in one transaction (spec §5.2).
    async fn complete_activity(
        &self,
        lease: &ActivityLease,
        result: CommandResult,
    ) -> anyhow::Result<()>;

    /// Non-terminal retry: return the task to pending with a new backoff time.
    async fn reschedule_activity(
        &self,
        lease: &ActivityLease,
        next_run_at: i64,
    ) -> anyhow::Result<()>;
}
```

- [ ] **Step 2: Wire module**

Append to `crates/engine/src/lib.rs`:

```rust
mod traits;
pub use traits::{History, TaskQueue};
```

- [ ] **Step 3: Build**

Run: `cargo build -p engine`
Expected: compiles (no impls yet).

- [ ] **Step 4: Commit**

```bash
git add crates/engine
git commit -m "feat(engine): History and TaskQueue traits"
```

---

### Task 3: `persist::Sqlite` + schema

**Files:**
- Modify: `crates/persist/Cargo.toml`, `crates/persist/src/lib.rs`
- Create: `crates/persist/src/schema.rs`, `crates/persist/src/sqlite.rs`

- [ ] **Step 1: Add deps**

Set `crates/persist/Cargo.toml`:

```toml
[package]
name = "persist"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
engine      = { path = "../engine" }
workflow    = { path = "../workflow" }
activity    = { path = "../activity" }
rusqlite    = { workspace = true }
serde_json  = { workspace = true }
async-trait = { workspace = true }
anyhow      = "1"

[dev-dependencies]
tempfile = { workspace = true }
tokio    = { workspace = true }
```

- [ ] **Step 2: Create `schema.rs`** (the spec §11 DDL)

```rust
pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS executions (
  run_id        TEXT PRIMARY KEY,
  workflow_id   TEXT NOT NULL,
  workflow_type TEXT NOT NULL,
  parent_run_id TEXT,
  parent_seq    INTEGER,
  input         BLOB,
  status        TEXT NOT NULL,
  result        BLOB,
  UNIQUE(workflow_id)
);
CREATE TABLE IF NOT EXISTS history (
  run_id   TEXT NOT NULL,
  event_id INTEGER NOT NULL,
  seq      INTEGER,
  kind     TEXT NOT NULL,
  payload  BLOB,
  ts       INTEGER NOT NULL,
  PRIMARY KEY (run_id, event_id)
);
CREATE TABLE IF NOT EXISTS activity_tasks (
  run_id        TEXT NOT NULL,
  seq           INTEGER NOT NULL,
  activity_type TEXT NOT NULL,
  input         BLOB,
  attempt       INTEGER NOT NULL DEFAULT 0,
  next_run_at   INTEGER NOT NULL,
  status        TEXT NOT NULL,
  PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS timers (
  run_id  TEXT NOT NULL,
  seq     INTEGER NOT NULL,
  fire_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS runnable (
  run_id TEXT PRIMARY KEY,
  since  INTEGER NOT NULL
);
"#;
```

- [ ] **Step 3: Create `sqlite.rs`**

```rust
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::schema::SCHEMA;

#[derive(Clone)]
pub struct Sqlite {
    pub(crate) conn: Arc<Mutex<Connection>>,
}

impl Sqlite {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }
}

/// Milliseconds since the Unix epoch. Engine-side wall clock (not subject to
/// workflow determinism — that is `ctx.now()`, deferred).
pub(crate) fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_tables() {
        let db = Sqlite::open_in_memory().unwrap();
        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN \
                 ('executions','history','activity_tasks','timers','runnable')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 5);
    }
}
```

- [ ] **Step 4: Wire `lib.rs`**

Set `crates/persist/src/lib.rs`:

```rust
//! SQLite implementation of engine::History + engine::TaskQueue (spec §11, §15).
mod schema;
mod sqlite;
mod history_impl;
mod taskqueue_impl;

pub use sqlite::Sqlite;
```

(The `history_impl`/`taskqueue_impl` modules are created in Tasks 4–5; add their
`mod` lines now and create empty files, or add the `mod` lines as you create them.)

- [ ] **Step 5: Run the schema test**

Run: `cargo test -p persist sqlite`
Expected: `open_creates_tables ... ok`.

- [ ] **Step 6: Commit**

```bash
git add crates/persist Cargo.toml
git commit -m "feat(persist): Sqlite handle + schema"
```

---

### Task 4: `impl History for Sqlite`

**Files:**
- Create: `crates/persist/src/history_impl.rs`

- [ ] **Step 1: Implement the trait**

```rust
use rusqlite::{params, OptionalExtension};

use engine::{CreateOutcome, ExecStatus, History, RunMeta, StoredEvent, TurnCommit};
use workflow::Event;

use crate::sqlite::{now_ms, Sqlite};

/// Encode an event to (seq, kind, payload-bytes) for a history row.
fn encode(event: &Event) -> (Option<i64>, &'static str, Vec<u8>) {
    let seq = match event {
        Event::ActivityScheduled { seq, .. }
        | Event::ActivityCompleted { seq, .. }
        | Event::ActivityFailed { seq, .. } => Some(*seq as i64),
        Event::WorkflowStarted { .. } => None,
    };
    let payload = serde_json::to_vec(event).expect("event serializes");
    (seq, event.kind(), payload)
}

fn next_event_id(tx: &rusqlite::Transaction, run_id: &str) -> rusqlite::Result<i64> {
    tx.query_row(
        "SELECT COALESCE(MAX(event_id), 0) + 1 FROM history WHERE run_id = ?1",
        params![run_id],
        |r| r.get(0),
    )
}

#[async_trait::async_trait]
impl History for Sqlite {
    async fn create_execution(
        &self,
        candidate_run_id: &str,
        workflow_id: &str,
        workflow_type: &str,
        input: &[u8],
    ) -> anyhow::Result<(CreateOutcome, String)> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let inserted = tx.execute(
            "INSERT OR IGNORE INTO executions \
             (run_id, workflow_id, workflow_type, input, status) \
             VALUES (?1, ?2, ?3, ?4, 'running')",
            params![candidate_run_id, workflow_id, workflow_type, input],
        )?;

        if inserted == 1 {
            // First creation: WorkflowStarted (event_id 1) + runnable.
            let (seq, kind, payload) = encode(&Event::WorkflowStarted { input: input.to_vec() });
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, 1, ?2, ?3, ?4, ?5)",
                params![candidate_run_id, seq, kind, payload, now_ms()],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
                params![candidate_run_id, now_ms()],
            )?;
            tx.commit()?;
            Ok((CreateOutcome::Created, candidate_run_id.to_string()))
        } else {
            let existing: String = tx.query_row(
                "SELECT run_id FROM executions WHERE workflow_id = ?1",
                params![workflow_id],
                |r| r.get(0),
            )?;
            tx.commit()?;
            Ok((CreateOutcome::AlreadyExists, existing))
        }
    }

    async fn read_history(&self, run_id: &str) -> anyhow::Result<Vec<StoredEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT event_id, payload FROM history WHERE run_id = ?1 ORDER BY event_id",
        )?;
        let rows = stmt.query_map(params![run_id], |r| {
            let event_id: i64 = r.get(0)?;
            let payload: Vec<u8> = r.get(1)?;
            Ok((event_id, payload))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (event_id, payload) = row?;
            let event: Event = serde_json::from_slice(&payload)?;
            out.push(StoredEvent { event_id, event });
        }
        Ok(out)
    }

    async fn load_run(&self, run_id: &str) -> anyhow::Result<Option<RunMeta>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT workflow_id, workflow_type, status FROM executions WHERE run_id = ?1",
                params![run_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(|(workflow_id, workflow_type, status)| RunMeta {
            run_id: run_id.to_string(),
            workflow_id,
            workflow_type,
            status: ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
        }))
    }

    async fn commit_turn(&self, run_id: &str, commit: &TurnCommit) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let mut event_id = next_event_id(&tx, run_id)?;
        for event in &commit.events {
            let (seq, kind, payload) = encode(event);
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![run_id, event_id, seq, kind, payload, now_ms()],
            )?;
            event_id += 1;
        }

        for task in &commit.new_tasks {
            tx.execute(
                "INSERT OR REPLACE INTO activity_tasks \
                 (run_id, seq, activity_type, input, attempt, next_run_at, status) \
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, 'pending')",
                params![run_id, task.seq, task.activity_type, task.input, task.next_run_at],
            )?;
        }

        tx.execute(
            "UPDATE executions SET status = ?2, result = ?3 WHERE run_id = ?1",
            params![run_id, commit.status.as_str(), commit.result],
        )?;
        tx.execute("DELETE FROM runnable WHERE run_id = ?1", params![run_id])?;

        tx.commit()?;
        Ok(())
    }

    async fn find_execution(
        &self,
        workflow_id: &str,
    ) -> anyhow::Result<Option<(String, ExecStatus, Option<Vec<u8>>)>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT run_id, status, result FROM executions WHERE workflow_id = ?1",
                params![workflow_id],
                |r| {
                    let run_id: String = r.get(0)?;
                    let status: String = r.get(1)?;
                    let result: Option<Vec<u8>> = r.get(2)?;
                    Ok((run_id, status, result))
                },
            )
            .optional()?;
        Ok(row.map(|(run_id, status, result)| {
            (run_id, ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running), result)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{ExecStatus, NewActivityTask};
    use workflow::RetryPolicy;

    #[tokio::test]
    async fn create_is_idempotent_by_workflow_id() {
        let db = Sqlite::open_in_memory().unwrap();
        let (o1, r1) = db.create_execution("run-1", "wf-A", "T", b"in").await.unwrap();
        let (o2, r2) = db.create_execution("run-2", "wf-A", "T", b"in").await.unwrap();
        assert_eq!(o1, CreateOutcome::Created);
        assert_eq!(o2, CreateOutcome::AlreadyExists);
        assert_eq!(r1, "run-1");
        assert_eq!(r2, "run-1"); // returns the pre-existing run
    }

    #[tokio::test]
    async fn create_writes_workflow_started_and_runnable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in").await.unwrap();
        let h = db.read_history("run-1").await.unwrap();
        assert_eq!(h.len(), 1);
        assert!(matches!(h[0].event, Event::WorkflowStarted { .. }));
        assert_eq!(<Sqlite as engine::TaskQueue>::next_runnable(&db).await.unwrap(), Some("run-1".into()));
    }

    #[tokio::test]
    async fn commit_turn_appends_clears_runnable_and_sets_status() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in").await.unwrap();

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
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();

        let h = db.read_history("run-1").await.unwrap();
        assert_eq!(h.len(), 2); // WorkflowStarted + ActivityScheduled
        assert!(matches!(h[1].event, Event::ActivityScheduled { seq: 0, .. }));
        // runnable cleared by the commit
        assert_eq!(<Sqlite as engine::TaskQueue>::next_runnable(&db).await.unwrap(), None);
    }
}
```

> The tests reference `engine::TaskQueue::next_runnable`, implemented in Task 5.
> Implement Task 5 before running this task's tests, or temporarily comment out
> the two `next_runnable` assertions, then restore them.

- [ ] **Step 2: Run (after Task 5) and commit**

```bash
git add crates/persist
git commit -m "feat(persist): impl History for Sqlite"
```

---

### Task 5: `impl TaskQueue for Sqlite`

**Files:**
- Create: `crates/persist/src/taskqueue_impl.rs`

- [ ] **Step 1: Implement the trait**

```rust
use rusqlite::{params, OptionalExtension};

use engine::{ActivityLease, TaskQueue};
use workflow::{CommandResult, Event, RetryPolicy};

use crate::sqlite::{now_ms, Sqlite};

#[async_trait::async_trait]
impl TaskQueue for Sqlite {
    async fn next_runnable(&self) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let run_id: Option<String> = conn
            .query_row(
                "SELECT run_id FROM runnable ORDER BY since LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(run_id)
    }

    async fn lease_activity(&self) -> anyhow::Result<Option<ActivityLease>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = now_ms();

        let row = tx
            .query_row(
                "SELECT t.run_id, t.seq, t.activity_type, t.input, t.attempt, e.workflow_id \
                 FROM activity_tasks t JOIN executions e ON e.run_id = t.run_id \
                 WHERE t.status = 'pending' AND t.next_run_at <= ?1 \
                 ORDER BY t.next_run_at LIMIT 1",
                params![now],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;

        let Some((run_id, seq, activity_type, input, attempt, workflow_id)) = row else {
            tx.commit()?;
            return Ok(None);
        };

        // Retry policy lives in the ActivityScheduled event payload (no extra column).
        let retry_payload: Vec<u8> = tx.query_row(
            "SELECT payload FROM history WHERE run_id = ?1 AND seq = ?2 AND kind = 'ActivityScheduled'",
            params![run_id, seq],
            |r| r.get(0),
        )?;
        let retry = match serde_json::from_slice::<Event>(&retry_payload)? {
            Event::ActivityScheduled { retry, .. } => retry,
            _ => RetryPolicy::none(),
        };

        let new_attempt = attempt + 1;
        tx.execute(
            "UPDATE activity_tasks SET status = 'running', attempt = ?3 \
             WHERE run_id = ?1 AND seq = ?2",
            params![run_id, seq, new_attempt],
        )?;
        tx.commit()?;

        Ok(Some(ActivityLease {
            run_id,
            workflow_id,
            seq,
            activity_type,
            input,
            attempt: new_attempt as u32,
            retry,
        }))
    }

    async fn complete_activity(
        &self,
        lease: &ActivityLease,
        result: CommandResult,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let event = match result {
            CommandResult::ActivityCompleted(output) => {
                Event::ActivityCompleted { seq: lease.seq as u64, output }
            }
            CommandResult::ActivityFailed(error) => {
                Event::ActivityFailed { seq: lease.seq as u64, error }
            }
        };
        let payload = serde_json::to_vec(&event)?;
        let next_id: i64 = tx.query_row(
            "SELECT COALESCE(MAX(event_id), 0) + 1 FROM history WHERE run_id = ?1",
            params![lease.run_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![lease.run_id, next_id, lease.seq, event.kind(), payload, now_ms()],
        )?;
        tx.execute(
            "UPDATE activity_tasks SET status = 'done' WHERE run_id = ?1 AND seq = ?2",
            params![lease.run_id, lease.seq],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
            params![lease.run_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(())
    }

    async fn reschedule_activity(
        &self,
        lease: &ActivityLease,
        next_run_at: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE activity_tasks SET status = 'pending', next_run_at = ?3 \
             WHERE run_id = ?1 AND seq = ?2",
            params![lease.run_id, lease.seq, next_run_at],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{ExecStatus, History, NewActivityTask, TurnCommit};
    use workflow::RetryPolicy;

    async fn db_with_task() -> Sqlite {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in").await.unwrap();
        let commit = TurnCommit {
            events: vec![Event::ActivityScheduled {
                seq: 0, activity_type: "Add".into(), input: b"[1,2]".to_vec(), retry: RetryPolicy::none(),
            }],
            new_tasks: vec![NewActivityTask { seq: 0, activity_type: "Add".into(), input: b"[1,2]".to_vec(), next_run_at: 0 }],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();
        db
    }

    #[tokio::test]
    async fn lease_then_complete_appends_event_and_makes_runnable() {
        let db = db_with_task().await;

        let lease = db.lease_activity().await.unwrap().expect("a task is due");
        assert_eq!(lease.seq, 0);
        assert_eq!(lease.attempt, 1);
        assert_eq!(lease.workflow_id, "wf-A");
        // leased task is no longer pending
        assert!(db.lease_activity().await.unwrap().is_none());

        db.complete_activity(&lease, CommandResult::ActivityCompleted(b"3".to_vec()))
            .await
            .unwrap();

        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(h.last().unwrap().event, Event::ActivityCompleted { seq: 0, .. }));
        assert_eq!(db.next_runnable().await.unwrap(), Some("run-1".into()));
    }

    #[tokio::test]
    async fn reschedule_makes_task_leasable_again() {
        let db = db_with_task().await;
        let lease = db.lease_activity().await.unwrap().unwrap();
        db.reschedule_activity(&lease, 0).await.unwrap();
        assert!(db.lease_activity().await.unwrap().is_some());
    }
}
```

- [ ] **Step 2: Run all persist tests**

Run: `cargo test -p persist`
Expected: schema, History, and TaskQueue tests all PASS. (Restore the
`next_runnable` assertions in Task 4 now if you commented them out.)

- [ ] **Step 3: Commit**

```bash
git add crates/persist
git commit -m "feat(persist): impl TaskQueue for Sqlite"
```

---

### Task 6: Whole-workspace green + clippy

- [ ] **Step 1:** Run `cargo test` — all crates PASS.
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean; fix any
  `OptionalExtension`/unused-import nits.
- [ ] **Step 3:** Commit if changes: `git commit -am "chore: clippy-clean pass 1c"`.

---

## Self-Review (completed during authoring)

- **Spec coverage:** §11 schema (all five tables), §5.1 atomic commit
  (`commit_turn` single tx: events + tasks + status + clear runnable), §5.2
  activity completion (`complete_activity` single tx), §7.1 start dedup
  (`create_execution` idempotent by workflow_id), §15 two traits in `engine`,
  implemented in `persist`. Timers table exists but is unused until Pass 2.
- **Placeholders:** none; the only cross-task ordering caveat (History tests use
  `next_runnable` from Task 5) is called out explicitly with a workaround.
- **Type consistency:** `History`/`TaskQueue` method signatures match `traits.rs`;
  `TurnCommit`, `NewActivityTask`, `ActivityLease`, `ExecStatus`, `CreateOutcome`,
  `StoredEvent` used consistently; event encode/decode round-trips via serde_json.
