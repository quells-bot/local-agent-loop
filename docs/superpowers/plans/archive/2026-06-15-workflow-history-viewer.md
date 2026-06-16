# Workflow History Viewer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a developer-facing workflow history viewer — a native menu item opens a separate window at `/history` that lists root runs and drills into a humanized, decoded event timeline.

**Architecture:** Two read-model methods (`list_executions`, `read_events`) are added to the Tauri-agnostic `engine::History` trait and implemented in `persist::Sqlite`, kept separate from the replay-critical `StoredEvent`. The `src-tauri` host exposes them as `list_runs` / `run_detail` commands, decoding event payloads to generic JSON so the host stays demo-agnostic, and wires a "Workflow History…" menu item that opens a second `WebviewWindow` at `/history`. A plain SvelteKit master–detail route renders the list and timeline, with snapshot-on-open + refresh-on-focus.

**Tech Stack:** Rust (rusqlite, async-trait, tokio, serde_json), Tauri v2 (menu + multi-window), SvelteKit (Svelte 5 runes, JSDoc), Vitest.

**Spec:** `docs/superpowers/specs/2026-06-15-workflow-history-viewer-design.md`

---

## File Structure

- `crates/engine/src/types.rs` (modify) — add read-model types `ExecutionSummary`, `HistoryRecord`.
- `crates/engine/src/traits.rs` (modify) — add `list_executions` + `read_events` to `History`.
- `crates/engine/tests/migration_seam.rs` (modify) — one-line note that read-model methods ride on `History`.
- `crates/persist/src/history_impl.rs` (modify) — implement both methods + tests.
- `src-tauri/Cargo.toml` (modify) — add `activity` dev-dependency (for one test).
- `src-tauri/src/lib.rs` (modify) — DTOs, `event_to_json`, `list_runs`/`run_detail` commands, manage `Arc<dyn History>`, menu + history-window wiring, register commands.
- `src-tauri/capabilities/default.json` (modify) — grant the `history` window permissions.
- `src/lib/historyView.js` (create) — pure render helpers.
- `src/lib/historyView.test.js` (create) — Vitest for the helpers.
- `src/routes/history/+page.svelte` (create) — master–detail viewer route.

---

## Task 1: Engine read-model types + `History` methods + persist impls

**Files:**
- Modify: `crates/engine/src/types.rs`
- Modify: `crates/engine/src/traits.rs`
- Modify: `crates/engine/tests/migration_seam.rs`
- Modify (impl + tests): `crates/persist/src/history_impl.rs`

- [ ] **Step 1: Add read-model types to `engine::types`**

Append to `crates/engine/src/types.rs` (it already has `use workflow::{Event, RetryPolicy};`):

```rust
/// Read-model summary of one root run for the history viewer (history-viewer
/// design §4.1). NOT part of the exactly-once boundary. `started_at` /
/// `last_event_at` / `event_count` are derived from the run's `history` rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub status: ExecStatus,
    pub started_at: i64,    // epoch ms — min(history.ts)
    pub last_event_at: i64, // epoch ms — max(history.ts)
    pub event_count: i64,
}

/// Read-model row of a run's timeline (history-viewer design §4.1). The viewer
/// analog of `StoredEvent`, but carries `ts` and a resolved `child_run_id` for
/// `ChildScheduled`/`ChildCompleted`. Deliberately distinct from `StoredEvent`
/// so the determinism/replay path is untouched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRecord {
    pub event_id: i64,
    pub ts: i64, // epoch ms (history.ts)
    pub event: Event,
    pub child_run_id: Option<String>,
}
```

- [ ] **Step 2: Add the two read-model methods to the `History` trait**

In `crates/engine/src/traits.rs`, extend the `use crate::{...}` line to include the new types:

```rust
use crate::{
    ActivityLease, CreateOutcome, ExecStatus, ExecutionSummary, HistoryRecord, RunMeta,
    SignalOutcome, StoredEvent, TurnCommit,
};
```

Then add these two methods inside `pub trait History` (e.g. right after `find_execution`):

```rust
    /// Read model for the history viewer (history-viewer design §4.2). NOT part of
    /// the exactly-once boundary: the engine never calls these. They ride on
    /// `History` because they are history-store reads, keeping the migration seam
    /// at two traits. Root executions only (`parent_run_id IS NULL`), newest first.
    async fn list_executions(&self) -> anyhow::Result<Vec<ExecutionSummary>>;

    /// Read model for the history viewer: all events for a run in `event_id` order,
    /// carrying timestamps. For `ChildScheduled`/`ChildCompleted`, resolves the
    /// child's run_id from `executions`. NOT used by replay.
    async fn read_events(&self, run_id: &str) -> anyhow::Result<Vec<HistoryRecord>>;
```

- [ ] **Step 3: Add a seam note to the migration-seam test**

In `crates/engine/tests/migration_seam.rs`, add one line to the module doc comment (after the existing `//! Spec references…` line) so the deliberate read-model choice is documented:

```rust
//! Read-model methods (`list_executions`, `read_events`) deliberately ride on
//! `History` rather than a third trait, keeping this seam at exactly two traits.
```

- [ ] **Step 4: Add compiling stubs so the workspace builds**

In `crates/persist/src/history_impl.rs`, update the `use engine::{...}` line to add the new types, then add stub method bodies at the end of `impl History for Sqlite` (these are replaced in later steps; never committed):

```rust
use engine::{
    CreateOutcome, ExecStatus, ExecutionSummary, History, HistoryRecord, RunMeta, SignalOutcome,
    StoredEvent, TurnCommit,
};
```

```rust
    async fn list_executions(&self) -> anyhow::Result<Vec<ExecutionSummary>> {
        todo!("step 6")
    }

    async fn read_events(&self, _run_id: &str) -> anyhow::Result<Vec<HistoryRecord>> {
        todo!("step 9")
    }
```

- [ ] **Step 5: Write failing tests for `list_executions`**

Add to the `#[cfg(test)] mod tests` block in `crates/persist/src/history_impl.rs`. Extend the test-module imports to include `NewChild`:

```rust
    use engine::{ExecStatus, NewActivityTask, NewChild, SignalOutcome};
```

```rust
    #[tokio::test]
    async fn list_executions_returns_roots_only() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-A", "wf-A", "Parent", b"in-A")
            .await
            .unwrap();
        db.create_execution("run-B", "wf-B", "Parent", b"in-B")
            .await
            .unwrap();
        // run-B spawns a child; the child must NOT appear in the list.
        let commit = TurnCommit {
            events: vec![Event::ChildScheduled {
                seq: 0,
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![NewChild {
                seq: 0,
                child_run_id: "child-run".into(),
                child_workflow_id: "wf-B::child::0".into(),
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-B", &commit).await.unwrap();

        let runs = db.list_executions().await.unwrap();
        let ids: Vec<&str> = runs.iter().map(|r| r.run_id.as_str()).collect();
        assert!(ids.contains(&"run-A"));
        assert!(ids.contains(&"run-B"));
        assert!(!ids.contains(&"child-run"), "children are not roots");

        // newest-first: sorted by started_at descending.
        assert!(runs.windows(2).all(|w| w[0].started_at >= w[1].started_at));

        let b = runs.iter().find(|r| r.run_id == "run-B").unwrap();
        assert_eq!(b.workflow_type, "Parent");
        assert_eq!(b.status, ExecStatus::Running);
        assert_eq!(b.event_count, 2); // WorkflowStarted + ChildScheduled
        assert!(b.started_at <= b.last_event_at);
    }
```

- [ ] **Step 6: Run the test to confirm it fails, then implement `list_executions`**

Run: `cargo test -p persist list_executions_returns_roots_only`
Expected: FAIL — panics at `todo!("step 6")`.

Replace the `list_executions` stub:

```rust
    async fn list_executions(&self) -> anyhow::Result<Vec<ExecutionSummary>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.run_id, e.workflow_id, e.workflow_type, e.status, \
                    MIN(h.ts), MAX(h.ts), COUNT(h.event_id) \
             FROM executions e JOIN history h ON h.run_id = e.run_id \
             WHERE e.parent_run_id IS NULL \
             GROUP BY e.run_id \
             ORDER BY MIN(h.ts) DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (run_id, workflow_id, workflow_type, status, started_at, last_event_at, event_count) =
                row?;
            out.push(ExecutionSummary {
                run_id,
                workflow_id,
                workflow_type,
                status: ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
                started_at,
                last_event_at,
                event_count,
            });
        }
        Ok(out)
    }
```

Run: `cargo test -p persist list_executions_returns_roots_only`
Expected: PASS.

- [ ] **Step 7: Write failing tests for `read_events`**

Add to the same test module:

```rust
    #[tokio::test]
    async fn read_events_carries_ts_in_order_without_child_ids() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-1", "Parent", b"in")
            .await
            .unwrap();
        let commit = TurnCommit {
            events: vec![Event::ActivityScheduled {
                seq: 0,
                activity_type: "Parse".into(),
                input: b"\"1 2\"".to_vec(),
                retry: RetryPolicy::none(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();

        let evs = db.read_events("run-1").await.unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].event_id, 1);
        assert!(matches!(evs[0].event, Event::WorkflowStarted { .. }));
        assert_eq!(evs[1].event_id, 2);
        assert!(matches!(evs[1].event, Event::ActivityScheduled { seq: 0, .. }));
        assert!(evs[0].ts > 0 && evs[1].ts >= evs[0].ts);
        assert!(evs.iter().all(|e| e.child_run_id.is_none()));
    }

    #[tokio::test]
    async fn read_events_resolves_child_run_id_for_child_events() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("parent-run", "parent-wf", "Parent", b"in")
            .await
            .unwrap();
        let commit = TurnCommit {
            events: vec![Event::ChildScheduled {
                seq: 0,
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![NewChild {
                seq: 0,
                child_run_id: "child-run".into(),
                child_workflow_id: "parent-wf::child::0".into(),
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("parent-run", &commit).await.unwrap();

        let evs = db.read_events("parent-run").await.unwrap();
        let child = evs
            .iter()
            .find(|e| matches!(e.event, Event::ChildScheduled { .. }))
            .unwrap();
        assert_eq!(child.child_run_id.as_deref(), Some("child-run"));
    }

    #[tokio::test]
    async fn read_events_empty_for_unknown_run() {
        let db = Sqlite::open_in_memory().unwrap();
        let evs = db.read_events("nope").await.unwrap();
        assert!(evs.is_empty());
    }
```

- [ ] **Step 8: Run the tests to confirm they fail, then implement `read_events`**

Run: `cargo test -p persist read_events`
Expected: FAIL — panics at `todo!("step 9")`.

Replace the `read_events` stub (`OptionalExtension` is already imported at the top of the file via `use rusqlite::{params, OptionalExtension};`):

```rust
    async fn read_events(&self, run_id: &str) -> anyhow::Result<Vec<HistoryRecord>> {
        let conn = self.conn.lock().unwrap();
        // Collect raw rows first, dropping the prepared statement before the
        // per-child lookups below.
        let raw: Vec<(i64, Vec<u8>, i64)> = {
            let mut stmt = conn.prepare(
                "SELECT event_id, payload, ts FROM history WHERE run_id = ?1 ORDER BY event_id",
            )?;
            let rows = stmt.query_map(params![run_id], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, i64>(2)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut out = Vec::with_capacity(raw.len());
        for (event_id, payload, ts) in raw {
            let event: Event = serde_json::from_slice(&payload)?;
            let child_run_id = match &event {
                Event::ChildScheduled { seq, .. } | Event::ChildCompleted { seq, .. } => conn
                    .query_row(
                        "SELECT run_id FROM executions \
                         WHERE parent_run_id = ?1 AND parent_seq = ?2",
                        params![run_id, *seq as i64],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?,
                _ => None,
            };
            out.push(HistoryRecord {
                event_id,
                ts,
                event,
                child_run_id,
            });
        }
        Ok(out)
    }
```

Run: `cargo test -p persist read_events`
Expected: PASS (all three).

- [ ] **Step 9: Run the full engine + persist suites**

Run: `cargo test -p engine -p persist`
Expected: PASS, including `migration_seam` (still compiles with the two-trait assertion).

- [ ] **Step 10: Commit**

```bash
git add crates/engine/src/types.rs crates/engine/src/traits.rs \
        crates/engine/tests/migration_seam.rs crates/persist/src/history_impl.rs
git commit -m "feat(engine): history read-model (list_executions, read_events)"
```

---

## Task 2: Host commands — DTOs, `event_to_json`, `list_runs`/`run_detail`

**Files:**
- Modify: `src-tauri/Cargo.toml`
- Modify (impl + tests): `src-tauri/src/lib.rs`

- [ ] **Step 1: Add the `activity` dev-dependency**

In `src-tauri/Cargo.toml`, add a `[dev-dependencies]` section (one test constructs an `activity::Error`):

```toml
[dev-dependencies]
activity = { path = "../crates/activity" }
```

- [ ] **Step 2: Add imports, DTOs, and the decode helpers to `src-tauri/src/lib.rs`**

Extend the existing `use engine::{...}` line and add a `workflow` import near the other `use` lines at the top of the file:

```rust
use engine::{
    Engine, ExecStatus, ExecutionSummary, History, HistoryRecord, RunCompleted, StartOptions,
    TaskQueue,
};
use workflow::{ChildResult, Event};
```

Add these types and helpers (place them above `pub fn run()`):

```rust
/// One row of the run list, pushed to the `/history` viewer.
#[derive(Clone, Serialize)]
struct RunSummaryDto {
    run_id: String,
    workflow_id: String,
    workflow_type: String,
    status: String,
    started_at: i64,
    last_event_at: i64,
    event_count: i64,
}

/// One humanized timeline row. `detail` carries the decoded, demo-agnostic
/// payload fields (input/output/error/…), flattened alongside the envelope fields.
#[derive(Clone, Serialize)]
struct EventDto {
    event_id: i64,
    ts: i64,
    kind: String,
    #[serde(flatten)]
    detail: serde_json::Value,
    child_run_id: Option<String>,
}

#[derive(Clone, Serialize)]
struct RunDetailDto {
    summary: RunSummaryDto,
    events: Vec<EventDto>,
}

fn summary_dto(s: ExecutionSummary) -> RunSummaryDto {
    RunSummaryDto {
        run_id: s.run_id,
        workflow_id: s.workflow_id,
        workflow_type: s.workflow_type,
        status: s.status.as_str().to_string(),
        started_at: s.started_at,
        last_event_at: s.last_event_at,
        event_count: s.event_count,
    }
}

/// Decode an event's inner byte payload to JSON so the host stays demo-agnostic.
/// Non-JSON bytes (shouldn't happen for our payloads) decode to `null`.
fn decode(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or(serde_json::Value::Null)
}

/// Humanize a `ChildResult`, decoding its inner output / failure-error.
fn child_result_json(result: &ChildResult) -> serde_json::Value {
    use serde_json::json;
    match result {
        ChildResult::Completed(output) => json!({ "status": "completed", "output": decode(output) }),
        ChildResult::Failed(error) => json!({ "status": "failed", "error": error }),
    }
}

/// Map a read-model `HistoryRecord` to the humanized DTO the viewer renders.
/// Inner byte payloads are decoded to generic JSON (mirrors `completion_payload`,
/// keeping `src-tauri` demo-agnostic). Unit-testable without a running Tauri app.
fn event_to_json(record: HistoryRecord) -> EventDto {
    use serde_json::json;
    let kind = record.event.kind().to_string();
    let detail = match &record.event {
        Event::WorkflowStarted { input } => json!({ "input": decode(input) }),
        Event::ActivityScheduled {
            seq,
            activity_type,
            input,
            retry,
        } => json!({
            "seq": seq,
            "activity_type": activity_type,
            "input": decode(input),
            "retry": retry,
        }),
        Event::ActivityCompleted { seq, output } => {
            json!({ "seq": seq, "output": decode(output) })
        }
        Event::ActivityFailed { seq, error } => json!({ "seq": seq, "error": error }),
        Event::TimerStarted { seq, duration_ms } => {
            json!({ "seq": seq, "duration_ms": duration_ms })
        }
        Event::TimerFired { seq } => json!({ "seq": seq }),
        Event::SignalReceived { name, payload } => {
            json!({ "name": name, "payload": decode(payload) })
        }
        Event::ChildScheduled {
            seq,
            workflow_type,
            input,
        } => json!({ "seq": seq, "workflow_type": workflow_type, "input": decode(input) }),
        Event::ChildCompleted { seq, result } => {
            json!({ "seq": seq, "result": child_result_json(result) })
        }
        Event::Patched { change_id } => json!({ "change_id": change_id }),
    };
    EventDto {
        event_id: record.event_id,
        ts: record.ts,
        kind,
        detail,
        child_run_id: record.child_run_id,
    }
}
```

- [ ] **Step 3: Write failing tests for `event_to_json`**

Add to the existing `#[cfg(test)] mod tests` block in `src-tauri/src/lib.rs` (it already has `use super::*;` and `use serde_json::json;`):

```rust
    fn record(event_id: i64, event: Event, child_run_id: Option<String>) -> HistoryRecord {
        HistoryRecord {
            event_id,
            ts: 1_700_000_000_000,
            event,
            child_run_id,
        }
    }

    #[test]
    fn event_to_json_humanizes_activity_scheduled() {
        let dto = event_to_json(record(
            2,
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Parse".into(),
                input: serde_json::to_vec(&json!({ "text": "1 2 3" })).unwrap(),
                retry: workflow::RetryPolicy::none(),
            },
            None,
        ));
        assert_eq!(dto.kind, "ActivityScheduled");
        assert_eq!(dto.event_id, 2);
        assert_eq!(dto.ts, 1_700_000_000_000);
        assert_eq!(dto.detail["activity_type"], "Parse");
        assert_eq!(dto.detail["input"], json!({ "text": "1 2 3" }));
        assert!(dto.child_run_id.is_none());
    }

    #[test]
    fn event_to_json_passes_child_run_id_for_child_events() {
        let dto = event_to_json(record(
            3,
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "SumChild".into(),
                input: b"[1,2]".to_vec(),
            },
            Some("child-run".into()),
        ));
        assert_eq!(dto.kind, "ChildScheduled");
        assert_eq!(dto.detail["workflow_type"], "SumChild");
        assert_eq!(dto.child_run_id.as_deref(), Some("child-run"));
    }

    #[test]
    fn event_to_json_decodes_activity_failure_error() {
        let dto = event_to_json(record(
            4,
            Event::ActivityFailed {
                seq: 0,
                error: activity::Error::fatal("could not parse 'two' as an integer"),
            },
            None,
        ));
        assert_eq!(dto.kind, "ActivityFailed");
        assert!(dto.detail["error"]["message"]
            .as_str()
            .unwrap()
            .contains("two"));
    }
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test -p app event_to_json`
Expected: PASS (the implementation from Step 2 already satisfies them).

> Note: these tests exercise the pure mapping written in Step 2. If a test fails, the bug is in `event_to_json`/`child_result_json` — fix there, not in the test.

- [ ] **Step 5: Add the `list_runs` and `run_detail` commands**

Add after the `submit` command in `src-tauri/src/lib.rs`:

```rust
/// List root runs for the history viewer, newest first (history-viewer design §5.1).
#[tauri::command]
async fn list_runs(
    history: State<'_, Arc<dyn History>>,
) -> Result<Vec<RunSummaryDto>, String> {
    let runs = history.list_executions().await.map_err(|e| e.to_string())?;
    Ok(runs.into_iter().map(summary_dto).collect())
}

/// Full detail (header + humanized timeline) for one run (history-viewer design §5.1).
#[tauri::command]
async fn run_detail(
    run_id: String,
    history: State<'_, Arc<dyn History>>,
) -> Result<RunDetailDto, String> {
    let meta = history
        .load_run(&run_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("run not found: {run_id}"))?;
    let records = history.read_events(&run_id).await.map_err(|e| e.to_string())?;
    let started_at = records.first().map(|r| r.ts).unwrap_or(0);
    let last_event_at = records.last().map(|r| r.ts).unwrap_or(0);
    let summary = RunSummaryDto {
        run_id: meta.run_id,
        workflow_id: meta.workflow_id,
        workflow_type: meta.workflow_type,
        status: meta.status.as_str().to_string(),
        started_at,
        last_event_at,
        event_count: records.len() as i64,
    };
    let events = records.into_iter().map(event_to_json).collect();
    Ok(RunDetailDto { summary, events })
}
```

- [ ] **Step 6: Manage `Arc<dyn History>` and register the commands**

In `pub fn run()`'s `setup`, the line that builds the engine currently moves `history`. Clone it instead, and manage the clone so the commands can resolve it. Change:

```rust
            let history: Arc<dyn History> = Arc::new(db.clone());
            let queue: Arc<dyn TaskQueue> = Arc::new(db.clone());
            let mut engine = Engine::new(history, queue);
```

to:

```rust
            let history: Arc<dyn History> = Arc::new(db.clone());
            let queue: Arc<dyn TaskQueue> = Arc::new(db.clone());
            let mut engine = Engine::new(history.clone(), queue);
```

Then, just before `Ok(())` at the end of `setup`, add:

```rust
            // Expose the read model to the history viewer commands.
            app.manage(history);
```

Finally, register the new commands in the invoke handler. Change:

```rust
        .invoke_handler(tauri::generate_handler![submit])
```

to:

```rust
        .invoke_handler(tauri::generate_handler![submit, list_runs, run_detail])
```

- [ ] **Step 7: Build and run the host test suite**

Run: `cargo test -p app`
Expected: PASS (existing `completion_payload` tests + the new `event_to_json` tests). Confirms `lib.rs` compiles with the new commands and managed state.

- [ ] **Step 8: Commit**

```bash
git add src-tauri/Cargo.toml src-tauri/src/lib.rs
git commit -m "feat(app): list_runs/run_detail commands + humanized event_to_json"
```

---

## Task 3: Menu item + history window + capability

**Files:**
- Modify: `src-tauri/src/lib.rs`
- Modify: `src-tauri/capabilities/default.json`

- [ ] **Step 1: Grant the `history` window permissions**

In `src-tauri/capabilities/default.json`, add `"history"` to the `windows` array so the second window gets the same core/event permissions as `main`:

```json
  "windows": ["main", "history"],
```

- [ ] **Step 2: Add the menu imports**

At the top of `src-tauri/src/lib.rs`, add:

```rust
use tauri::menu::{MenuBuilder, MenuItemBuilder, SubmenuBuilder};
```

- [ ] **Step 3: Build the menu and handle its event in `setup`**

Inside `setup`, after `app.manage(history);` (and before `Ok(())`), add:

```rust
            // "View ▸ Workflow History…" opens a second window at /history.
            let history_item =
                MenuItemBuilder::with_id("history", "Workflow History…").build(app)?;
            let view_menu = SubmenuBuilder::new(app, "View")
                .item(&history_item)
                .build()?;
            let menu = MenuBuilder::new(app).item(&view_menu).build()?;
            app.set_menu(menu)?;

            app.on_menu_event(move |app, event| {
                if event.id().as_ref() == "history" {
                    if let Some(w) = app.get_webview_window("history") {
                        let _ = w.set_focus();
                    } else {
                        let _ = tauri::WebviewWindowBuilder::new(
                            app,
                            "history",
                            tauri::WebviewUrl::App("history".into()),
                        )
                        .title("Workflow History")
                        .inner_size(900.0, 640.0)
                        .build();
                    }
                }
            });
```

> Verify-during-impl: the exact menu-event id comparison can vary across Tauri 2.x point releases. If `event.id().as_ref()` does not compile, check the installed version with `cargo doc -p tauri --open` (look at `tauri::menu::MenuId`) — `event.id().0.as_str() == "history"` is the fallback form. `set_menu`/`on_menu_event` are on `App` in `setup`.

- [ ] **Step 4: Compile the host**

Run: `cargo build -p app`
Expected: builds cleanly. (The menu/window behavior is verified manually in Task 5.)

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/lib.rs src-tauri/capabilities/default.json
git commit -m "feat(app): View menu opens the history window at /history"
```

---

## Task 4: Frontend pure render helpers

**Files:**
- Create: `src/lib/historyView.js`
- Create (test): `src/lib/historyView.test.js`

- [ ] **Step 1: Write the failing test**

Create `src/lib/historyView.test.js`:

```js
import { describe, it, expect } from 'vitest';
import { eventLabel, statusClass, formatTime, pushCrumb, popTo } from './historyView.js';

describe('eventLabel', () => {
  it('labels an activity-scheduled row with type and seq', () => {
    expect(eventLabel({ kind: 'ActivityScheduled', activity_type: 'Parse', seq: 0 })).toBe(
      'Activity scheduled: Parse (#0)'
    );
  });

  it('labels a child-scheduled row', () => {
    expect(eventLabel({ kind: 'ChildScheduled', workflow_type: 'SumChild', seq: 1 })).toBe(
      'Child scheduled: SumChild (#1)'
    );
  });

  it('falls back to the raw kind for unknown events', () => {
    expect(eventLabel({ kind: 'Mystery' })).toBe('Mystery');
  });
});

describe('statusClass', () => {
  it('maps known statuses', () => {
    expect(statusClass('completed')).toBe('status-completed');
    expect(statusClass('failed')).toBe('status-failed');
    expect(statusClass('running')).toBe('status-running');
  });
  it('maps unknown to status-unknown', () => {
    expect(statusClass('weird')).toBe('status-unknown');
  });
});

describe('formatTime', () => {
  it('returns empty string for a falsy timestamp', () => {
    expect(formatTime(0)).toBe('');
  });
  it('returns a non-empty string for a real timestamp', () => {
    expect(formatTime(1_700_000_000_000).length).toBeGreaterThan(0);
  });
});

describe('breadcrumb stack', () => {
  it('pushCrumb appends', () => {
    expect(pushCrumb([{ run_id: 'a' }], { run_id: 'b' })).toEqual([
      { run_id: 'a' },
      { run_id: 'b' }
    ]);
  });
  it('popTo truncates back to and including the index', () => {
    const stack = [{ run_id: 'a' }, { run_id: 'b' }, { run_id: 'c' }];
    expect(popTo(stack, 0)).toEqual([{ run_id: 'a' }]);
    expect(popTo(stack, 1)).toEqual([{ run_id: 'a' }, { run_id: 'b' }]);
  });
});
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `npm run test -- historyView`
Expected: FAIL — `Failed to resolve import './historyView.js'`.

- [ ] **Step 3: Write the implementation**

Create `src/lib/historyView.js`:

```js
/**
 * @typedef {Object} EventDto
 * @property {number} event_id
 * @property {number} ts
 * @property {string} kind
 * @property {string|null} [child_run_id]
 * @property {number} [seq]
 * @property {string} [activity_type]
 * @property {string} [workflow_type]
 * @property {string} [name]
 * @property {number} [duration_ms]
 * @property {string} [change_id]
 */

/**
 * Short human label for one timeline row.
 * @param {EventDto} ev
 * @returns {string}
 */
export function eventLabel(ev) {
  switch (ev.kind) {
    case 'WorkflowStarted':
      return 'Workflow started';
    case 'ActivityScheduled':
      return `Activity scheduled: ${ev.activity_type} (#${ev.seq})`;
    case 'ActivityCompleted':
      return `Activity completed (#${ev.seq})`;
    case 'ActivityFailed':
      return `Activity failed (#${ev.seq})`;
    case 'TimerStarted':
      return `Timer started: ${ev.duration_ms}ms (#${ev.seq})`;
    case 'TimerFired':
      return `Timer fired (#${ev.seq})`;
    case 'SignalReceived':
      return `Signal received: ${ev.name}`;
    case 'ChildScheduled':
      return `Child scheduled: ${ev.workflow_type} (#${ev.seq})`;
    case 'ChildCompleted':
      return `Child completed (#${ev.seq})`;
    case 'Patched':
      return `Patched: ${ev.change_id}`;
    default:
      return ev.kind;
  }
}

/**
 * CSS modifier class for a run status.
 * @param {string} status
 * @returns {string}
 */
export function statusClass(status) {
  switch (status) {
    case 'completed':
      return 'status-completed';
    case 'failed':
      return 'status-failed';
    case 'running':
      return 'status-running';
    default:
      return 'status-unknown';
  }
}

/**
 * Epoch-ms → local datetime string; '' for a falsy timestamp.
 * @param {number} ms
 * @returns {string}
 */
export function formatTime(ms) {
  if (!ms) return '';
  return new Date(ms).toLocaleString();
}

/**
 * @typedef {Object} Crumb
 * @property {string} run_id
 * @property {string} label
 */

/**
 * Append a crumb to the breadcrumb trail.
 * @param {Crumb[]} stack
 * @param {Crumb} crumb
 * @returns {Crumb[]}
 */
export function pushCrumb(stack, crumb) {
  return [...stack, crumb];
}

/**
 * Truncate the trail back to (and including) the crumb at `index`.
 * @param {Crumb[]} stack
 * @param {number} index
 * @returns {Crumb[]}
 */
export function popTo(stack, index) {
  return stack.slice(0, index + 1);
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npm run test -- historyView`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/lib/historyView.js src/lib/historyView.test.js
git commit -m "feat(frontend): pure history-view helpers (labels, status, crumbs)"
```

---

## Task 5: `/history` route component

**Files:**
- Create: `src/routes/history/+page.svelte`

- [ ] **Step 1: Create the route component**

Create `src/routes/history/+page.svelte`:

```svelte
<script>
  import { onMount } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { eventLabel, statusClass, formatTime, pushCrumb, popTo } from '$lib/historyView.js';

  /** @type {import('$lib/applyCompletion.js').Message[] | any[]} */
  let runs = $state([]);
  /** @type {any} */
  let detail = $state(null);
  /** @type {{ run_id: string, label: string }[]} */
  let crumbs = $state([]);
  let listError = $state('');
  let detailError = $state('');

  async function loadRuns() {
    try {
      runs = await invoke('list_runs');
      listError = '';
    } catch (e) {
      listError = String(e);
    }
  }

  /** @param {string} runId @param {string} label @param {boolean} drill */
  async function openRun(runId, label, drill) {
    try {
      detail = await invoke('run_detail', { runId });
      detailError = '';
      crumbs = drill ? pushCrumb(crumbs, { run_id: runId, label }) : [{ run_id: runId, label }];
    } catch (e) {
      detailError = String(e);
    }
  }

  /** @param {number} i */
  async function gotoCrumb(i) {
    const c = crumbs[i];
    crumbs = popTo(crumbs, i);
    try {
      detail = await invoke('run_detail', { runId: c.run_id });
      detailError = '';
    } catch (e) {
      detailError = String(e);
    }
  }

  /** @param {any} run */
  const runLabel = (run) => `${run.workflow_type} · ${run.run_id.slice(0, 8)}`;

  onMount(() => {
    loadRuns();
    const onFocus = () => loadRuns();
    window.addEventListener('focus', onFocus);
    return () => window.removeEventListener('focus', onFocus);
  });
</script>

<main>
  <header>
    <h1>Workflow History</h1>
    <button onclick={loadRuns}>Refresh</button>
  </header>

  <div class="panes">
    <section class="list">
      {#if listError}
        <p class="error">{listError}</p>
      {/if}
      {#each runs as run (run.run_id)}
        <button class="run-row" onclick={() => openRun(run.run_id, runLabel(run), false)}>
          <span class="badge {statusClass(run.status)}">{run.status}</span>
          <span class="rtype">{run.workflow_type}</span>
          <span class="rid">{run.run_id.slice(0, 8)}</span>
          <span class="rtime">{formatTime(run.started_at)}</span>
        </button>
      {/each}
      {#if runs.length === 0 && !listError}
        <p class="empty">No runs yet.</p>
      {/if}
    </section>

    <section class="detail">
      {#if detailError}
        <p class="error">{detailError}</p>
      {/if}
      {#if detail}
        {#if crumbs.length > 1}
          <nav class="crumbs">
            {#each crumbs as c, i}
              <button class="crumb" onclick={() => gotoCrumb(i)}>{c.label}</button>
              {#if i < crumbs.length - 1}<span class="sep">›</span>{/if}
            {/each}
          </nav>
        {/if}

        <div class="detail-head">
          <span class="badge {statusClass(detail.summary.status)}">{detail.summary.status}</span>
          <strong>{detail.summary.workflow_type}</strong>
          <code>{detail.summary.run_id}</code>
          <span class="rtime">{formatTime(detail.summary.started_at)}</span>
        </div>

        <ol class="timeline">
          {#each detail.events as ev (ev.event_id)}
            <li>
              <span class="ev-time">{formatTime(ev.ts)}</span>
              {#if ev.child_run_id}
                <button
                  class="ev-link"
                  onclick={() => openRun(ev.child_run_id, ev.workflow_type ?? 'child', true)}
                >
                  {eventLabel(ev)} →
                </button>
              {:else}
                <span class="ev-label">{eventLabel(ev)}</span>
              {/if}
            </li>
          {/each}
        </ol>
      {:else if !detailError}
        <p class="empty">Select a run to view its timeline.</p>
      {/if}
    </section>
  </div>
</main>

<style>
  main { max-width: 60rem; margin: 0 auto; padding: 1rem; font-family: system-ui, sans-serif; }
  header { display: flex; justify-content: space-between; align-items: center; }
  .panes { display: grid; grid-template-columns: 18rem 1fr; gap: 1rem; height: 70vh; }
  .list, .detail { overflow-y: auto; border: 1px solid #ddd; border-radius: 8px; padding: 0.5rem; }
  .run-row { display: flex; gap: 0.5rem; align-items: center; width: 100%; text-align: left;
    background: none; border: 0; border-bottom: 1px solid #eee; padding: 0.5rem 0.25rem; cursor: pointer; }
  .run-row:hover { background: #f6f6f6; }
  .rtype { font-weight: 600; }
  .rid { color: #666; font-family: ui-monospace, monospace; }
  .rtime, .ev-time { color: #888; font-size: 0.8rem; margin-left: auto; }
  .badge { font-size: 0.7rem; padding: 0.1rem 0.4rem; border-radius: 999px; text-transform: uppercase; }
  .status-completed { background: #dcfce7; color: #166534; }
  .status-failed { background: #fee2e2; color: #991b1b; }
  .status-running { background: #e0e7ff; color: #3730a3; }
  .status-unknown { background: #f1f1f1; color: #555; }
  .crumbs { display: flex; gap: 0.4rem; align-items: center; margin-bottom: 0.5rem; }
  .crumb { background: none; border: 0; color: #2563eb; cursor: pointer; padding: 0; }
  .sep { color: #999; }
  .detail-head { display: flex; gap: 0.5rem; align-items: center; margin-bottom: 0.75rem; }
  .timeline { list-style: none; margin: 0; padding: 0; }
  .timeline li { display: flex; gap: 0.5rem; align-items: baseline; padding: 0.3rem 0; border-bottom: 1px solid #f0f0f0; }
  .ev-link { background: none; border: 0; color: #2563eb; cursor: pointer; padding: 0; text-align: left; }
  .error { color: #991b1b; }
  .empty { color: #888; }
</style>
```

- [ ] **Step 2: Build the frontend to confirm the route compiles and prerenders**

Run: `npm run build`
Expected: build succeeds; SvelteKit emits the `/history` route. (`adapter-static` with `fallback: index.html` is already configured, so the second window's client-side route resolves.)

- [ ] **Step 3: Run the full frontend test suite**

Run: `npm run test`
Expected: PASS (`applyCompletion` + `historyView` suites).

- [ ] **Step 4: Manual verification**

Run: `npm run tauri dev`
Then, in the app:
1. Submit a couple of chat inputs (e.g. `1 2 3`, then `1 two 3`) so there are completed and failed runs.
2. Open **View ▸ Workflow History…** — a second window titled "Workflow History" opens.
3. Confirm the list shows the root runs (newest first) with status badges; selecting one shows the decoded timeline (WorkflowStarted, ActivityScheduled with decoded input, ChildScheduled, etc.).
4. Click a `Child scheduled →` row and confirm it drills into the child's timeline with a working breadcrumb back to the parent.
5. Submit another chat input, return to the history window, click **Refresh** (or focus the window) and confirm the new run appears.

Expected: all of the above behave as described.

- [ ] **Step 5: Commit**

```bash
git add src/routes/history/+page.svelte
git commit -m "feat(frontend): /history master-detail viewer with child drill-down"
```

---

## Task 6: Final full-suite verification

**Files:** none (verification only)

- [ ] **Step 1: Run the entire Rust workspace test suite**

Run: `cargo test --workspace`
Expected: PASS across `engine`, `persist`, `app`, `demo`, `workflow`, `activity`.

- [ ] **Step 2: Run the frontend suite**

Run: `npm run test`
Expected: PASS.

- [ ] **Step 3: Confirm the host still builds for the app target**

Run: `cargo build -p app`
Expected: builds cleanly with no warnings introduced by this work.

---

## Self-Review Notes

- **Spec coverage:** §4 (engine/persist read model) → Task 1; §5.1/5.2 (commands + `event_to_json`) → Task 2; §5.3 (menu + window + capability) → Task 3; §6 frontend helpers → Task 4 and route → Task 5; §8 testing → tests embedded in Tasks 1–5 plus Task 6; §7 error handling → command `Err` paths (Task 2) and `listError`/`detailError` panes (Task 5).
- **Out of scope (§9)** — no streaming, search, pagination, or replay/delete actions are implemented, by design.
- **Type consistency:** `ExecutionSummary`/`HistoryRecord` defined in Task 1 are imported unchanged in Tasks 1–2; `RunSummaryDto`/`EventDto`/`RunDetailDto` are defined once in Task 2 and consumed by the commands in the same task and the frontend in Task 5; helper names (`eventLabel`, `statusClass`, `formatTime`, `pushCrumb`, `popTo`) match between `historyView.js` and its test and the route component.
