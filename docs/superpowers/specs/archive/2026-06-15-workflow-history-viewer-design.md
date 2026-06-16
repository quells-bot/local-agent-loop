# Workflow History Viewer — Design

**Date:** 2026-06-15
**Status:** Approved (pre-implementation)
**Spec references** ("§N") point to the durable workflow engine design:
`docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md`.
This is the "workflow history view" follow-up deferred from the chat-app spec
(`2026-06-15-tauri-chat-app-design.md` §9).

## 1. Goal

A developer-facing debugging view, deliberately kept *off* the primary chat UI.
A native menu item **"Workflow History…"** opens a **second window** at the
`/history` route. That window lists past runs (root workflows, newest first),
and each run drills into a humanized event timeline. `Child*` events link to the
child run's own timeline.

This builds entirely on plumbing the chat slice already established and on data
the engine already persists durably in SQLite — the chat slice deliberately
keeps the transcript in memory, but every run's history is on disk. The viewer
adds a read path over that store plus the window/menu surface; it changes
nothing on the engine's correctness (replay/determinism) path.

## 2. Scope decisions

- **Surface:** a native app menu item opens a **separate `WebviewWindow`** at
  `/history` (not an in-chat panel, not a predeclared hidden window). Keeps the
  debug tool out of the primary UI and independently open/closable.
- **List scope:** **root workflows only** (`parent_run_id IS NULL`). Child
  workflows (e.g. `SumChild`) are reached by drilling into a parent's timeline,
  not listed as top-level rows.
- **Timeline rendering:** **decoded & humanized** — each event shown as a
  friendly row (kind, seq, timestamp, decoded JSON payload).
- **Read-model boundary:** **extend the existing `History` trait** (option A).
  The migration story stays "implement `History` + `TaskQueue`". Viewer-facing
  record types stay **separate from the replay-critical `StoredEvent`**.
- **Freshness:** **snapshot-on-open + manual Refresh + refetch on window focus.**
  No live event streaming into the viewer.
- **Host stays demo-agnostic:** commands decode payloads to generic
  `serde_json::Value` (same approach as the chat slice's `CompletionPayload`);
  no `demo`-specific types leak into `src-tauri`.
- **No determinism-path changes.** `StoredEvent`, replay, and the exactly-once
  boundary are untouched.

## 3. Crate / workspace layout

No new crates. Changes land in three existing places:

```
/crates/engine/    History trait gains two read-model methods; engine::types
                   gains ExecutionSummary + HistoryRecord (read-model types).
/crates/persist/   Sqlite implements the two new History methods.
/src-tauri/        list_runs + run_detail commands; event_to_json mapping;
                   menu + history-window wiring; capability grant.
/src/routes/history/   NEW SvelteKit route (master-detail viewer).
/src/lib/historyView.js + .test.js   NEW pure render helpers + Vitest tests.
```

Per the engine/Tauri boundary ([[durable-workflow-engine-is-tauri-backend]]),
the new engine/persist code takes **no `tauri` dependency**; `tauri` appears
only in `src-tauri`.

## 4. Engine + persist (Tauri-agnostic) — boundary choice A

### 4.1 Read-model types (`engine::types`)

New types, distinct from `StoredEvent` so the determinism/replay path is not
touched:

```rust
/// One row of the run list (roots only). Timestamps/count derived from `history`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSummary {
    pub run_id: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub status: ExecStatus,
    pub started_at: i64,     // epoch ms — min(history.ts) for the run
    pub last_event_at: i64,  // epoch ms — max(history.ts) for the run
    pub event_count: i64,
}

/// One row of a run's timeline. The viewer analog of `StoredEvent`, but carries
/// `ts` and a resolved `child_run_id` for Child* events. NOT used by replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryRecord {
    pub event_id: i64,
    pub ts: i64,                       // epoch ms (from history.ts)
    pub event: Event,                  // full typed event
    pub child_run_id: Option<String>, // Some for ChildScheduled/ChildCompleted
}
```

### 4.2 `History` trait additions

```rust
/// Read model for the history viewer (NOT part of the exactly-once boundary).
/// Root executions only, newest first.
async fn list_executions(&self) -> anyhow::Result<Vec<ExecutionSummary>>;

/// All events for a run, in `event_id` order, carrying timestamps. For
/// Child* events, resolves the child's run_id from `executions`
/// (parent_run_id = run_id AND parent_seq = event.seq).
async fn read_events(&self, run_id: &str) -> anyhow::Result<Vec<HistoryRecord>>;
```

The trait doc and `crates/engine/tests/migration_seam.rs` gain a one-line note
that these are **read-model** methods: the engine itself never calls them; they
ride on `History` because they are history-store reads, keeping the migration
story at two traits. The seam assertion (`Sqlite: History + TaskQueue`) is
unchanged.

### 4.3 `persist::Sqlite` implementations

- **`list_executions`** — join `executions` to an aggregate over `history`:

  ```sql
  SELECT e.run_id, e.workflow_id, e.workflow_type, e.status,
         MIN(h.ts), MAX(h.ts), COUNT(h.event_id)
  FROM executions e JOIN history h ON h.run_id = e.run_id
  WHERE e.parent_run_id IS NULL
  GROUP BY e.run_id
  ORDER BY MIN(h.ts) DESC
  ```

  `status` decoded via `ExecStatus::from_str`. Every execution has at least a
  `WorkflowStarted` row (`create_execution` appends it), so the inner join never
  drops a real run.

- **`read_events`** — like `read_history` but selects `event_id, payload, ts`
  ordered by `event_id`; deserializes `payload` into `Event`. For
  `ChildScheduled`/`ChildCompleted`, run a lookup
  `SELECT run_id FROM executions WHERE parent_run_id = ?1 AND parent_seq = ?2`
  (seq from the event) and attach `child_run_id`. Other events get `None`.

## 5. Host / IPC surface (`src-tauri`)

### 5.1 Commands

```rust
#[tauri::command]
async fn list_runs(history: State<'_, Arc<dyn History>>) -> Result<Vec<RunSummaryDto>, String>;

#[tauri::command]
async fn run_detail(run_id: String, history: State<'_, Arc<dyn History>>)
    -> Result<RunDetailDto, String>;
```

`setup` already constructs `history: Arc<dyn History>` (lib.rs) before building
the engine; the host `app.manage(history.clone())` so the read commands resolve
it directly via `State`, rather than threading reads through `Engine`'s API.
`list_runs` calls `list_executions`; `run_detail` calls `load_run` +
`read_events`. `RunDetailDto { summary, events }` where `summary` is built from
`load_run` plus the run's first/last `ts` (so a drilled-into child gets the same
header shape as a list row).

### 5.2 Humanizing (the host test seam)

A pure free function maps a `HistoryRecord` to a frontend-friendly DTO, decoding
inner byte payloads to generic JSON so the host stays demo-agnostic:

```rust
fn event_to_json(record: HistoryRecord) -> EventDto
```

Per-variant shape (inner `Vec<u8>` payloads decoded to `serde_json::Value`;
`None` if the bytes are not valid JSON):

| Event | EventDto fields (besides `kind`, `event_id`, `ts`) |
|---|---|
| `WorkflowStarted` | `input` |
| `ActivityScheduled` | `seq`, `activity_type`, `input`, `retry` |
| `ActivityCompleted` | `seq`, `output` |
| `ActivityFailed` | `seq`, `error` (`activity::Error`, serializable) |
| `TimerStarted` | `seq`, `duration_ms` |
| `TimerFired` | `seq` |
| `SignalReceived` | `name`, `payload` |
| `ChildScheduled` | `seq`, `workflow_type`, `input`, `child_run_id` |
| `ChildCompleted` | `seq`, `result` (`ChildResult`; decode inner bytes), `child_run_id` |
| `Patched` | `change_id` |

`event_to_json` is unit-tested per variant, mirroring the existing
`completion_payload` tests in `src-tauri/src/lib.rs`.

### 5.3 Menu + history window wiring (`setup`)

- Build an app menu with `MenuBuilder`: a "View" submenu containing a
  `MenuItem` with id `history`, label "Workflow History…".
- Register `on_menu_event`. On id `history`:

  ```rust
  if let Some(w) = app.get_webview_window("history") {
      let _ = w.set_focus();
  } else {
      let _ = tauri::WebviewWindowBuilder::new(
          app, "history", tauri::WebviewUrl::App("history".into()),
      )
      .title("Workflow History")
      .build();
  }
  ```

  The reuse guard means re-triggering the menu focuses the existing window
  rather than opening duplicates.

- `src-tauri/capabilities/default.json`: ensure the `history` window is granted
  the same core window/event permissions and the `list_runs` / `run_detail`
  commands. Whether to widen the capability `windows` list or add a capability
  for `history` is a verify-during-impl wiring detail.

- The new commands are added to `invoke_handler(tauri::generate_handler![submit,
  list_runs, run_detail])`.

## 6. Frontend — `/history` route (Svelte 5 runes, JSDoc)

- New route `src/routes/history/+page.svelte`. The existing static-SPA setup
  (`adapter-static`, `ssr = false`, `prerender = true`) prerenders it;
  `WebviewUrl::App("history")` resolves to it. Trailing-slash / output-path
  resolution is a verify-during-impl detail.
- **Master–detail layout:**
  - **Left:** run list — one row per root run showing `workflow_type`, a short
    `run_id`, a status badge, and a start time. A **Refresh** button at the top.
  - **Right:** the selected run's header (type, id, status, start time) plus the
    decoded event timeline (one row per `EventDto`).
  - **Drill-down:** a `ChildScheduled`/`ChildCompleted` row with a `child_run_id`
    renders as a clickable link; clicking loads that child's detail and pushes a
    **breadcrumb** (parent → child) with a **Back** affordance.
- **State** via runes: `runs`, `selected` (run_id), `detail` (`RunDetailDto`),
  and a `navStack` array driving the breadcrumb.
- **Data flow:** `onMount` → `invoke('list_runs')`. Selecting a run →
  `invoke('run_detail', { runId })`. Refresh button and **window-focus**
  re-fetch the list (snapshot model — no streaming). Child link →
  `invoke('run_detail', { runId: childRunId })` + push breadcrumb.
- **Pure helpers (`src/lib/historyView.js`) — the frontend test seam**, mirroring
  `applyCompletion`:
  - event label/summary formatting from an `EventDto`,
  - status → badge CSS class,
  - epoch-ms → readable time,
  - breadcrumb-stack reducer (push child / pop to an ancestor).
- **Style:** plain, matching the chat view's "dead simple" ethos. The
  `frontend-design` skill is **not** invoked.

## 7. Error handling

| Failure | Path | UI result |
|---|---|---|
| `list_runs` / `run_detail` command error | `Result::Err(String)` rejects `invoke` | inline error message in the affected pane; Refresh retries |
| Run has no events (shouldn't happen) | `read_events` returns empty | empty-timeline placeholder |
| Undecodable payload bytes | `event_to_json` sets that field to `null` | row renders kind/seq/ts; payload shown as absent |
| `child_run_id` unresolved (child not yet created) | `read_events` attaches `None` | row renders as a normal (non-link) event |

## 8. Testing plan (TDD)

**engine/persist** — in-memory `Sqlite`, seed runs via the existing
create/commit helpers:
- `list_executions`: roots-only filter (children excluded), newest-first
  ordering, correct `started_at` / `last_event_at` / `event_count`.
- `read_events`: `ts` present and `event_id`-ordered; `child_run_id` resolved
  for `ChildScheduled`/`ChildCompleted` and `None` otherwise; empty history.

**src-tauri** — `event_to_json` per-variant mapping: decoded payloads, the
failed/error shape, and `child_run_id` passthrough for child events. Mirrors the
existing `completion_payload` tests.

**frontend** — Vitest over `historyView.js`: event-label formatting, status
class, time formatting, and the breadcrumb reducer (push child, pop to
ancestor). The UI itself is verified manually via `cargo tauri dev`.

**Engine determinism** — no changes to `StoredEvent`/replay, so no new
determinism tests.

## 9. Out of scope (follow-ups)

- Live auto-refresh / streaming of an open run's timeline.
- Search, filter, and pagination of the run list (fine at dev volumes; revisit
  if the list grows large).
- Replay, cancel, signal, or delete actions from the viewer.
- Chat-transcript persistence (still tracked under the chat-app spec).
- Frontend visual polish.
