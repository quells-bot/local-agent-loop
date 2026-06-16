# Tauri Chat App over the Workflow Engine — Design

**Date:** 2026-06-15
**Status:** Approved (pre-implementation)
**Spec references** ("§N") point to the durable workflow engine design:
`docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md`.

## 1. Goal

Wrap the finished workflow engine in a Tauri desktop app with a deliberately
minimal chat UI, and prove the host/IPC seam (§7) end to end with a small,
entirely unit-testable workflow flow.

A user types text into a chat box and submits. The text starts a parent
workflow. An activity parses the text as space-separated integers (error on
parse failure). A child workflow sums the integers via its own activity. The
parent's output — the sum — is pushed back to the chat UI and rendered as a
reply. Parse failures render as an error reply.

This is the first slice. A separate workflow **history view** (for debugging
workflows during development) is a fast follow-up on top of the plumbing this
slice establishes; it is **out of scope here** (see §9).

## 2. Scope decisions

- **Frontend:** SvelteKit with Svelte 5 (runes), JSDoc (not TypeScript), Vite.
- **First slice:** chat only. History view is a later spec.
- **Chat transcript:** in-memory frontend state; cleared on app restart. The
  engine still durably persists every run in SQLite.
- **Completion delivery:** push via the completion observer → Tauri event
  (§7.3), not awaiting across IPC and not polling. The frontend correlates the
  pushed event to its pending chat bubble by `workflow_id`.
- **No engine changes.** `start_workflow` (§7.1) and `on_run_completed` (§7.3)
  already exist and are tested. `list_workflows` / `read_history` (§7.4) arrive
  with the history-view follow-up.

## 3. Crate / workspace layout

```
/                  SvelteKit frontend (package.json, src/, svelte.config.js, vite.config.js)
/src-tauri/        Tauri host crate — #[tauri::command]s, managed Engine state,
                   observer -> app_handle.emit. The ONLY crate depending on both
                   engine + persist. Thin wiring shell.
/crates/
  demo/            NEW library crate: Parse + SumActivity + Parent + SumChild
                   definitions, plus integration tests (in-memory SQLite, no Tauri).
  activity/ workflow/ engine/ persist/   (unchanged)
```

Workspace `members` gains `"src-tauri"`; `crates/demo` is already covered by the
existing `crates/*` glob. Per §10 the engine crates stay tauri-free — `tauri`
appears only in `src-tauri`. Putting the demo definitions in their own library
crate (not inside `src-tauri`) keeps the flow unit-testable with zero Tauri
involvement; `src-tauri` depends on `demo` and registers its four types.

## 4. Demo flow (`crates/demo`)

**Type house style.** Every activity and every workflow takes a bespoke named
*params* struct and returns a bespoke named *results* struct — never a bare
primitive or tuple, even when trivial. This is what makes version evolution
backwards-compatible: a new business requirement becomes an added field, not a
breaking signature change that forces a "v2" of the activity/workflow. Each type
derives `Serialize, Deserialize, Clone, Debug`. Structurally identical siblings
(e.g. `SumParams` vs `SumChildParams`) stay separate types so they can evolve
independently.

```
Parse        activity:  ParseParams { text: String }      -> ParseResult { values: Vec<i64> }
SumActivity  activity:  SumParams { values: Vec<i64> }    -> SumResult { total: i64 }
SumChild     workflow:  SumChildParams { values: Vec<i64> } -> SumChildResult { total: i64 }
Parent       workflow:  ParentParams { text: String }     -> ParentResult { total: i64 }
```

```rust
// SumChild
let summed = ctx.activity::<SumActivity>(SumParams { values: params.values }).await?;
Ok(SumChildResult { total: summed.total })

// Parent
let parsed = ctx.activity::<Parse>(ParseParams { text: params.text }).await?; // parse failure => Failed
let summed = ctx.child_workflow::<SumChild>(SumChildParams { values: parsed.values }).await?;
Ok(ParentResult { total: summed.total })
```

This single flow exercises a parent activity, error propagation (`?` converts
`activity::Error` into `workflow::Error` via the existing `From` impl), a child
workflow, and the child's own activity.

**Behaviour decisions:**
- **Parse:** `text.split_whitespace()`, parse each token as `i64`. On any bad
  token, return `activity::Error::fatal("could not parse '<token>' as an
  integer")` — `fatal` is non-retryable, so it fails fast rather than retrying a
  deterministic parse error. The fatal activity error propagates through
  `Parent`'s `?` and the run reaches `Failed` with a `workflow::Error` carrying
  the message.
- **Empty input:** `""` → `split_whitespace` yields nothing → `Vec::new()` →
  sum `0`. Empty is **valid**, not a parse failure.
- **Negative integers:** supported (`"-5 10"` → `5`).
- **i64 overflow:** not handled in v1 (plain `sum`); acceptable for the demo.

## 5. Host / IPC surface (`src-tauri`)

### 5.1 Command

```rust
#[tauri::command]
async fn submit(
    text: String,
    workflow_id: String,
    engine: State<'_, Arc<Engine>>,
) -> Result<String /* run_id */, String> {
    engine
        .start_workflow::<Parent>(ParentParams { text }, StartOptions { id: workflow_id })
        .await
        .map(|h| h.run_id().to_string())
        .map_err(|e| e.to_string())
}
```

The command keeps a flat IPC signature (`text`, `workflow_id`) and wraps `text`
into `ParentParams` at the boundary, so the frontend stays unaware of the
workflow's params struct.

The frontend generates `workflow_id` (`crypto.randomUUID()`) per submission, so
start-dedup (§7.1) and event correlation both come for free.

### 5.2 Completion push

In Tauri `setup`, register the observer and adapt it to a Tauri event:

```rust
engine.on_run_completed(move |ev: RunCompleted| {
    let payload = CompletionPayload {
        workflow_id: ev.workflow_id,
        run_id: ev.run_id,
        status: match ev.status { Completed => "completed", _ => "failed" },
        // Decode the stored bytes as a generic JSON value so src-tauri stays
        // demo-agnostic (not hard-coded to ParentResult). On `completed` this is
        // the ParentResult object { "total": N }; on `failed` it is the
        // workflow::Error object { "message": "..." }.
        result: ev.result
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok()),
    };
    let _ = app_handle.emit("run_completed", payload);
});
```

Emitted event shape:

```jsonc
{ "workflow_id": "...", "run_id": "...",
  "status": "completed" | "failed",
  "result": <JSON value | null> }   // {total} when completed, {message} when failed
```

The frontend interprets `result` by `status`. `engine` takes no `tauri`
dependency; this adaptation lives entirely in `src-tauri` (§7.3, §10).

### 5.3 State wiring (`setup`)

1. `Sqlite::open(app_data_dir/"workflows.db")` (the path resolver provides
   `app_data_dir`; create it if missing).
2. Clone the `Sqlite` (it is `Clone`, an `Arc<Mutex<Connection>>`) into a
   `History` Arc and a `TaskQueue` Arc.
3. `Engine::new(history, queue)`.
4. Register the four types: `register_workflow::<Parent>()`,
   `register_workflow::<SumChild>()`, `register_activity::<Parse>()`,
   `register_activity::<SumActivity>()`.
5. `engine.on_run_completed(...)` capturing an `AppHandle` clone (§5.2).
6. `let engine = engine.start();` — spawns the driver / worker / timer / sweeper
   loops and returns `Arc<Engine>`.
7. `app.manage(engine);`

`Engine::start` uses `tokio::spawn`, so a tokio runtime must be active. Tauri v2
uses tokio as its async runtime (`tauri::async_runtime`); ensure the loops are
spawned within that runtime (call `start()` from `setup`/an async context, or
via `tauri::async_runtime::spawn` if needed). This is a wiring detail to verify
during implementation, not a design change.

## 6. SvelteKit frontend (Svelte 5 runes, JSDoc)

- **Static SPA:** `@sveltejs/adapter-static`; root `+layout.js` sets
  `export const ssr = false;` and `export const prerender = true;`. `tauri dev`
  points at the Vite dev server; `tauri build` serves the static output.
- **Single route `/`** (chat). The history view lands later at `/history`.
- **State** via the `$state` rune — a `messages` array:

  ```js
  /**
   * @typedef {Object} Message
   * @property {string} id        // == workflow_id, the correlation key
   * @property {string} text      // the submitted input
   * @property {'pending'|'done'|'error'} status
   * @property {number} [output]  // set when status === 'done' (from result.total)
   * @property {string} [error]   // set when status === 'error' (from result.message)
   */
  ```

- **Submit handler:** `const id = crypto.randomUUID();` push
  `{ id, text, status: 'pending' }`; `await invoke('submit', { text, workflowId: id })`;
  if the command rejects, mark that message `error` with the rejection string.
- **Event listener:** `listen('run_completed', (e) => { messages = applyCompletion(messages, e.payload); })`.
- **Pure transition for testability:** `applyCompletion(messages, payload)` is a
  pure function in its own module. Given the current array and an event payload,
  it returns the next array with the matching message (by `workflow_id`) moved
  to `done` (`output` ← `payload.result.total`) or `error` (`error` ←
  `payload.result.message`). This is
  the frontend's unit-test seam (Vitest), keeping the "unit-testable" ethos
  without a full UI harness.
- **Components:** one `ChatView` — a scrolling message list, a text input, and a
  submit button. Deliberately plain. The `frontend-design` skill is **not**
  invoked; "dead simple" stands for v1.

## 7. Error handling

| Failure | Path | UI result |
|---|---|---|
| Non-integer token | `Parse` fatal error → `Parent` `?` → run `Failed` → observer emits `status:"failed"` | error bubble with the parse message |
| `submit` command error (engine start failure) | `Result::Err(String)` rejects the `invoke` | the pending bubble marked `error` |
| Activity panic / unregistered type / nondeterminism | engine dead-letters → `Failed` → observer | error bubble |

## 8. Testing plan (TDD)

**`crates/demo` integration tests** — tokio, in-memory `Sqlite`, pump to
quiescence (the `end_to_end.rs` pattern: alternate `process_one_runnable` /
`process_one_activity` until neither makes progress):

- `"1 2 3"` → `ParentResult { total: 6 }`
- `"10 20 30"` → `total: 60`
- `""` → `total: 0`
- `"-5 10"` → `total: 5`
- `"1 two 3"` → run `Failed`, `workflow::Error` message contains `two`

Plus direct unit tests of the parse logic and `SumActivity::run` (asserting the
`ParseResult` / `SumResult` payloads).

**Frontend** — Vitest unit test of `applyCompletion` (pending → done; pending →
error; non-matching `workflow_id` left untouched). The UI itself is verified
manually via `cargo tauri dev`.

**Engine** — no changes in this slice, so no new engine tests.

## 9. Out of scope (follow-ups)

- **Workflow history view** — a `/history` route listing runs and showing each
  run's event history for debugging. Requires exposing `list_workflows` /
  `describe_workflow` / `read_history` as host methods (§7.4; the `History`
  trait already has `read_history`). Its own spec.
- Chat transcript persistence / rehydration across restarts.
- Signals, timers, cancellation surfaced in the UI (engine supports them; no UI
  yet).
- Frontend visual design polish.
- i64 overflow handling.
