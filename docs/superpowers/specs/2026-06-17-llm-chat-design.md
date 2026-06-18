# LLM Chat — Design

**Status:** Approved design, ready for an implementation plan.

**Prerequisite:** [Activity Dependency Injection](2026-06-17-activity-dependency-injection-design.md)
(approved design). This feature injects clients/config into activity *instances*;
that engine change must land before chat is implemented.

## Goal

Replace the sum-of-integers demo with a simple back-and-forth LLM chat. The chat
talks to a local llama.cpp instance over its OpenAI-compatible HTTP API. Instead
of starting a workflow per message, the frontend starts **one long-lived workflow
when the chat window opens** and terminates it when the window closes. User
messages are delivered to that workflow as **signals**.

llama.cpp endpoint: `http://mss1.quells.house:8080`, model
`Qwen3.6-35B-A3B-MTP-GGUF`, via `POST /v1/chat/completions`.

## Decisions (settled during brainstorming)

- **Whole replies, not token streaming.** One LLM activity per user message; the
  full reply appears when it completes. Matches the durable-activity model.
  Streaming is explicitly out of scope (a possible later project).
- **Fresh conversation each window-open.** Closing the window terminates the
  workflow; reopening starts a brand-new workflow with an empty transcript. Old
  runs remain in the durable history viewer but are never resumed.
- **Errors: bounded retry, then a visible error reply.** The LLM activity carries
  `RetryPolicy::exponential(4)`. After exhaustion the workflow records an
  assistant message with `status = "error"`; the session stays alive.
- **A mock "chat service" owns chat history.** It stands in for a cloud service
  that would own chat history in a real deployment. It lives outside the engine
  crates (it is not a Temporal/engine concern). The workflow records both the
  user message and the LLM reply into it; the frontend polls it.
- **The engine durability core is otherwise untouched.** Chat is built from
  existing primitives: signals, activities with retry, and `select_biased!`. The
  one core change is activity-instance DI (its own spec).

## Architecture

```
┌─────────────┐  open/send/close (Tauri commands)   ┌────────────────────────┐
│  Frontend   │ ──────────────────────────────────► │  Host (src-tauri)      │
│ +page.svelte│ ◄── poll chat_history ────────────  │  - commands            │
└─────────────┘                                      │  - registers workflow  │
                                                     │    + activity instances│
                                                     └───────┬────────────────┘
                                                             │ signals / start
                                                             ▼
                                              ┌──────────────────────────────┐
                                              │  Engine (unchanged core)      │
                                              │  ChatSession workflow         │
                                              │   loop: select_biased!{       │
                                              │     stop → terminate          │
                                              │     message → record/llm/record│
                                              │   }                           │
                                              └───┬───────────────┬───────────┘
                                  RecordMessage   │               │  LlmComplete
                                                  ▼               ▼
                                        ┌───────────────┐  ┌──────────────────┐
                                        │ chat-service  │  │ llama.cpp         │
                                        │ (mock, SQLite)│◄─┤ /v1/chat/...      │
                                        └───────▲───────┘  └──────────────────┘
                                                │ list_messages (host read)
                                                └──────────── Frontend poll
```

### Crates

**Replace `demo`** with two new crates:

- **`chat-service`** — the mock service. No engine dependencies. Owns its own
  SQLite database (`chat-service.db`, separate from `workflows.db` to emphasize
  the boundary). `Client` is `Send + Sync` (a `Mutex<rusqlite::Connection>`), so
  it can be injected into shared activity instances. API:
  - `open(path) -> Client`
  - `record_message(RecordArgs)` — idempotent insert
  - `list_messages(conversation_id) -> Vec<ChatMessage>` — ordered by `seq`

  Schema:

  ```sql
  CREATE TABLE messages (
    conversation_id TEXT NOT NULL,
    message_id      TEXT NOT NULL,
    role            TEXT NOT NULL,    -- 'user' | 'assistant'
    content         TEXT NOT NULL,
    status          TEXT NOT NULL,    -- 'complete' | 'error'
    seq             INTEGER NOT NULL, -- monotonic per conversation, for ordering
    created_at      INTEGER NOT NULL,
    PRIMARY KEY (conversation_id, message_id)
  );
  ```

  `record_message` uses `INSERT … ON CONFLICT(conversation_id, message_id) DO
  NOTHING`, so the at-least-once retry of a `RecordMessage` activity never
  double-writes. `seq` is assigned monotonically per conversation at write time.

- **`chat`** — the workflow + activities. Depends on `chat-service`, `workflow`,
  `activity`, and `reqwest` (features `json`, `rustls-tls`).
  - **`ChatSession`** workflow — one long-lived run per open window. Thin
    orchestrator; holds no transcript (the service owns it).
  - **`RecordMessage`** activity — instance holds `chat_service::Client`.
  - **`LlmComplete`** activity — instance holds `chat_service::Client` +
    `reqwest::Client` + `base_url` + `model`.

### Host (`src-tauri`)

At startup: open the `chat_service::Client`, register the workflow and the two
activity instances (injecting the client / HTTP client / LLM config), and keep
the client in Tauri state for reads.

```rust
let chat = chat_service::Client::open(&chat_db_path)?;
engine.register_workflow::<ChatSession>();
engine.register_activity(RecordMessage::new(chat.clone()));
engine.register_activity(LlmComplete::new(
    chat.clone(), reqwest::Client::new(), BASE_URL.into(), MODEL.into()));
app.manage(chat); // for chat_history reads
```

`BASE_URL` and `MODEL` are host constants. The existing history-viewer menu and
its `list_runs` / `run_detail` commands are unchanged.

Commands:

| Command | Action |
|---|---|
| `open_chat()` | generate `conversation_id`, start `ChatSession { conversation_id }`, return the id |
| `send_message(conversation_id, message_id, text)` | `signal_workflow(id, "message", { message_id, text })` |
| `chat_history(conversation_id)` | `client.list_messages(id)` → DTOs |
| `close_chat(conversation_id)` | `signal_workflow(id, "stop", {})` |

### Frontend (`src/routes/+page.svelte`)

Replaces the integer demo.

- `onMount`: `open_chat()` → `conversation_id`; start a ~500 ms poll of
  `chat_history`.
- **Render** from the polled service messages. A just-sent message not yet
  present in the poll is shown optimistically and reconciled by `message_id` (a
  small pure merge function — a testable seam like the existing
  `applyCompletion.js`).
- **Input gating**: locked whenever the latest user message has no
  `"{message_id}-reply"` row yet; unlocks when that reply lands as `complete`
  *or* `error` (a pure `awaitingReply(messages)` reducer).
- **Send**: fresh `message_id` (`crypto.randomUUID()`), optimistic user bubble,
  `send_message(...)`, lock the input.
- **Window close**: `getCurrentWindow().onCloseRequested` → `await
  close_chat(...)` then proceed; clear the poll interval on `onDestroy`.

## Data flow (one message)

1. **Open** — frontend `open_chat()`; host starts `ChatSession` with
   `StartOptions { id: conversation_id }`, input `{ conversation_id }`.
2. **Send** — frontend `send_message(conversation_id, message_id, text)`; host
   signals `"message"`. The signal is durable before the command returns.
3. **Workflow turn** (sequential):
   - `RecordMessage { role: "user", message_id, content: text, status: "complete" }`
   - `LlmComplete { conversation_id }` → reads history from the service, calls
     llama.cpp → `Ok(reply)` or `Err` after retries
   - `RecordMessage { role: "assistant", message_id: "{message_id}-reply",
     content: reply-or-error, status: "complete" | "error" }`
4. **Poll** — frontend renders the transcript from `chat_history`; input stays
   locked until the `"{message_id}-reply"` row appears.
5. **Close** — frontend `close_chat()`; host signals `"stop"`; the workflow
   returns and the run reaches `Completed`.

## The `ChatSession` loop

```rust
async fn run(ctx: Context, params: ChatSessionParams) -> Result<ChatSessionResult, Error> {
    let conversation_id = params.conversation_id;
    let messages = ctx.signal_channel::<UserMessage>("message");
    let stop = ctx.signal_channel::<StopSignal>("stop");
    loop {
        futures::select_biased! {
            _ = stop.recv().fuse() => break,                 // window closed → terminate
            msg = messages.recv().fuse() => {
                let msg = msg?;
                ctx.activity::<RecordMessage>(user_msg(&conversation_id, &msg)).await?;
                let reply = ctx.activity::<LlmComplete>(
                        LlmParams { conversation_id: conversation_id.clone() })
                    .retry(RetryPolicy::exponential(4))
                    .await;
                ctx.activity::<RecordMessage>(
                    assistant_msg(&conversation_id, &msg, reply)).await?;
            }
        }
    }
    Ok(ChatSessionResult {})
}
```

`assistant_msg` maps `Ok(reply)` → `{ status: "complete", content: reply }` and
`Err(e)` → `{ status: "error", content: e.message }`, both with `message_id =
"{user message_id}-reply"`. The LLM `Err` is handled inline (no `?`) so one bad
reply never fails the session.

### Determinism & termination

- Uses `futures::select_biased!` (engine-approved deterministic combinator) — not
  `select!` / bare `FuturesUnordered`.
- A `stop` arriving mid-message is buffered and consumed on the next loop
  iteration: the in-flight reply finishes, then the run terminates. Acceptable
  because the window is closing.
- Hard-kill (no `close_chat`): the run is left parked on `recv` — inert,
  harmless, never resumed (fresh conversation per open). It lingers in history.
- Replay is deterministic: signals re-apply in `event_id` order and activity
  results come from history, so `select_biased!` resolves identically. The LLM
  activity reading external state is replay-safe because only its recorded
  *output* feeds replay.

## Activities

### `RecordMessage`

- Instance: `{ chat: chat_service::Client }`.
- Input: `{ conversation_id, message_id, role, content, status }`.
- Writes one row via `chat.record_message(...)`. Idempotent.

### `LlmComplete`

- Instance: `{ chat: chat_service::Client, http: reqwest::Client, base_url, model }`.
- Input: `{ conversation_id }`.
- Steps:
  1. `chat.list_messages(conversation_id)`, keep `complete` rows, map to OpenAI
     `messages` (`role`, `content`).
  2. `POST {base_url}/v1/chat/completions` with `{ model, messages, stream: false }`.
  3. Return `{ reply: choices[0].message.content }`.
- Error classification:
  - network error / timeout / non-2xx → **retryable** `Error` (engine retries per
    policy).
  - 2xx with a malformed body (no `choices[0].message.content`) → **fatal**
    `Error`.
- The HTTP mapping is split into pure functions `build_request(messages, model)`
  and `parse_response(json) -> Result<String>` for unit testing without a live
  server.

## Error handling summary

| Failure | Behavior |
|---|---|
| LLM transient (network/5xx/timeout) | retried up to 4 attempts with exponential backoff |
| LLM still failing after retries | assistant message recorded with `status = "error"`; session continues |
| LLM 2xx malformed body | fatal activity error → same `status = "error"` reply |
| `RecordMessage` retried (at-least-once) | idempotent insert, no double-write |
| App hard-killed | parked run left inert; conversation not resumed |

## Testing

- **`chat-service`** (rusqlite + `tempfile`): `record_message` idempotency (a
  repeat insert is a no-op) and `list_messages` ordering by `seq`.
- **`LlmComplete`**: pure `build_request` / `parse_response` unit tests;
  retryable-vs-fatal error classification.
- **`ChatSession`**: drive with the engine's `process_one_*` stepping, registering
  a **test-double `LlmComplete` instance** (enabled by activity DI) returning
  canned replies. Assert a `"message"` signal produces `RecordMessage(user) →
  LlmComplete → RecordMessage(assistant)` in order, and a `"stop"` signal
  terminates the run. Exercises `select_biased!` determinism via replay.
- **Frontend** (Vitest): pure `awaitingReply` and optimistic-merge reducers
  (mirrors `applyCompletion.test.js`).
- **Host**: DTO mapping functions (mirrors the existing pattern in `lib.rs`).

## Out of scope

- Token streaming.
- Resuming / continuing a previous conversation across window-open.
- System prompt / persona configuration (none for now; the conversation is just
  the user/assistant turns). Can be added later as a host constant or service
  field.
- Multiple concurrent in-flight messages (chat is strictly sequential: the input
  is locked until the reply lands).
