# Activity Dependency Injection — Design

**Status:** STUB / DRAFT. Captures initial thinking only. Needs its own
brainstorming pass before implementation. Surfaced while designing the LLM chat
feature (`2026-06-17-llm-chat-design.md`), which depends on this change.

## Problem

Activities are registered by *type*: `Engine::register_activity::<A>()` stores a
runner that calls the static `A::run(ctx, input)`. There is no activity instance,
so an activity cannot hold injected dependencies (an HTTP client, a database/
service client, configuration like a base URL or model name).

Temporal's Go SDK supports this: you register an activity *struct value*
(`RegisterActivity(&Activities{db: ...})`) whose methods close over injected
dependencies. We want the same parity, both for the chat feature (whose
activities need a chat-service client, a `reqwest` client, and LLM config) and as
a general capability.

Today the only workarounds are threading all config through each activity's
`Input` struct, or process-global singletons — neither is the Go-SDK pattern.

## Sketch (to be validated in the dedicated discussion)

### `activity::Definition` — `run` becomes a method

```rust
#[async_trait::async_trait]
pub trait Definition: Send + Sync + 'static {
    type Input;
    type Output;
    const TYPE: &'static str;
    async fn run(&self, ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}
```

The instance is shared across the parallel worker pool, so it (and its injected
dependencies) must be `Send + Sync + 'static`.

### `Engine::register_activity` takes a value

```rust
pub fn register_activity<A: Definition>(&mut self, instance: A) {
    let inst = Arc::new(instance);
    self.activities.insert(A::TYPE.to_string(), Arc::new(move |ctx, bytes| {
        let inst = inst.clone();
        Box::pin(async move {
            let input = serde_json::from_slice(&bytes)?;
            let out = inst.run(ctx, input).await?;   // &self borrow lives inside the owned block
            serde_json::to_vec(&out)
        })
    }));
}
```

- `A::TYPE` remains the registry key.
- The workflow side (`ctx.activity::<A>(input)`) is **unchanged**: it only uses
  `A::TYPE` / `A::Input` / `A::Output`, never `run`. The instance is needed only
  at registration to supply `run`'s `&self`.
- Workflows stay type-registered and dependency-free (they must remain
  deterministic). Only activities gain instance DI.

### Consequence: `Sync` dependencies only

`rusqlite::Connection` is `!Sync`, so a client wrapping a bare connection cannot
live in a shared activity instance. Dependencies must be `Send + Sync` —
e.g. wrap the connection in a `Mutex`, use a connection pool, or hold only a
handle/endpoint and connect per call. (`reqwest::Client` is already
`Send + Sync + Clone`.)

## Migration impact (touch points to inventory in the plan)

The trait change ripples to every activity impl and its registration. All
mechanical (`A::run(...)` → `instance.run(...)`, add `&self`, add the
`Send + Sync` bound):

- `crates/activity/src/def.rs` — the trait and its in-module `Add` test.
- `crates/demo/*` — being removed by the chat feature anyway.
- `crates/engine/src/engine.rs` — `register_activity` signature + `RunnerFn`
  construction.
- Engine tests that register activities (inventory during planning).
- `src-tauri/src/lib.rs` — host registration calls.

## Open questions (for the dedicated brainstorming)

- Should `register_activity` accept `A` by value, `Arc<A>`, or both (ergonomics
  vs. sharing one instance across registrations)?
- Do we want a blanket impl / shim so zero-dependency activities can still be
  written as today (e.g. unit-struct activities) without boilerplate?
- Backoff/retry, `Context`, and `Info` are unchanged — confirm no interaction.
- Is `Mutex<Connection>` acceptable for the chat-service client, or do we want a
  pool from the start? (Chat volume is low; a `Mutex` is likely fine.)
- Any divergence/determinism implications? (Expected: none — activities already
  run outside the deterministic replay loop; only their recorded output feeds
  replay.)

## Relationship to the chat feature

The chat spec assumes this change exists and injects a `chat_service::Client`
(and `reqwest::Client` + LLM config) into its `LlmComplete` / `RecordMessage`
activity instances. If this change is deferred, the chat feature would fall back
to config-via-input as an interim measure.
