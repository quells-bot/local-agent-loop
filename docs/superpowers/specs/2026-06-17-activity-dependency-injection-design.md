# Activity Dependency Injection — Design

**Status:** Approved design, ready for an implementation plan.

**Motivation:** Surfaced while designing the LLM chat feature
(`2026-06-17-llm-chat-design.md`), which injects a chat-service client, a
`reqwest` client, and LLM config into its activity instances. This change is that
feature's prerequisite, and is a general engine capability in its own right.

## Problem

Activities are registered by *type*: `Engine::register_activity::<A>()` stores a
runner that calls the static `A::run(ctx, input)`. There is no activity instance,
so an activity cannot hold injected dependencies — an HTTP client, a service
client, or configuration like a base URL or model name.

Temporal's Go SDK supports this: you register an activity *struct value*
(`RegisterActivity(&Activities{db: ...})`) whose methods close over injected
dependencies. We want the same parity.

Today the only workarounds are threading all config through each activity's
`Input` struct, or process-global singletons — neither is the Go-SDK pattern.

## Decisions (settled during brainstorming)

- **Single `&self` trait, no shim.** `Definition::run` becomes an `&self` method
  for *all* activities. We rejected both a parallel instance-only path and a
  blanket impl/macro for zero-dependency activities. Rationale: a truly stateless,
  deterministic activity should live in the workflow body instead, so "real"
  stateless activities should not exist. The only field-less activities are the
  handful built for unit tests, and those *should* mirror real activity machinery
  — so the short-term churn of updating them is a feature, not a cost. If the
  zero-field boilerplate ever becomes onerous, we revisit a shim then.
- **`register_activity` takes the instance by value.** It wraps it in `Arc`
  internally. `Arc<A>` as the public input would only help if one *activity
  instance* were shared across multiple type registrations, which never happens —
  sharing is at the dependency level (e.g. `chat.clone()` into two different
  activity structs), and the dependency is already `Clone`. By-value is the
  ergonomic Temporal-parity shape.
- **Dependencies must be `Send + Sync`.** A single instance is shared across the
  parallel worker pool. `reqwest::Client` already satisfies this. `!Sync`
  resources (e.g. `rusqlite::Connection`) must be wrapped (a `Mutex`, a pool, or a
  per-call connect). The specific wrapping for the chat client is a chat-spec
  concern, out of scope here.
- **Test doubles use the "fake struct, same TYPE" pattern.** See the dedicated
  section below. This needs *zero* extra machinery from the DI mechanism — the
  registry is already keyed by the type string.
- **No replay/determinism interaction.** Activities already run outside the
  deterministic replay loop; only their recorded *output* feeds replay. Backoff,
  retry, `Context`, and `Info` are unchanged.

## Design

### 1. The trait — `run` becomes an `&self` method

`crates/activity/src/def.rs`:

```rust
#[async_trait::async_trait]
pub trait Definition: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;
    const TYPE: &'static str;
    async fn run(&self, ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}
```

The instance (and its injected dependencies) is shared across the worker pool, so
the trait gains `Send + Sync`. A dependency-free activity is a field-less struct
whose `run` ignores `&self`.

### 2. Registration — `register_activity` takes a value

`crates/engine/src/engine.rs` (replacing the current `register_activity` at
`engine.rs:103`):

```rust
pub fn register_activity<A: activity::Definition>(&mut self, instance: A) {
    let inst = Arc::new(instance);
    self.activities.insert(
        A::TYPE.to_string(),
        Arc::new(move |ctx, bytes| {
            let inst = inst.clone();
            Box::pin(async move {
                let input: A::Input = serde_json::from_slice(&bytes).map_err(|e| {
                    activity::Error::fatal(format!("activity input deserialize: {e}"))
                })?;
                let out = inst.run(ctx, input).await?;
                serde_json::to_vec(&out).map_err(|e| {
                    activity::Error::fatal(format!("activity output serialize: {e}"))
                })
            })
        }),
    );
}
```

- `A::TYPE` remains the registry key.
- The `Arc` is cloned per invocation so the `&self` borrow lives entirely inside
  the owned async block.
- `RunnerFn` and the rest of the dispatch path are unchanged.

### 3. Workflow side — unchanged

`ctx.activity::<A>(input)` uses only `A::TYPE` / `A::Input` / `A::Output`, never
`run`. The instance is needed only at registration to supply `run`'s `&self`.
Workflows stay type-registered and dependency-free — they must remain
deterministic. Only activities gain instance DI.

### 4. Test doubles — "fake struct, same TYPE"

The activity registry is keyed by the `TYPE` *string*, and the workflow only ever
references `A::TYPE` / `Input` / `Output`. So a test substitutes a fake by
registering a different struct under the same type key — the Rust analog of the Go
SDK's mock-by-name:

```rust
struct FakeLlm { canned: chat::LlmResult }

#[async_trait::async_trait]
impl activity::Definition for FakeLlm {
    type Input  = chat::LlmParams;                       // reuse the REAL public types
    type Output = chat::LlmResult;
    const TYPE: &'static str = chat::LlmComplete::TYPE;  // key linked at compile time
    async fn run(&self, _ctx: Context, _input: LlmParams) -> Result<LlmResult, Error> {
        Ok(self.canned.clone())
    }
}
```

The only Rust wrinkle is that the fake/real contract (matching `TYPE` string and
serialize-compatible `Input`/`Output`) is otherwise checked at *runtime*, not
compile time — a drifted payload surfaces as a deserialize error inside the
activity. Two disciplines neutralize this and make it safer than Go's bare-string
names:

1. **Reuse the real public `Input`/`Output` types** in the fake — payload
   compatibility becomes a compile-time guarantee.
2. **Define `const TYPE = RealActivity::TYPE`** — the registry key cannot drift.

This needs no extra machinery from the DI mechanism itself; string-keyed
registration already enables it.

**Test layering rationale.** The fake deliberately skips the real `run()` body
(for `LlmComplete`: history read → `build_request` → HTTP → `parse_response`).
That logic is covered by activity-level unit tests (pure `build_request` /
`parse_response` functions and error classification, per the chat spec). The
workflow test is then free to test exactly what it should: that a `"message"`
signal drives `RecordMessage → LlmComplete → RecordMessage` in order and that
`select_biased!` replays deterministically. Tests that would need "the real struct
with fake dependencies" belong at the activity level, not the workflow level.

## Migration impact

The trait change ripples to every activity impl and its registration. All
mechanical (`A::run(...)` → `instance.run(...)`, add `&self`, add the
`Send + Sync` bound on impls that need it, turbofish registration → by-value):

- `crates/activity/src/def.rs` — the trait and its in-module `Add` test (add
  `&self` to the test activity; update the `Add::run(...)` call to an instance
  call).
- `crates/engine/src/engine.rs` — `register_activity` signature + `RunnerFn`
  construction.
- Engine tests that register activities — inventory and update during planning.
- `crates/demo/src/activities.rs` — the `Parse` and `SumActivity` impls (add
  `&self`), plus the two in-crate unit tests calling `SumActivity::run(...)`
  (lines ~83 and ~91), which become instance calls. The `demo` workflows
  (`SumChild`, `Parent`) are `workflow::Definition` and unaffected.
- `src-tauri/src/lib.rs:232` — the `Parse` / `SumActivity` registration calls
  (`register_activity::<Parse>()` → `register_activity(Parse)`).

DI lands *before* the chat feature (it is a prerequisite), so the `demo` crate is
live and must be migrated — it is **not** deleted by this change. The blast radius
is still small and entirely mechanical: two demo activities, the in-trait test
activity, and the engine/host registration sites.

## Relationship to the chat feature

The chat spec assumes this change exists and injects a `chat_service::Client`
(and `reqwest::Client` + LLM config) into its `LlmComplete` / `RecordMessage`
activity instances, and relies on the test-double pattern above to drive its
`ChatSession` workflow test. This spec is its prerequisite.

## Out of scope

- Any change to workflow registration or determinism semantics.
- The concrete `!Sync` wrapping for the chat-service client (`Mutex` vs. pool) —
  decided in the chat spec.
- A shim/blanket impl for zero-field activities — deferred unless boilerplate
  proves onerous.
