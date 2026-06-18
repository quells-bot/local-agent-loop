# Activity Dependency Injection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make activities instance-based so they can hold injected dependencies (HTTP clients, service clients, config), matching Temporal's Go SDK `RegisterActivity(value)` pattern.

**Architecture:** `activity::Definition::run` becomes an `&self` method; `Engine::register_activity` takes the activity *instance* by value and wraps it in an `Arc` shared across the worker pool. The workflow side is unchanged (it references only `A::TYPE`/`Input`/`Output`). This is a breaking trait-signature change, so the trait change and *every* impl/registration site must move together in one atomic commit — the workspace will not compile mid-migration.

**Tech Stack:** Rust, `async_trait`, `tokio`, `serde_json`, `rusqlite` (via `persist`), workspace of engine crates + a Tauri `app` crate.

**Spec:** `docs/superpowers/specs/2026-06-17-activity-dependency-injection-design.md`

## Global Constraints

- **Deterministic concurrency only** inside workflow code: `futures::join!` / `futures::select_biased!` only — never `futures::select!` or a bare `FuturesUnordered`. (Not touched by this plan, but do not introduce it.)
- **Bespoke params/results types:** every activity/workflow takes a named params struct and returns a named results struct — never bare primitives or tuples. (Existing test activities use tuples/primitives; this plan preserves them as-is and does **not** retrofit them — that is out of scope.)
- **`cargo test --workspace`** is the only command that includes the Tauri `app` crate; bare `cargo test` excludes it (`default-members` omits `app`). Always verify with `--workspace`.
- **Conventional commit messages** (`feat(...)`, `refactor(...)`, `test(...)`, `chore(...)`).
- Activity instances are shared across the parallel worker pool, so an activity's injected dependencies must be `Send + Sync`. (All activities in this plan are field-less unit structs, which satisfy this automatically.)

---

### Task 1: Flip `activity::Definition` and `register_activity` to instance-based, migrate every site (atomic)

This is one atomic, behavior-preserving refactor. A new integration test drives it
red→green; the existing workspace suite is the safety net proving no behavior
changed. **Do not run tests between Steps 4 and 9 — the workspace will not compile
until every impl and registration site is migrated.** Everything lands in a single
commit because a breaking trait change cannot be split across commits that each
compile.

**Files:**
- Create: `crates/engine/tests/activity_di.rs`
- Modify: `crates/activity/src/def.rs` (trait at line 6/11; in-module `Add` test at lines 22, 42)
- Modify: `crates/engine/src/engine.rs:103-118` (`register_activity` + closure)
- Modify: `crates/demo/src/activities.rs` (impls at lines 22, 35; tests at lines 83, 91)
- Modify: `crates/engine/tests/end_to_end.rs` (impl line 13; register line 37)
- Modify: `crates/engine/tests/concurrency.rs` (impl line 16; register line 76)
- Modify: `crates/engine/tests/children.rs` (impl line 14; register line 107)
- Modify: `crates/engine/tests/timers.rs` (impl line 13; register line 39)
- Modify: `crates/engine/tests/hardening.rs` (impl line 14; registers lines 53, 70)
- Modify: `crates/engine/tests/equivalence.rs` (impl line 16; register line 55)
- Modify: `crates/demo/tests/flow.rs` (registers lines 13, 14)
- Modify: `src-tauri/src/lib.rs:232-233` (registers)
- Modify: `crates/workflow/src/future.rs`, `crates/workflow/src/state.rs`, `crates/workflow/src/replay.rs` — `#[cfg(test)]` `activity::Definition for Add` impls; add `&self` (same mechanical change as the engine test activities).

**Interfaces:**
- Consumes: existing `engine::{Engine, History, TaskQueue, StartOptions}`, `persist::Sqlite`, `activity::{Definition, Context, Error}`, `workflow::{Definition, Context}` (all already public).
- Produces (the new public shape later tasks and the chat feature rely on):
  - Trait: `activity::Definition: Send + Sync + 'static` with
    `async fn run(&self, ctx: Context, input: Self::Input) -> Result<Self::Output, Error>`.
  - `Engine::register_activity<A: activity::Definition>(&mut self, instance: A)` — by value.

- [ ] **Step 1: Baseline — confirm the whole workspace is green before refactoring**

Run: `cargo test --workspace`
Expected: PASS (all existing engine + demo + app tests pass). This is the safety net; do not proceed if anything is already failing.

- [ ] **Step 2: Write the failing capability test** (`crates/engine/tests/activity_di.rs`)

This test injects a dependency (a constant `addend`) into an activity instance and
asserts the activity uses it. It is written against the *new* API, so it will not
compile until the trait and registration change.

```rust
use std::sync::Arc;

use engine::{Engine, History, StartOptions, TaskQueue};
use persist::Sqlite;

// Activity instance carrying an injected dependency: a constant addend.
struct AddConst {
    addend: i64,
}

#[async_trait::async_trait]
impl activity::Definition for AddConst {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "AddConst";
    async fn run(&self, _c: activity::Context, n: i64) -> Result<i64, activity::Error> {
        Ok(n + self.addend) // uses the injected field
    }
}

// Workflow: calls AddConst(5); the result depends on the injected addend.
struct UseAddConst;

#[async_trait::async_trait(?Send)]
impl workflow::Definition for UseAddConst {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "UseAddConst";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let r = ctx.activity::<AddConst>(5).await?;
        Ok(r)
    }
}

async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        if !drove && !worked {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn injected_dependency_is_used() {
    let db = Sqlite::open_in_memory().unwrap();
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<UseAddConst>();
    e.register_activity(AddConst { addend: 100 }); // inject 100, by value

    let handle = e
        .start_workflow::<UseAddConst>((), StartOptions { id: "di-1".into() })
        .await
        .unwrap();
    pump(&e).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 105, "5 + injected addend 100");
}
```

- [ ] **Step 3: Run the new test to verify it fails (red)**

Run: `cargo test -p engine --test activity_di`
Expected: FAIL — compile error. The closure form differs and `register_activity(AddConst { .. })` takes an argument the current `register_activity::<A>()` does not accept, and `async fn run(&self, ..)` does not match the current static trait method. (Errors like "this function takes 0 arguments" / "method `run` has a `&self` declaration in the impl, but not in the trait".)

- [ ] **Step 4: Change the trait** (`crates/activity/src/def.rs`)

Add the `Send + Sync` supertrait bound and the `&self` receiver:

```rust
// Activities run on the parallel worker pool, so their futures must be Send,
// and the shared instance (with its injected deps) must be Send + Sync.
#[async_trait::async_trait]
pub trait Definition: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;
    const TYPE: &'static str;

    async fn run(&self, ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}
```

Then update the in-module `Add` test impl and its call site in the same file:

```rust
    #[async_trait::async_trait]
    impl Definition for Add {
        type Input = (i64, i64);
        type Output = i64;
        const TYPE: &'static str = "Add";
        async fn run(&self, _ctx: Context, input: (i64, i64)) -> Result<i64, Error> {
            Ok(input.0 + input.1)
        }
    }
```

and change the assertion from the static call to an instance call:

```rust
        assert_eq!(Add.run(ctx, (2, 3)).await.unwrap(), 5);
```

- [ ] **Step 5: Change `register_activity` to take a value** (`crates/engine/src/engine.rs:103-118`)

Replace the whole method body:

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

(`Arc` is already imported in `engine.rs` — it is used by `RunnerFn`. The outer closure stays `Fn`: it captures `inst` by move and clones the `Arc` per invocation so the `&self` borrow lives inside the owned async block.)

- [ ] **Step 6: Migrate the demo activities and their unit tests** (`crates/demo/src/activities.rs`)

Add `&self` to both activity impls:

```rust
    async fn run(&self, _ctx: Context, params: ParseParams) -> Result<ParseResult, Error> {
```

```rust
    async fn run(&self, _ctx: Context, params: SumParams) -> Result<SumResult, Error> {
```

Change the two in-crate test calls from static to instance calls:

```rust
        let out = SumActivity.run(test_ctx(), SumParams { values: vec![1, 2, 3] })
            .await
            .unwrap();
```

```rust
        let out = SumActivity.run(test_ctx(), SumParams { values: vec![] })
            .await
            .unwrap();
```

- [ ] **Step 7: Migrate every engine test activity impl + registration**

In each file, add `&self` to the activity impl's `run`, and change the turbofish
registration to a by-value call. The activities are field-less unit structs, so
`register_activity(Add)` / `register_activity(Double)` constructs the value inline.

`crates/engine/tests/end_to_end.rs`:
```rust
    async fn run(&self, _c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
```
```rust
    e.register_activity(Add);
```

`crates/engine/tests/concurrency.rs`:
```rust
    async fn run(&self, _c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
```
```rust
    e.register_activity(Add);
```

`crates/engine/tests/children.rs`:
```rust
    async fn run(&self, _c: activity::Context, n: i64) -> Result<i64, activity::Error> {
```
```rust
    e.register_activity(Double);
```

`crates/engine/tests/timers.rs`:
```rust
    async fn run(&self, _c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
```
```rust
    e.register_activity(Add);
```

`crates/engine/tests/hardening.rs` (two registration sites — lines 53 and 70):
```rust
    async fn run(&self, _c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
```
```rust
    e.register_activity(Add);
```
```rust
    engine.register_activity(Add); // deliberately no register_workflow
```

`crates/engine/tests/equivalence.rs`:
```rust
    async fn run(&self, _c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
```
```rust
    e.register_activity(Add);
```

- [ ] **Step 8: Migrate the demo flow test and the Tauri host registrations**

`crates/demo/tests/flow.rs` (lines 13-14):
```rust
    e.register_activity(Parse);
    e.register_activity(SumActivity);
```

`src-tauri/src/lib.rs` (lines 232-233):
```rust
            engine.register_activity(Parse);
            engine.register_activity(SumActivity);
```

- [ ] **Step 9: Run the full workspace suite to verify green**

Run: `cargo test --workspace`
Expected: PASS — every existing test still passes (behavior unchanged) **and** the new `injected_dependency_is_used` test passes. The `--workspace` flag is required so the Tauri `app` crate's registration sites are compiled and checked.

- [ ] **Step 10: Commit**

```bash
git add crates/activity/src/def.rs crates/engine/src/engine.rs crates/engine/tests/activity_di.rs \
        crates/demo/src/activities.rs crates/demo/tests/flow.rs \
        crates/engine/tests/end_to_end.rs crates/engine/tests/concurrency.rs \
        crates/engine/tests/children.rs crates/engine/tests/timers.rs \
        crates/engine/tests/hardening.rs crates/engine/tests/equivalence.rs \
        crates/workflow/src/future.rs crates/workflow/src/state.rs crates/workflow/src/replay.rs \
        src-tauri/src/lib.rs
git commit -m "feat(activity): instance-based activities with dependency injection

Make Definition::run an &self method and register_activity take the activity
instance by value (Arc-shared across the worker pool), so activities can hold
injected dependencies. Migrate every impl and registration site. Behavior is
unchanged; a new engine integration test proves an injected dependency is used.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Lock the "fake struct, same TYPE" test-double seam

The chat feature's tests rely on substituting a fake activity registered under the
real activity's type string (spec §4). This task adds an additive regression test
that locks two guarantees: two distinct structs can share a `TYPE`, and
`const TYPE = Real::TYPE` compiles (so the key cannot drift). It exercises the
last-registration-wins behavior of the string-keyed registry. **No production code
changes** — this is pure coverage of behavior enabled in Task 1, so it has no
red phase.

**Files:**
- Create: `crates/engine/tests/activity_double_register.rs`

**Interfaces:**
- Consumes: the public API produced by Task 1 (`register_activity(instance)`, the `&self` trait) plus existing `engine`/`persist` exports.
- Produces: nothing consumed by later tasks (terminal regression test).

- [ ] **Step 1: Write the test** (`crates/engine/tests/activity_double_register.rs`)

```rust
use std::sync::Arc;

use engine::{Engine, History, StartOptions, TaskQueue};
use persist::Sqlite;

// The "real" activity: identity.
struct RealEcho;

#[async_trait::async_trait]
impl activity::Definition for RealEcho {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Echo";
    async fn run(&self, _c: activity::Context, n: i64) -> Result<i64, activity::Error> {
        Ok(n)
    }
}

// A fake claiming the SAME type, with the key linked at compile time (spec §4
// discipline) and the real public Input/Output types reused. Returns a canned value.
struct FakeEcho;

#[async_trait::async_trait]
impl activity::Definition for FakeEcho {
    type Input = i64; // reuse the real Input/Output (here primitives) for compatibility
    type Output = i64;
    const TYPE: &'static str = RealEcho::TYPE; // compile-time-linked registry key
    async fn run(&self, _c: activity::Context, _n: i64) -> Result<i64, activity::Error> {
        Ok(999) // canned reply
    }
}

// Workflow references the REAL activity type; whatever is registered under
// "Echo" actually runs.
struct CallEcho;

#[async_trait::async_trait(?Send)]
impl workflow::Definition for CallEcho {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "CallEcho";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let r = ctx.activity::<RealEcho>(7).await?;
        Ok(r)
    }
}

async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        if !drove && !worked {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn fake_registered_under_real_type_overrides_it() {
    let db = Sqlite::open_in_memory().unwrap();
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<CallEcho>();
    e.register_activity(RealEcho); // real first
    e.register_activity(FakeEcho); // fake registered last, under the same TYPE -> wins

    let handle = e
        .start_workflow::<CallEcho>((), StartOptions { id: "echo-1".into() })
        .await
        .unwrap();
    pump(&e).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 999, "the fake registered under the real type runs");
}
```

- [ ] **Step 2: Run the test to verify it passes**

Run: `cargo test -p engine --test activity_double_register`
Expected: PASS — `fake_registered_under_real_type_overrides_it` returns 999, confirming the workflow's `RealEcho` call dispatched to the last instance registered under `"Echo"` (the fake).

- [ ] **Step 3: Confirm the workspace is still green**

Run: `cargo test --workspace`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add crates/engine/tests/activity_double_register.rs
git commit -m "test(engine): lock the fake-struct-same-TYPE activity override seam

Regression test for the chat feature's test-double pattern (DI spec section 4):
a fake activity registered under the real activity's TYPE overrides it, and
const TYPE = Real::TYPE compiles so the registry key cannot drift.

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review

**1. Spec coverage:**
- §"Single `&self` trait, no shim" → Task 1 Step 4 (trait change). ✓
- §"`register_activity` takes the instance by value" → Task 1 Step 5. ✓
- §"Dependencies must be `Send + Sync`" → encoded as the supertrait bound (Step 4); Global Constraints note. ✓
- §"Test doubles — fake struct, same TYPE" → Task 2 (locks the override + `const TYPE = Real::TYPE`). ✓
- §1 trait shape, §2 registration code → Task 1 Steps 4-5 use the spec's exact code. ✓
- §3 workflow side unchanged → no workflow files modified; verified by Task 1 Step 9 existing-suite pass. ✓
- §Migration impact (every listed file) → Task 1 Steps 4-8 cover `def.rs`, `engine.rs`, `demo/src/activities.rs`, all six engine test files, `demo/tests/flow.rs`, `src-tauri/src/lib.rs`. ✓
- §"DI lands before chat; demo migrated not deleted" → Task 1 migrates `demo`, does not delete it. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to". Every code step shows complete code. ✓

**3. Type consistency:** `register_activity(instance)` signature used identically in Steps 5, 7, 8, and both test files. `async fn run(&self, ..)` consistent across all impls. `pump` helper identical in both new test files (mirrors the existing `end_to_end.rs` helper). `TYPE`/`Input`/`Output` reused as primitives consistently. ✓
