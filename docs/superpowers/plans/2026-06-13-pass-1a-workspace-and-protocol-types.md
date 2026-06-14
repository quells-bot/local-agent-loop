# Pass 1a — Workspace + Protocol Types — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Cargo workspace (4 crates) and the backend-agnostic
protocol types in the `activity` and `workflow` crates, fully unit-tested.

**Architecture:** No engine logic yet — only the value types that every later
chunk depends on (spec §3, §9). `activity` is the leaf crate; `workflow` depends
on it and re-exports `Execution`. `engine`/`persist` are created as empty stubs so
the workspace compiles end-to-end. Types follow the canonical definitions in the
ROADMAP.

**Tech Stack:** Rust 2021, serde + serde_json, thiserror, async-trait, tokio
(dev), uuid (declared, used later).

---

## File Structure

```
/Cargo.toml                      # workspace
/crates/activity/Cargo.toml
/crates/activity/src/lib.rs      # re-exports
/crates/activity/src/execution.rs
/crates/activity/src/error.rs
/crates/activity/src/info.rs
/crates/activity/src/context.rs
/crates/activity/src/def.rs
/crates/workflow/Cargo.toml
/crates/workflow/src/lib.rs      # re-exports (incl. Execution from activity)
/crates/workflow/src/info.rs
/crates/workflow/src/retry.rs
/crates/workflow/src/command.rs
/crates/workflow/src/event.rs
/crates/workflow/src/result.rs   # CommandResult
/crates/workflow/src/error.rs
/crates/workflow/src/context.rs  # minimal in 1a; replay state added in 1b
/crates/workflow/src/def.rs
/crates/engine/Cargo.toml
/crates/engine/src/lib.rs        # stub
/crates/persist/Cargo.toml
/crates/persist/src/lib.rs       # stub
```

---

### Task 1: Workspace + four crate skeletons

**Files:**
- Create: `Cargo.toml`, `crates/{activity,workflow,engine,persist}/Cargo.toml`,
  and each crate's `src/lib.rs`.

- [ ] **Step 1: Create the workspace manifest**

Create `Cargo.toml`:

```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.dependencies]
serde       = { version = "1", features = ["derive"] }
serde_json  = "1"
thiserror   = "1"
async-trait = "0.1"
tokio       = { version = "1", features = ["rt-multi-thread", "macros", "sync", "time"] }
rusqlite    = { version = "0.31", features = ["bundled"] }
uuid        = { version = "1", features = ["v4"] }
futures     = "0.3"
tempfile    = "3"
```

- [ ] **Step 2: Create the `activity` crate manifest**

Create `crates/activity/Cargo.toml`:

```toml
[package]
name = "activity"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
serde       = { workspace = true }
thiserror   = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
serde_json = { workspace = true }
tokio      = { workspace = true }
```

- [ ] **Step 3: Create the `workflow` crate manifest**

Create `crates/workflow/Cargo.toml`:

```toml
[package]
name = "workflow"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
activity    = { path = "../activity" }
serde       = { workspace = true }
thiserror   = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
serde_json = { workspace = true }
tokio      = { workspace = true }
```

- [ ] **Step 4: Create the `engine` and `persist` stub manifests**

Create `crates/engine/Cargo.toml`:

```toml
[package]
name = "engine"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
workflow = { path = "../workflow" }
activity = { path = "../activity" }
```

Create `crates/persist/Cargo.toml`:

```toml
[package]
name = "persist"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
engine   = { path = "../engine" }
workflow = { path = "../workflow" }
```

- [ ] **Step 5: Create placeholder lib files**

Create `crates/engine/src/lib.rs` and `crates/persist/src/lib.rs`, each with:

```rust
//! Stub — populated in later chunks.
```

Create `crates/activity/src/lib.rs`:

```rust
//! Activity-authoring surface (mirrors Temporal Go SDK `activity` package).
```

Create `crates/workflow/src/lib.rs`:

```rust
//! Workflow-authoring surface + replay protocol (mirrors Go SDK `workflow`).
```

- [ ] **Step 6: Verify the workspace builds**

Run: `cargo build`
Expected: `Finished` with no errors (4 crates compile, empty).

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml crates
git commit -m "feat: scaffold workflow-engine cargo workspace (4 crates)"
```

---

### Task 2: `activity::Execution`

**Files:**
- Create: `crates/activity/src/execution.rs`
- Modify: `crates/activity/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Append to `crates/activity/src/execution.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Execution {
    pub workflow_id: String,
    pub run_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let e = Execution { workflow_id: "order-123".into(), run_id: "run-abc".into() };
        let json = serde_json::to_string(&e).unwrap();
        let back: Execution = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }
}
```

Add to `crates/activity/src/lib.rs`:

```rust
mod execution;
pub use execution::Execution;
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p activity execution`
Expected: at first the module/test may not yet compile if `lib.rs` wasn't
updated; once it compiles, the test PASSES (this task's "implementation" and test
are written together because the type is trivial). If it does not compile, fix
the `mod`/`pub use` lines, then re-run.

- [ ] **Step 3: Run test to verify it passes**

Run: `cargo test -p activity execution`
Expected: `test execution::tests::round_trips_through_json ... ok`

- [ ] **Step 4: Commit**

```bash
git add crates/activity
git commit -m "feat(activity): add Execution id-pair type"
```

---

### Task 3: `activity::Error`

**Files:**
- Create: `crates/activity/src/error.rs`
- Modify: `crates/activity/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/activity/src/error.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub message: String,
    pub non_retryable: bool,
}

impl Error {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self { message: message.into(), non_retryable: false }
    }
    pub fn fatal(message: impl Into<String>) -> Self {
        Self { message: message.into(), non_retryable: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctors_set_retryability() {
        assert!(!Error::retryable("boom").non_retryable);
        assert!(Error::fatal("boom").non_retryable);
    }

    #[test]
    fn displays_message() {
        assert_eq!(Error::fatal("nope").to_string(), "nope");
    }

    #[test]
    fn round_trips_through_json() {
        let e = Error::fatal("x");
        let back: Error = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
}
```

Add to `crates/activity/src/lib.rs`:

```rust
mod error;
pub use error::Error;
```

- [ ] **Step 2: Run test to verify it fails/compiles**

Run: `cargo test -p activity error`
Expected: compiles and PASSES. If a compile error mentions `thiserror`, confirm
the dependency is present in `crates/activity/Cargo.toml`.

- [ ] **Step 3: Commit**

```bash
git add crates/activity
git commit -m "feat(activity): add Error with retryable/fatal ctors"
```

---

### Task 4: `activity::Info`

**Files:**
- Create: `crates/activity/src/info.rs`
- Modify: `crates/activity/src/lib.rs`

- [ ] **Step 1: Write the type + test**

Create `crates/activity/src/info.rs`:

```rust
use crate::Execution;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub execution: Execution,
    pub activity_id: String,
    pub activity_type: String,
    pub attempt: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_fields() {
        let i = Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            activity_id: "7".into(),
            activity_type: "Charge".into(),
            attempt: 1,
        };
        assert_eq!(i.activity_type, "Charge");
        assert_eq!(i.execution.run_id, "r");
    }
}
```

Add to `crates/activity/src/lib.rs`:

```rust
mod info;
pub use info::Info;
```

- [ ] **Step 2: Run test**

Run: `cargo test -p activity info`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/activity
git commit -m "feat(activity): add Info"
```

---

### Task 5: `activity::Context` (with `idempotency_key`)

**Files:**
- Create: `crates/activity/src/context.rs`
- Modify: `crates/activity/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/activity/src/context.rs`:

```rust
use crate::Info;

#[derive(Clone)]
pub struct Context {
    info: Info,
}

impl Context {
    pub fn new(info: Info) -> Self {
        Self { info }
    }

    pub fn info(&self) -> &Info {
        &self.info
    }

    /// Stable across retries/redeliveries: "{run_id}:{activity_id}" (spec §8).
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.info.execution.run_id, self.info.activity_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Execution;

    fn ctx() -> Context {
        Context::new(Info {
            execution: Execution { workflow_id: "order-1".into(), run_id: "run-9".into() },
            activity_id: "3".into(),
            activity_type: "Charge".into(),
            attempt: 2,
        })
    }

    #[test]
    fn idempotency_key_is_run_id_colon_activity_id() {
        assert_eq!(ctx().idempotency_key(), "run-9:3");
    }

    #[test]
    fn info_is_accessible() {
        assert_eq!(ctx().info().attempt, 2);
    }
}
```

Add to `crates/activity/src/lib.rs`:

```rust
mod context;
pub use context::Context;
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p activity context`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/activity
git commit -m "feat(activity): add Context with idempotency_key"
```

---

### Task 6: `activity::Definition` trait

**Files:**
- Create: `crates/activity/src/def.rs`
- Modify: `crates/activity/src/lib.rs`

- [ ] **Step 1: Write the trait + a sample impl test**

Create `crates/activity/src/def.rs`:

```rust
use crate::{Context, Error};
use serde::{de::DeserializeOwned, Serialize};

// Activities run on the parallel worker pool, so their futures must be Send.
#[async_trait::async_trait]
pub trait Definition: 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;
    const TYPE: &'static str;

    async fn run(ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Execution, Info};

    struct Add;

    #[async_trait::async_trait]
    impl Definition for Add {
        type Input = (i64, i64);
        type Output = i64;
        const TYPE: &'static str = "Add";
        async fn run(_ctx: Context, input: (i64, i64)) -> Result<i64, Error> {
            Ok(input.0 + input.1)
        }
    }

    #[tokio::test]
    async fn sample_activity_runs() {
        let ctx = Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            activity_id: "1".into(),
            activity_type: Add::TYPE.into(),
            attempt: 1,
        });
        assert_eq!(Add::run(ctx, (2, 3)).await.unwrap(), 5);
        assert_eq!(Add::TYPE, "Add");
    }
}
```

Add to `crates/activity/src/lib.rs`:

```rust
mod def;
pub use def::Definition;
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p activity def`
Expected: `sample_activity_runs ... ok`. If `async_trait` is unresolved, confirm
it is in `crates/activity/Cargo.toml`.

- [ ] **Step 3: Commit**

```bash
git add crates/activity
git commit -m "feat(activity): add Definition trait"
```

---

### Task 7: `workflow` re-exports + `workflow::Info`

**Files:**
- Create: `crates/workflow/src/info.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Write the type + test (re-using activity::Execution)**

Create `crates/workflow/src/info.rs`:

```rust
use activity::Execution;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub execution: Execution,
    pub parent: Option<Execution>,
    pub workflow_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_defaults_to_none_for_root() {
        let i = Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "OrderWorkflow".into(),
        };
        assert!(i.parent.is_none());
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
pub use activity::Execution; // re-export so workflow::Execution exists (spec §9)

mod info;
pub use info::Info;
```

- [ ] **Step 2: Run test**

Run: `cargo test -p workflow info`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): re-export Execution; add Info"
```

---

### Task 8: `workflow::RetryPolicy`

**Files:**
- Create: `crates/workflow/src/retry.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Write the failing test**

Create `crates/workflow/src/retry.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_ms: u64,
    pub multiplier: u32,
}

impl RetryPolicy {
    pub fn exponential(max_attempts: u32) -> Self {
        Self { max_attempts, initial_ms: 100, multiplier: 2 }
    }

    pub fn none() -> Self {
        Self { max_attempts: 1, initial_ms: 0, multiplier: 1 }
    }

    /// Delay before the given 1-based attempt. Attempt 1 (first try) has no delay.
    pub fn backoff_ms(&self, attempt: u32) -> u64 {
        if attempt <= 1 {
            return 0;
        }
        self.initial_ms
            .saturating_mul((self.multiplier as u64).saturating_pow(attempt - 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_schedule() {
        let p = RetryPolicy::exponential(5);
        assert_eq!(p.backoff_ms(1), 0);    // first attempt: immediate
        assert_eq!(p.backoff_ms(2), 100);  // initial
        assert_eq!(p.backoff_ms(3), 200);  // *2
        assert_eq!(p.backoff_ms(4), 400);  // *2
    }

    #[test]
    fn none_means_single_attempt() {
        assert_eq!(RetryPolicy::none().max_attempts, 1);
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
mod retry;
pub use retry::RetryPolicy;
```

- [ ] **Step 2: Run test to verify it passes**

Run: `cargo test -p workflow retry`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): add RetryPolicy with backoff schedule"
```

---

### Task 9: `workflow::Command`

**Files:**
- Create: `crates/workflow/src/command.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Write the type + serde test**

Create `crates/workflow/src/command.rs`:

```rust
use crate::RetryPolicy;
use serde::{Deserialize, Serialize};

/// Issued by workflow futures, drained by the driver each turn (spec §3).
/// Pass 2 adds StartTimer; Pass 4 adds StartChild.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    ScheduleActivity {
        seq: u64,
        activity_type: String,
        input: Vec<u8>,
        retry: RetryPolicy,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let c = Command::ScheduleActivity {
            seq: 1,
            activity_type: "Charge".into(),
            input: b"{}".to_vec(),
            retry: RetryPolicy::exponential(3),
        };
        let back: Command = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
mod command;
pub use command::Command;
```

- [ ] **Step 2: Run test**

Run: `cargo test -p workflow command`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): add Command enum (ScheduleActivity)"
```

---

### Task 10: `workflow::Event` (with `kind()`)

**Files:**
- Create: `crates/workflow/src/event.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Write the type + tests**

Create `crates/workflow/src/event.rs`:

```rust
use crate::RetryPolicy;
use serde::{Deserialize, Serialize};

/// One row of history (spec §11). Pass 2 adds TimerFired; Pass 3 SignalReceived;
/// Pass 4 ChildCompleted; WorkflowCancelRequested is reserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    WorkflowStarted { input: Vec<u8> },
    ActivityScheduled { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    ActivityCompleted { seq: u64, output: Vec<u8> },
    ActivityFailed { seq: u64, error: activity::Error },
}

impl Event {
    /// Discriminant string stored in `history.kind`.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::WorkflowStarted { .. } => "WorkflowStarted",
            Event::ActivityScheduled { .. } => "ActivityScheduled",
            Event::ActivityCompleted { .. } => "ActivityCompleted",
            Event::ActivityFailed { .. } => "ActivityFailed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_matches_variant() {
        assert_eq!(Event::WorkflowStarted { input: vec![] }.kind(), "WorkflowStarted");
        assert_eq!(
            Event::ActivityCompleted { seq: 1, output: vec![] }.kind(),
            "ActivityCompleted"
        );
    }

    #[test]
    fn round_trips_through_json() {
        let e = Event::ActivityFailed { seq: 2, error: activity::Error::fatal("x") };
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
mod event;
pub use event::Event;
```

- [ ] **Step 2: Run test**

Run: `cargo test -p workflow event`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): add Event enum with kind()"
```

---

### Task 11: `workflow::CommandResult`

**Files:**
- Create: `crates/workflow/src/result.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Write the type + conversion test**

Create `crates/workflow/src/result.rs`:

```rust
/// The recorded outcome the driver applies into `ContextInner.results` (spec §3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandResult {
    ActivityCompleted(Vec<u8>),
    ActivityFailed(activity::Error),
}

impl From<CommandResult> for Result<Vec<u8>, activity::Error> {
    fn from(r: CommandResult) -> Self {
        match r {
            CommandResult::ActivityCompleted(output) => Ok(output),
            CommandResult::ActivityFailed(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_converts_to_ok() {
        let r: Result<Vec<u8>, activity::Error> =
            CommandResult::ActivityCompleted(b"hi".to_vec()).into();
        assert_eq!(r.unwrap(), b"hi");
    }

    #[test]
    fn failed_converts_to_err() {
        let r: Result<Vec<u8>, activity::Error> =
            CommandResult::ActivityFailed(activity::Error::fatal("boom")).into();
        assert_eq!(r.unwrap_err().message, "boom");
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
mod result;
pub use result::CommandResult;
```

- [ ] **Step 2: Run test**

Run: `cargo test -p workflow result`
Expected: both tests PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): add CommandResult and Result conversion"
```

---

### Task 12: `workflow::Error`

**Files:**
- Create: `crates/workflow/src/error.rs`
- Modify: `crates/workflow/src/lib.rs`

- [ ] **Step 1: Write the type + test**

Create `crates/workflow/src/error.rs`:

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub message: String,
}

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_displays() {
        let e = Error::new("workflow blew up");
        assert_eq!(e.to_string(), "workflow blew up");
        let back: Error = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
mod error;
pub use error::Error;
```

- [ ] **Step 2: Run test**

Run: `cargo test -p workflow error`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): add Error"
```

---

### Task 13: minimal `workflow::Context` + `workflow::Definition`

**Files:**
- Create: `crates/workflow/src/context.rs`, `crates/workflow/src/def.rs`
- Modify: `crates/workflow/src/lib.rs`

> NOTE: `Context` here is intentionally minimal (holds only `Info`). Chunk 1b
> replaces its internals with `Rc<ContextInner>` carrying the replay state
> (`results`, `scheduled`, `commands`, `next_seq`) and adds `activity()`,
> `now()`, `random()`. Keep the `info()` method stable across that change.

- [ ] **Step 1: Write minimal Context + test**

Create `crates/workflow/src/context.rs`:

```rust
use crate::Info;

/// Workflow-side context. Minimal in 1a; replay machinery added in 1b.
#[derive(Clone)]
pub struct Context {
    info: Info,
}

impl Context {
    pub fn new(info: Info) -> Self {
        Self { info }
    }

    pub fn info(&self) -> &Info {
        &self.info
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::Execution;

    #[test]
    fn exposes_info() {
        let ctx = Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "OrderWorkflow".into(),
        });
        assert_eq!(ctx.info().workflow_type, "OrderWorkflow");
    }
}
```

- [ ] **Step 2: Write the Definition trait + sample impl test**

Create `crates/workflow/src/def.rs`:

```rust
use crate::{Context, Error};
use serde::{de::DeserializeOwned, Serialize};

// Workflow futures hold Rc/RefCell (single-threaded decision loop, spec §5.1),
// so they are NOT Send — hence `?Send`. Activities are the Send half.
#[async_trait::async_trait(?Send)]
pub trait Definition: 'static {
    type Input: Serialize + DeserializeOwned + 'static;
    type Output: Serialize + DeserializeOwned + 'static;
    const TYPE: &'static str;

    async fn run(ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Info;
    use activity::Execution;

    struct Echo;

    #[async_trait::async_trait(?Send)]
    impl Definition for Echo {
        type Input = String;
        type Output = String;
        const TYPE: &'static str = "Echo";
        async fn run(_ctx: Context, input: String) -> Result<String, Error> {
            Ok(input)
        }
    }

    #[tokio::test]
    async fn sample_workflow_runs() {
        let ctx = Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: Echo::TYPE.into(),
        });
        assert_eq!(Echo::run(ctx, "hi".into()).await.unwrap(), "hi");
    }
}
```

Add to `crates/workflow/src/lib.rs`:

```rust
mod context;
pub use context::Context;

mod def;
pub use def::Definition;
```

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test -p workflow`
Expected: all `workflow` tests PASS (info, retry, command, event, result, error,
context, def).

- [ ] **Step 4: Commit**

```bash
git add crates/workflow
git commit -m "feat(workflow): add minimal Context and Definition trait"
```

---

### Task 14: Whole-workspace green check

- [ ] **Step 1: Build and test everything**

Run: `cargo test`
Expected: all tests across `activity` and `workflow` PASS; `engine`/`persist`
compile (no tests yet).

- [ ] **Step 2: Lint clean**

Run: `cargo clippy --all-targets -- -D warnings`
Expected: no warnings. Fix any (likely unused-import) before continuing.

- [ ] **Step 3: Commit (if clippy required changes)**

```bash
git add -A
git commit -m "chore: clippy-clean pass 1a"
```

---

## Self-Review (completed during authoring)

- **Spec coverage:** §9 types (`Execution`, both `Info`, both `Context`,
  `idempotency_key`, both `Definition`), §3 protocol (`Command`, `Event`,
  `CommandResult`, `RetryPolicy`), §10 workspace layout + naming — all have tasks.
  Replay behavior (§3 futures/`run_turn`, §4, §12) is intentionally deferred to
  1b; persistence (§11) to 1c; driver/workers/start/observer (§5, §7, §8) to 1d.
- **Placeholders:** none — every step has complete code and exact commands.
- **Type consistency:** matches the ROADMAP canonical types verbatim;
  `Context::info()` signature is stable into 1b.
