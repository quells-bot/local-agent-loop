# Tauri Chat App Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wrap the finished workflow engine in a Tauri desktop app with a minimal SvelteKit chat UI that runs a parse→sum demo workflow and displays the result (or a parse error).

**Architecture:** A new tauri-free `crates/demo` library holds the demo workflows/activities (bespoke params/result types). `src-tauri` is a thin host that opens a SQLite-backed engine, registers the demo types, exposes a `submit` command, and pushes run completions to the frontend via the engine's `on_run_completed` observer → `app_handle.emit` (spec §7.3). The SvelteKit frontend (Svelte 5 runes, JSDoc) generates a `workflow_id` per submission, invokes `submit`, and correlates the pushed `run_completed` event back to its chat bubble through a pure `applyCompletion` reducer.

**Tech Stack:** Rust (existing engine/workflow/activity/persist crates), Tauri v2, SvelteKit 2 + Svelte 5 (runes), JavaScript + JSDoc, Vite, Vitest.

**Spec:** `docs/superpowers/specs/2026-06-15-tauri-chat-app-design.md`. "§N" references point to `docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md`.

---

## Prerequisites (environment, one-time)

The pure-Rust (`crates/demo`) and frontend-logic (Vitest, `npm run build`) tasks need only the existing Rust toolchain + Node (both present). The **Tauri host crate and the GUI run** additionally require:

- **Tauri Linux system libraries** (need sudo + network):
  ```bash
  sudo apt-get update
  sudo apt-get install -y libwebkit2gtk-4.1-dev build-essential curl wget file \
    libxdo-dev libssl-dev libayatana-appindicator3-dev librsvg2-dev pkg-config
  ```
- **Tauri CLI**, installed project-local once the frontend `package.json` exists (added in Task 6, run as `npm run tauri ...`).

If these cannot be installed in the working environment, Tasks 1–9 are still fully verifiable; Tasks 10–11 (compiling `src-tauri` and the GUI smoke test) are environment-gated — write the code, but their `cargo build -p app` / `cargo tauri dev` verification must run on a machine with the libraries.

---

## File Structure

```
/Cargo.toml                         MODIFY: members += "src-tauri"; add default-members = ["crates/*"]
/crates/demo/                       NEW Rust library (tauri-free; deps: activity, workflow)
  Cargo.toml
  src/lib.rs                        module decls + public re-exports
  src/types.rs                      ParseParams/Result, SumParams/Result, SumChildParams/Result, ParentParams/Result
  src/activities.rs                 Parse, SumActivity (+ unit tests of pure helpers / ::run)
  src/workflows.rs                  SumChild, Parent
  tests/flow.rs                     integration: in-memory SQLite engine, pump-to-quiescence
/package.json, /svelte.config.js, /vite.config.js, /jsconfig.json, /app.html (via src/), /static/
                                    NEW frontend (scaffolded, then edited)
  src/routes/+layout.js             ssr=false, prerender=true
  src/routes/+page.svelte           ChatView: input + submit + scrolling transcript
  src/lib/applyCompletion.js        pure reducer (event payload → next messages array)
  src/lib/applyCompletion.test.js   Vitest unit tests
/src-tauri/                         NEW Tauri host crate (scaffolded, then rewired)
  Cargo.toml                        name = "app"; path deps to crates/*, demo
  tauri.conf.json                   frontendDist ../build, devUrl :1420
  capabilities/default.json         core:default + event permission
  src/main.rs                       calls app_lib::run()
  src/lib.rs                        CompletionPayload, submit command, setup wiring
```

---

## Part A — `crates/demo` (pure Rust, fully verifiable now)

### Task 1: Create the `demo` crate skeleton + params/result types

**Files:**
- Create: `crates/demo/Cargo.toml`
- Create: `crates/demo/src/lib.rs`
- Create: `crates/demo/src/types.rs`

- [ ] **Step 1: Write `crates/demo/Cargo.toml`**

```toml
[package]
name = "demo"
version = "0.0.0"
edition = "2021"
publish = false

[dependencies]
activity    = { path = "../activity" }
workflow    = { path = "../workflow" }
serde       = { workspace = true }
async-trait = { workspace = true }

[dev-dependencies]
engine     = { path = "../engine" }
persist    = { path = "../persist" }
tokio      = { workspace = true }
serde_json = { workspace = true }
```

- [ ] **Step 2: Write `crates/demo/src/types.rs`** (bespoke params/result types — house style, spec §4)

```rust
//! Bespoke params/result types: every activity/workflow takes a named params
//! struct and returns a named results struct, so business changes are added
//! fields, not breaking signature changes (spec §4).
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParseParams {
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParseResult {
    pub values: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumParams {
    pub values: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumResult {
    pub total: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumChildParams {
    pub values: Vec<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SumChildResult {
    pub total: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentParams {
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParentResult {
    pub total: i64,
}
```

- [ ] **Step 3: Write `crates/demo/src/lib.rs`** (types-only for now; Task 2 adds `mod activities;` and Task 4 adds `mod workflows;` with their re-exports)

```rust
//! Demo workflows + activities for the Tauri chat app: parse space-separated
//! integers, then sum them via a child workflow (spec §4).
mod types;

pub use types::{
    ParentParams, ParentResult, ParseParams, ParseResult, SumChildParams, SumChildResult,
    SumParams, SumResult,
};
```

- [ ] **Step 4: Verify it builds**

Run: `cargo build -p demo`
Expected: compiles clean (only the types module so far).

- [ ] **Step 5: Commit**

```bash
git add crates/demo/Cargo.toml crates/demo/src/
git commit -m "feat(demo): scaffold demo crate with bespoke params/result types"
```

---

### Task 2: `Parse` activity (TDD on the pure `parse_ints` helper)

**Files:**
- Modify: `crates/demo/src/activities.rs`
- Modify: `crates/demo/src/lib.rs` (add `mod activities;` + re-export)

- [ ] **Step 1: Write the failing test** — put this in `crates/demo/src/activities.rs`:

```rust
use crate::types::{ParseParams, ParseResult};
use activity::{Context, Definition, Error};

/// Pure parse: split on whitespace, parse each token as i64. Empty input is a
/// valid empty list (spec §4). A bad token is a fatal (non-retryable) error.
fn parse_ints(text: &str) -> Result<Vec<i64>, String> {
    text.split_whitespace()
        .map(|tok| {
            tok.parse::<i64>()
                .map_err(|_| format!("could not parse '{tok}' as an integer"))
        })
        .collect()
}

pub struct Parse;

#[async_trait::async_trait]
impl Definition for Parse {
    type Input = ParseParams;
    type Output = ParseResult;
    const TYPE: &'static str = "Parse";
    async fn run(_ctx: Context, params: ParseParams) -> Result<ParseResult, Error> {
        let values = parse_ints(&params.text).map_err(Error::fatal)?;
        Ok(ParseResult { values })
    }
}

pub struct SumActivity; // implemented in Task 3

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_space_separated_integers() {
        assert_eq!(parse_ints("1 2 3"), Ok(vec![1, 2, 3]));
    }

    #[test]
    fn empty_input_is_empty_list() {
        assert_eq!(parse_ints(""), Ok(vec![]));
        assert_eq!(parse_ints("   "), Ok(vec![]));
    }

    #[test]
    fn parses_negative_integers() {
        assert_eq!(parse_ints("-5 10"), Ok(vec![-5, 10]));
    }

    #[test]
    fn bad_token_is_an_error_naming_the_token() {
        let err = parse_ints("1 two 3").unwrap_err();
        assert!(err.contains("two"), "got: {err}");
    }
}
```

Note: `SumActivity` here is a bare unit struct with no `Definition` impl yet — it exists so Task 2's `lib.rs` re-export of `SumActivity` compiles. Task 3 adds its impl.

- [ ] **Step 2: Add the `activities` module + re-export to `lib.rs`**

`crates/demo/src/lib.rs`:
```rust
//! Demo workflows + activities for the Tauri chat app: parse space-separated
//! integers, then sum them via a child workflow (spec §4).
mod activities;
mod types;

pub use activities::{Parse, SumActivity};
pub use types::{
    ParentParams, ParentResult, ParseParams, ParseResult, SumChildParams, SumChildResult,
    SumParams, SumResult,
};
```

(`mod workflows;` and its re-exports arrive in Task 4.)

- [ ] **Step 3: Run the tests to verify they pass**

Run: `cargo test -p demo --lib`
Expected: 4 tests pass (`parses_space_separated_integers`, `empty_input_is_empty_list`, `parses_negative_integers`, `bad_token_is_an_error_naming_the_token`).

- [ ] **Step 4: Commit**

```bash
git add crates/demo/src/activities.rs crates/demo/src/lib.rs
git commit -m "feat(demo): Parse activity with whitespace-separated int parsing"
```

---

### Task 3: `SumActivity` activity (TDD on `::run`)

**Files:**
- Modify: `crates/demo/src/activities.rs`

- [ ] **Step 1: Write the failing test** — replace the placeholder `pub struct SumActivity;` line with the impl, and add a test. The full `crates/demo/src/activities.rs` becomes:

```rust
use crate::types::{ParseParams, ParseResult, SumParams, SumResult};
use activity::{Context, Definition, Error};

fn parse_ints(text: &str) -> Result<Vec<i64>, String> {
    text.split_whitespace()
        .map(|tok| {
            tok.parse::<i64>()
                .map_err(|_| format!("could not parse '{tok}' as an integer"))
        })
        .collect()
}

pub struct Parse;

#[async_trait::async_trait]
impl Definition for Parse {
    type Input = ParseParams;
    type Output = ParseResult;
    const TYPE: &'static str = "Parse";
    async fn run(_ctx: Context, params: ParseParams) -> Result<ParseResult, Error> {
        let values = parse_ints(&params.text).map_err(Error::fatal)?;
        Ok(ParseResult { values })
    }
}

pub struct SumActivity;

#[async_trait::async_trait]
impl Definition for SumActivity {
    type Input = SumParams;
    type Output = SumResult;
    const TYPE: &'static str = "Sum";
    async fn run(_ctx: Context, params: SumParams) -> Result<SumResult, Error> {
        Ok(SumResult {
            total: params.values.iter().sum(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::{Execution, Info};

    fn test_ctx() -> Context {
        Context::new(Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            activity_id: "0".into(),
            activity_type: "Sum".into(),
            attempt: 1,
        })
    }

    #[test]
    fn parses_space_separated_integers() {
        assert_eq!(parse_ints("1 2 3"), Ok(vec![1, 2, 3]));
    }

    #[test]
    fn empty_input_is_empty_list() {
        assert_eq!(parse_ints(""), Ok(vec![]));
        assert_eq!(parse_ints("   "), Ok(vec![]));
    }

    #[test]
    fn parses_negative_integers() {
        assert_eq!(parse_ints("-5 10"), Ok(vec![-5, 10]));
    }

    #[test]
    fn bad_token_is_an_error_naming_the_token() {
        let err = parse_ints("1 two 3").unwrap_err();
        assert!(err.contains("two"), "got: {err}");
    }

    #[tokio::test]
    async fn sum_activity_totals_values() {
        let out = SumActivity::run(test_ctx(), SumParams { values: vec![1, 2, 3] })
            .await
            .unwrap();
        assert_eq!(out, SumResult { total: 6 });
    }

    #[tokio::test]
    async fn sum_activity_empty_is_zero() {
        let out = SumActivity::run(test_ctx(), SumParams { values: vec![] })
            .await
            .unwrap();
        assert_eq!(out, SumResult { total: 0 });
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test -p demo --lib`
Expected: 6 tests pass (the 4 from Task 2 plus `sum_activity_totals_values`, `sum_activity_empty_is_zero`).

- [ ] **Step 3: Commit**

```bash
git add crates/demo/src/activities.rs
git commit -m "feat(demo): SumActivity totals a list of integers"
```

---

### Task 4: `Parent` + `SumChild` workflows (TDD via integration test)

**Files:**
- Create: `crates/demo/src/workflows.rs`
- Modify: `crates/demo/src/lib.rs` (add `mod workflows;` + re-export)
- Create: `crates/demo/tests/flow.rs`

- [ ] **Step 1: Write the failing integration test** — `crates/demo/tests/flow.rs`:

```rust
use std::sync::Arc;

use demo::{Parent, ParentParams, ParentResult, Parse, SumActivity, SumChild};
use engine::{Engine, History, StartOptions, TaskQueue};
use persist::Sqlite;

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Parent>();
    e.register_workflow::<SumChild>();
    e.register_activity::<Parse>();
    e.register_activity::<SumActivity>();
    e
}

/// Pump driver + worker turns until quiescent (deterministic; no background loops).
async fn pump(engine: &Engine) {
    loop {
        let drove = engine.process_one_runnable().await.unwrap();
        let worked = engine.process_one_activity().await.unwrap();
        if !drove && !worked {
            return;
        }
    }
}

#[tokio::test]
async fn sums_space_separated_integers() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(
            ParentParams { text: "1 2 3".into() },
            StartOptions { id: "calc-1".into() },
        )
        .await
        .unwrap();

    pump(&engine).await;

    let out: ParentResult = handle.result().await.unwrap();
    assert_eq!(out, ParentResult { total: 6 });
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p demo --test flow`
Expected: FAIL to compile — `Parent` / `SumChild` are not exported (no `workflows` module yet).

- [ ] **Step 3: Write `crates/demo/src/workflows.rs`**

```rust
use crate::activities::{Parse, SumActivity};
use crate::types::{
    ParentParams, ParentResult, ParseParams, SumChildParams, SumChildResult, SumParams,
};
use workflow::{Context, Definition, Error};

/// Child: sum a list of integers via the `SumActivity`.
pub struct SumChild;

#[async_trait::async_trait(?Send)]
impl Definition for SumChild {
    type Input = SumChildParams;
    type Output = SumChildResult;
    const TYPE: &'static str = "SumChild";
    async fn run(ctx: Context, params: SumChildParams) -> Result<SumChildResult, Error> {
        let summed = ctx
            .activity::<SumActivity>(SumParams { values: params.values })
            .await?;
        Ok(SumChildResult { total: summed.total })
    }
}

/// Parent: parse text → integers (parse failure fails the workflow via `?`),
/// then delegate the sum to the `SumChild` child workflow (spec §4).
pub struct Parent;

#[async_trait::async_trait(?Send)]
impl Definition for Parent {
    type Input = ParentParams;
    type Output = ParentResult;
    const TYPE: &'static str = "Parent";
    async fn run(ctx: Context, params: ParentParams) -> Result<ParentResult, Error> {
        let parsed = ctx
            .activity::<Parse>(ParseParams { text: params.text })
            .await?;
        let summed = ctx
            .child_workflow::<SumChild>(SumChildParams { values: parsed.values })
            .await?;
        Ok(ParentResult { total: summed.total })
    }
}
```

- [ ] **Step 4: Add the module + re-exports back to `crates/demo/src/lib.rs`** (final form)

```rust
//! Demo workflows + activities for the Tauri chat app: parse space-separated
//! integers, then sum them via a child workflow (spec §4).
mod activities;
mod types;
mod workflows;

pub use activities::{Parse, SumActivity};
pub use types::{
    ParentParams, ParentResult, ParseParams, ParseResult, SumChildParams, SumChildResult,
    SumParams, SumResult,
};
pub use workflows::{Parent, SumChild};
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `cargo test -p demo --test flow`
Expected: PASS (`sums_space_separated_integers`).

- [ ] **Step 6: Commit**

```bash
git add crates/demo/src/workflows.rs crates/demo/src/lib.rs crates/demo/tests/flow.rs
git commit -m "feat(demo): Parent + SumChild workflows; parse->child-sum flow"
```

---

### Task 5: Integration coverage — empty, negative, and parse-failure paths

**Files:**
- Modify: `crates/demo/tests/flow.rs`

- [ ] **Step 1: Add the failing/edge tests** — append to `crates/demo/tests/flow.rs` (add the extra imports `ExecStatus` and `workflow` need):

Add `ExecStatus` to the engine import line:
```rust
use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
```

Add `serde_json` and `workflow` as dev-dependencies if not already present — they are (Task 1 dev-deps include `serde_json`; `workflow` is a normal dep). Then append:

```rust
#[tokio::test]
async fn empty_input_sums_to_zero() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(
            ParentParams { text: "".into() },
            StartOptions { id: "calc-empty".into() },
        )
        .await
        .unwrap();
    pump(&engine).await;
    let out: ParentResult = handle.result().await.unwrap();
    assert_eq!(out, ParentResult { total: 0 });
}

#[tokio::test]
async fn negative_integers_sum() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(
            ParentParams { text: "-5 10".into() },
            StartOptions { id: "calc-neg".into() },
        )
        .await
        .unwrap();
    pump(&engine).await;
    let out: ParentResult = handle.result().await.unwrap();
    assert_eq!(out, ParentResult { total: 5 });
}

#[tokio::test]
async fn parse_failure_fails_the_workflow_with_a_message() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<Parent>(
            ParentParams { text: "1 two 3".into() },
            StartOptions { id: "calc-err".into() },
        )
        .await
        .unwrap();
    pump(&engine).await;

    let (_run_id, status, result) = db.find_execution("calc-err").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Failed);
    let err: workflow::Error = serde_json::from_slice(&result.unwrap()).unwrap();
    assert!(err.message.contains("two"), "got: {}", err.message);
}
```

- [ ] **Step 2: Run the full demo test suite**

Run: `cargo test -p demo`
Expected: all pass — 6 lib unit tests + 4 integration tests (`sums_space_separated_integers`, `empty_input_sums_to_zero`, `negative_integers_sum`, `parse_failure_fails_the_workflow_with_a_message`).

- [ ] **Step 3: Commit**

```bash
git add crates/demo/tests/flow.rs
git commit -m "test(demo): empty, negative, and parse-failure flow coverage"
```

---

## Part B — Frontend (Node; Vitest + build verifiable now)

### Task 6: Scaffold the Tauri + SvelteKit app and rewire the Cargo workspace

This uses the official scaffolder for a known-good, version-matched skeleton, then ports the pieces into the existing workspace. (Fallback if `create-tauri-app` is unavailable: hand-write the files — but the scaffolder is strongly preferred to avoid Tauri-v2 config drift.)

**Files:**
- Create (scaffolded): `/package.json`, `/package-lock.json`, `/svelte.config.js`, `/vite.config.js`, `/jsconfig.json`, `/.npmrc`, `/src/` (routes, `app.html`, `lib`), `/static/`
- Create (scaffolded): `/src-tauri/` (`Cargo.toml`, `build.rs`, `tauri.conf.json`, `capabilities/`, `icons/`, `src/main.rs`, `src/lib.rs`)
- Modify: `/Cargo.toml` (workspace members + default-members)
- Modify: `/.gitignore` (node_modules, build, target already ignored)

- [ ] **Step 1: Scaffold into a temp dir** (network required)

```bash
cd /tmp && rm -rf wc-scaffold
npm create tauri-app@latest wc-scaffold -- --template svelte --package-manager npm --yes
```
If `--yes` is not honored interactively, choose: Frontend language **JavaScript**, UI template **Svelte** (SvelteKit), and npm as the package manager.

- [ ] **Step 2: Copy the frontend + tauri skeleton into the repo** (do NOT copy the scaffold's root `Cargo.toml` or `.git`)

```bash
cd /tmp/wc-scaffold
REPO=/home/sprite/Dev/local-agent-loop
cp -r src static src-tauri "$REPO"/
cp package.json svelte.config.js vite.config.js jsconfig.json .npmrc "$REPO"/ 2>/dev/null || true
```
(Some scaffolds name the Vite config `vite.config.js`; copy whatever `vite.config.*` and `svelte.config.*` exist. Do not overwrite the repo's `Cargo.toml`.)

- [ ] **Step 3: Rewire the repo's root `/Cargo.toml`** so `src-tauri` joins the workspace but a bare `cargo build`/`cargo test` still excludes it (keeps the engine crates buildable without the Tauri system libs)

```toml
[workspace]
resolver = "2"
members = ["crates/*", "src-tauri"]
default-members = ["crates/*"]
```
(Leave the existing `[workspace.dependencies]` block unchanged.)

- [ ] **Step 4: Rewire `/src-tauri/Cargo.toml`** — set the package name to `app`, the lib name to `app_lib`, and add our path dependencies. Replace the `[package]` name and the `[dependencies]` section so it reads:

```toml
[package]
name = "app"
version = "0.0.0"
edition = "2021"
publish = false

[lib]
name = "app_lib"
crate-type = ["staticlib", "cdylib", "rlib"]

[build-dependencies]
tauri-build = { version = "2", features = [] }

[dependencies]
tauri      = { version = "2", features = [] }
serde      = { workspace = true }
serde_json = { workspace = true }
anyhow     = "1"
engine     = { path = "../crates/engine" }
persist    = { path = "../crates/persist" }
demo       = { path = "../crates/demo" }
workflow   = { path = "../crates/workflow" }
```
(Keep any `tauri-plugin-*` lines the scaffold added only if Step 6's `lib.rs` uses them; this plan's `lib.rs` uses none, so plugin deps may be removed.)

- [ ] **Step 5: Install frontend dependencies and the Tauri CLI** (network required)

```bash
cd /home/sprite/Dev/local-agent-loop
npm install
npm install --save-dev @tauri-apps/cli@^2 vitest
```

- [ ] **Step 6: Verify the engine crates still build without Tauri libs, and the frontend builds**

Run: `cargo build` (uses default-members → `crates/*` only; must NOT try to build `src-tauri`)
Expected: builds clean.
Run: `npm run build`
Expected: SvelteKit produces a static `build/` directory.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml src-tauri package.json package-lock.json svelte.config.js vite.config.js jsconfig.json .npmrc src static .gitignore
git commit -m "chore: scaffold Tauri + SvelteKit app; wire workspace (default-members excludes src-tauri)"
```

---

### Task 7: Configure SvelteKit for a static Tauri SPA

**Files:**
- Modify: `/svelte.config.js` (adapter-static)
- Create/Modify: `/src/routes/+layout.js` (ssr off, prerender on)
- Modify: `/vite.config.js` (fixed dev port for Tauri)

- [ ] **Step 1: Ensure `/svelte.config.js` uses `adapter-static`** (the Tauri svelte template usually already does; confirm/replace):

```js
import adapter from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  preprocess: vitePreprocess(),
  kit: {
    adapter: adapter({ fallback: 'index.html' })
  }
};

export default config;
```
If `@sveltejs/adapter-static` is not installed: `npm install --save-dev @sveltejs/adapter-static`.

- [ ] **Step 2: Write `/src/routes/+layout.js`**

```js
// Tauri serves a static bundle: no SSR, prerender the shell.
export const ssr = false;
export const prerender = true;
```

- [ ] **Step 3: Ensure `/vite.config.js` pins the dev server to port 1420** (Tauri's `devUrl`). Confirm it contains a `server` block like:

```js
import { sveltekit } from '@sveltejs/kit/vite';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [sveltekit()],
  clearScreen: false,
  server: {
    port: 1420,
    strictPort: true,
    host: '0.0.0.0',
    hmr: { protocol: 'ws', host: '0.0.0.0', port: 1421 },
    watch: { ignored: ['**/src-tauri/**'] }
  }
});
```

- [ ] **Step 4: Verify the static build**

Run: `npm run build`
Expected: `build/index.html` exists (SPA fallback present).

- [ ] **Step 5: Commit**

```bash
git add svelte.config.js src/routes/+layout.js vite.config.js
git commit -m "chore(frontend): static SPA config for Tauri (adapter-static, ssr off, port 1420)"
```

---

### Task 8: `applyCompletion` pure reducer (TDD with Vitest)

**Files:**
- Create: `/src/lib/applyCompletion.js`
- Create: `/src/lib/applyCompletion.test.js`
- Modify: `/package.json` (add `test` script)

- [ ] **Step 1: Add a `test` script to `/package.json`** under `"scripts"`:

```json
"test": "vitest run"
```

- [ ] **Step 2: Write the failing test** — `/src/lib/applyCompletion.test.js`:

```js
import { describe, it, expect } from 'vitest';
import { applyCompletion } from './applyCompletion.js';

/** @returns {import('./applyCompletion.js').Message[]} */
const pending = () => [{ id: 'wf-1', text: '1 2 3', status: 'pending' }];

describe('applyCompletion', () => {
  it('moves a pending message to done with the total', () => {
    const next = applyCompletion(pending(), {
      workflow_id: 'wf-1',
      run_id: 'r1',
      status: 'completed',
      result: { total: 6 }
    });
    expect(next).toEqual([{ id: 'wf-1', text: '1 2 3', status: 'done', output: 6 }]);
  });

  it('moves a pending message to error with the message', () => {
    const next = applyCompletion(pending(), {
      workflow_id: 'wf-1',
      run_id: 'r1',
      status: 'failed',
      result: { message: "could not parse 'two' as an integer" }
    });
    expect(next).toEqual([
      { id: 'wf-1', text: '1 2 3', status: 'error', error: "could not parse 'two' as an integer" }
    ]);
  });

  it('leaves non-matching messages untouched', () => {
    const before = pending();
    const next = applyCompletion(before, {
      workflow_id: 'other',
      run_id: 'r9',
      status: 'completed',
      result: { total: 42 }
    });
    expect(next).toEqual(before);
  });

  it('tolerates a null result on failure', () => {
    const next = applyCompletion(pending(), {
      workflow_id: 'wf-1',
      run_id: 'r1',
      status: 'failed',
      result: null
    });
    expect(next[0].status).toBe('error');
    expect(typeof next[0].error).toBe('string');
  });
});
```

- [ ] **Step 3: Run it to verify it fails**

Run: `npm test`
Expected: FAIL — `applyCompletion` is not defined.

- [ ] **Step 4: Write `/src/lib/applyCompletion.js`**

```js
/**
 * @typedef {Object} Message
 * @property {string} id        // == workflow_id, the correlation key
 * @property {string} text      // the submitted input
 * @property {'pending'|'done'|'error'} status
 * @property {number} [output]  // set when status === 'done' (from result.total)
 * @property {string} [error]   // set when status === 'error' (from result.message)
 */

/**
 * @typedef {Object} CompletionPayload
 * @property {string} workflow_id
 * @property {string} run_id
 * @property {'completed'|'failed'} status
 * @property {{ total?: number, message?: string } | null} result
 */

/**
 * Pure reducer: given the current messages and a `run_completed` event payload,
 * return the next messages array with the matching message resolved. The
 * frontend's unit-test seam (spec §6).
 *
 * @param {Message[]} messages
 * @param {CompletionPayload} payload
 * @returns {Message[]}
 */
export function applyCompletion(messages, payload) {
  return messages.map((m) => {
    if (m.id !== payload.workflow_id) return m;
    if (payload.status === 'completed') {
      return { id: m.id, text: m.text, status: 'done', output: payload.result?.total };
    }
    const error = payload.result?.message ?? 'workflow failed';
    return { id: m.id, text: m.text, status: 'error', error };
  });
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `npm test`
Expected: 4 tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/lib/applyCompletion.js src/lib/applyCompletion.test.js package.json
git commit -m "feat(frontend): applyCompletion reducer correlating run_completed to bubbles"
```

---

### Task 9: ChatView page (`+page.svelte`)

**Files:**
- Modify: `/src/routes/+page.svelte`

- [ ] **Step 1: Write `/src/routes/+page.svelte`** (Svelte 5 runes; invoke `submit`, listen for `run_completed`)

```svelte
<script>
  import { onMount } from 'svelte';
  import { invoke } from '@tauri-apps/api/core';
  import { listen } from '@tauri-apps/api/event';
  import { applyCompletion } from '$lib/applyCompletion.js';

  /** @type {import('$lib/applyCompletion.js').Message[]} */
  let messages = $state([]);
  let draft = $state('');

  onMount(() => {
    const unlisten = listen('run_completed', (event) => {
      messages = applyCompletion(messages, /** @type {any} */ (event.payload));
    });
    return () => {
      unlisten.then((off) => off());
    };
  });

  async function submit() {
    const text = draft.trim();
    if (text === '') return;
    const id = crypto.randomUUID();
    messages = [...messages, { id, text, status: 'pending' }];
    draft = '';
    try {
      await invoke('submit', { text, workflowId: id });
    } catch (e) {
      messages = messages.map((m) =>
        m.id === id ? { ...m, status: 'error', error: String(e) } : m
      );
    }
  }
</script>

<main>
  <h1>Workflow Chat</h1>

  <div class="transcript">
    {#each messages as m (m.id)}
      <div class="bubble user">{m.text}</div>
      {#if m.status === 'pending'}
        <div class="bubble reply pending">…</div>
      {:else if m.status === 'done'}
        <div class="bubble reply">{m.output}</div>
      {:else if m.status === 'error'}
        <div class="bubble reply error">{m.error}</div>
      {/if}
    {/each}
  </div>

  <form onsubmit={(e) => { e.preventDefault(); submit(); }}>
    <input placeholder="space-separated integers, e.g. 1 2 3" bind:value={draft} />
    <button type="submit">Send</button>
  </form>
</main>

<style>
  main { max-width: 40rem; margin: 0 auto; padding: 1rem; font-family: system-ui, sans-serif; }
  .transcript { display: flex; flex-direction: column; gap: 0.5rem; height: 60vh; overflow-y: auto; padding: 0.5rem; border: 1px solid #ddd; border-radius: 8px; }
  .bubble { padding: 0.4rem 0.7rem; border-radius: 12px; max-width: 75%; }
  .user { align-self: flex-end; background: #2563eb; color: white; }
  .reply { align-self: flex-start; background: #f1f1f1; }
  .reply.error { background: #fee2e2; color: #991b1b; }
  .reply.pending { opacity: 0.6; }
  form { display: flex; gap: 0.5rem; margin-top: 0.75rem; }
  input { flex: 1; padding: 0.5rem; }
  button { padding: 0.5rem 1rem; }
</style>
```

- [ ] **Step 2: Verify the frontend still builds and unit tests pass**

Run: `npm run build && npm test`
Expected: build succeeds; 4 Vitest tests pass. (The `@tauri-apps/api` imports resolve at build time even without a running Tauri host.)

- [ ] **Step 3: Commit**

```bash
git add src/routes/+page.svelte
git commit -m "feat(frontend): minimal chat UI (submit, pending/done/error bubbles)"
```

---

## Part C — `src-tauri` host wiring (compile + GUI need the Tauri prerequisites)

### Task 10: `submit` command + engine wiring in `src-tauri/src/lib.rs`

**Files:**
- Modify: `/src-tauri/src/lib.rs`
- Verify: `/src-tauri/src/main.rs` calls `app_lib::run()` (the scaffold's main; adjust if the lib name differs)
- Modify: `/src-tauri/capabilities/default.json` (ensure event permission)

- [ ] **Step 1: Replace `/src-tauri/src/lib.rs`** with the engine wiring:

```rust
use std::sync::Arc;

use engine::{Engine, ExecStatus, History, RunCompleted, StartOptions, TaskQueue};
use persist::Sqlite;
use serde::Serialize;
use tauri::{Emitter, Manager, State};

use demo::{Parent, ParentParams, Parse, SumActivity, SumChild};

/// Pushed to the frontend after a run reaches a terminal status (spec §7.3).
/// `result` is decoded as a generic JSON value so this host stays demo-agnostic:
/// on `completed` it is the workflow's result object (`{ "total": N }`); on
/// `failed` it is the `workflow::Error` object (`{ "message": "..." }`).
#[derive(Clone, Serialize)]
struct CompletionPayload {
    workflow_id: String,
    run_id: String,
    status: &'static str,
    result: Option<serde_json::Value>,
}

/// Start the parse→sum workflow for `text`, deduped by the frontend-supplied
/// `workflow_id` (spec §7.1). Returns the run_id.
#[tauri::command]
async fn submit(
    text: String,
    workflow_id: String,
    engine: State<'_, Arc<Engine>>,
) -> Result<String, String> {
    engine
        .start_workflow::<Parent>(ParentParams { text }, StartOptions { id: workflow_id })
        .await
        .map(|h| h.run_id().to_string())
        .map_err(|e| e.to_string())
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            // Durable store under the OS app-data dir.
            let dir = app.path().app_data_dir().expect("app data dir");
            std::fs::create_dir_all(&dir).ok();
            let db_path = dir.join("workflows.db");
            let db = Sqlite::open(db_path.to_str().expect("utf-8 db path")).expect("open db");

            let history: Arc<dyn History> = Arc::new(db.clone());
            let queue: Arc<dyn TaskQueue> = Arc::new(db.clone());
            let mut engine = Engine::new(history, queue);
            engine.register_workflow::<Parent>();
            engine.register_workflow::<SumChild>();
            engine.register_activity::<Parse>();
            engine.register_activity::<SumActivity>();

            // Push terminal completions to the frontend (spec §7.3).
            let app_handle = app.handle().clone();
            engine.on_run_completed(move |ev: RunCompleted| {
                let payload = CompletionPayload {
                    workflow_id: ev.workflow_id,
                    run_id: ev.run_id,
                    status: if matches!(ev.status, ExecStatus::Completed) {
                        "completed"
                    } else {
                        "failed"
                    },
                    result: ev
                        .result
                        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok()),
                };
                let _ = app_handle.emit("run_completed", payload);
            });

            // `Engine::start` uses `tokio::spawn`, so a tokio runtime must be the
            // current runtime when we call it. Build one for the engine's
            // background loops and keep it alive for the app's lifetime.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let engine = {
                let _guard = rt.enter();
                engine.start() // spawns driver/worker/timer/sweeper loops on `rt`
            };
            app.manage(rt); // keep the runtime (and its threads) alive
            app.manage(engine);
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![submit])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
```

Note: `tokio` must be a dependency of `src-tauri`. Add to `/src-tauri/Cargo.toml` `[dependencies]`:
```toml
tokio = { workspace = true }
```

- [ ] **Step 2: Confirm `/src-tauri/src/main.rs`** matches the lib name `app_lib`:

```rust
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    app_lib::run();
}
```

- [ ] **Step 3: Ensure the frontend may listen for events** — `/src-tauri/capabilities/default.json` `permissions` must include event access. It should contain at least:

```json
{
  "$schema": "../gen/schemas/desktop-schema.json",
  "identifier": "default",
  "description": "Default capability for the main window",
  "windows": ["main"],
  "permissions": ["core:default", "core:event:default"]
}
```

- [ ] **Step 4: Compile the host crate** (REQUIRES the Tauri system libraries from Prerequisites)

Run: `cargo build -p app`
Expected: compiles. If it fails with `pkg-config`/`webkit2gtk` errors, the system libraries are not installed — see Prerequisites; this step is environment-gated.

- [ ] **Step 5: Commit**

```bash
git add src-tauri/src/lib.rs src-tauri/src/main.rs src-tauri/Cargo.toml src-tauri/capabilities/default.json
git commit -m "feat(app): submit command + engine wiring with pushed completions"
```

---

### Task 11: End-to-end GUI smoke test

**Files:** none (manual verification; capture fixes as commits if needed)

This task REQUIRES the Tauri prerequisites (system libs + CLI). It is environment-gated.

- [ ] **Step 1: Launch the app in dev mode**

Run: `npm run tauri dev`
Expected: the Vite dev server starts on :1420, the Rust host compiles, and a window titled "Workflow Chat" opens.

- [ ] **Step 2: Happy path** — type `1 2 3`, press Send.
Expected: a user bubble `1 2 3`, then a brief pending `…`, then a reply bubble `6`.

- [ ] **Step 3: Larger / negative inputs** — `10 20 30` → `60`; `-5 10` → `5`.

- [ ] **Step 4: Parse failure** — type `1 two 3`, press Send.
Expected: an error reply bubble containing `could not parse 'two' as an integer`.

- [ ] **Step 5: Durability sanity** — submit `2 2`, see `4`; close and relaunch (`npm run tauri dev`).
Expected: the app reopens cleanly with an empty transcript (in-memory v1, spec §2) and new submissions still work; the prior run remains in `workflows.db`.

- [ ] **Step 6: Commit any fixes** discovered during the smoke test (e.g. capability/permission tweaks):

```bash
git add -A
git commit -m "fix(app): smoke-test adjustments"
```

---

## Notes / Out of scope (per spec §9)

- **Workflow history view** (`/history` route + `engine.list_workflows`/`read_history`) is a separate follow-up plan.
- Chat-transcript persistence across restarts, signals/timers/cancellation in the UI, visual polish, and i64-overflow handling are all deferred.
- No engine-crate changes are made by this plan; `start_workflow` + the completion observer already exist and are tested.
