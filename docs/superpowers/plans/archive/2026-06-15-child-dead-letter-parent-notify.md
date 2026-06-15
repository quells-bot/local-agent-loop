# Child dead-letter → parent notify Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the Pass-4a durability hole where a child workflow that *dead-letters* (unregistered type or replay divergence) terminates `Failed` but never notifies its parent, leaving the parent's `ChildFuture` parked forever and the parent run stuck `Running` indefinitely.

**Architecture:** The fix lives entirely in `engine::Engine::dead_letter`. Both dead-letter call sites already have the run's `RunMeta` in scope (with `parent_run_id` / `parent_seq`), so we thread the parent linkage into `dead_letter` and, when the dead-lettered run is a child, emit a `ParentNotify { ChildCompleted { result: ChildResult::Failed(..) } }` inside the *same* `commit_turn` transaction — exactly mirroring the terminal-failure notify already built in `process_one_runnable` (spec §5.4, Invariant 5). No schema, type, or trait changes: `dead_letter` already builds a `TurnCommit`, which already carries `parent_notify`.

**Tech Stack:** Rust 2021, tokio, rusqlite, serde_json, anyhow — existing `engine` / `workflow` / `persist` crates. No new dependencies.

**Spec/context:** Builds on `docs/superpowers/plans/2026-06-14-pass-4a-child-workflows.md` (Pass 4a). Relevant code: `crates/engine/src/engine.rs` — `process_one_runnable` (the working terminal-notify at lines ~374-392), the two `dead_letter` call sites (~276-283 unregistered, ~289-296 divergence), and `dead_letter` itself (~424-451). The acceptance-test harness is `crates/engine/tests/children.rs`.

**Why NOT the early-return terminal path:** `process_one_runnable`'s already-terminal early-return (engine.rs:217-238) deliberately keeps `parent_notify: None`. That path only fires for a run that is *already* terminal and got re-marked runnable; its parent was already notified during the original terminal commit, so notifying again would double-fire. **Leave it as `None` — do not touch it.** `dead_letter` is different: it is the run's *first and only* terminal commit (it is gated by `meta.status == Running`), so notifying there is correct and cannot double-fire.

---

## File Structure

| File | Responsibility | Change |
| --- | --- | --- |
| `crates/engine/tests/children.rs` | Pass-4 e2e acceptance tests | ADD one test: a child with an unregistered type dead-letters and fails its parent |
| `crates/engine/src/engine.rs` | Driver: dead-letter + child-id derivation | MODIFY `dead_letter` signature + body; update its 2 call sites; add a continue-as-new caveat comment at the `child_workflow_id` derivation |

---

### Task 1: Failing acceptance test — a dead-lettered child fails its parent

**Files:**
- Modify: `crates/engine/tests/children.rs`

- [ ] **Step 1: Add the failing test**

The harness already defines `Parent` (starts `ctx.child_workflow::<Child>(n)`, returns `child + 1`), `build`, and `pump`. This test builds a *bespoke* engine that registers `Parent` but **deliberately omits `Child`**, so the child run hits the unregistered-type dead-letter path. Add this test to the end of `crates/engine/tests/children.rs`:

```rust
#[tokio::test]
async fn dead_lettered_child_fails_parent() {
    // Build an engine that knows Parent but NOT Child, so the child run dead-letters
    // on "unregistered workflow Child". (Cannot use `build`, which registers Child.)
    let db = Sqlite::open_in_memory().unwrap();
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut engine = Engine::new(h, q);
    engine.register_workflow::<Parent>();
    // NOTE: Child is intentionally not registered.

    engine
        .start_workflow::<Parent>(5, StartOptions { id: "p-dead".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    // The child dead-lettered (unregistered type) and, in the SAME transaction, notified
    // its parent — so the parent's ChildFuture resolved to Failed and the parent failed
    // too, instead of parking forever.
    let (_, child_status, _) = db.find_execution("p-dead::child::0").await.unwrap().unwrap();
    assert_eq!(child_status, ExecStatus::Failed, "child dead-letters to Failed");
    let (_, parent_status, _) = db.find_execution("p-dead").await.unwrap().unwrap();
    assert_eq!(
        parent_status,
        ExecStatus::Failed,
        "parent must observe the child's failure, not hang Running"
    );
}
```

- [ ] **Step 2: Run the test — verify it FAILS for the right reason**

Run: `cargo test -p engine --test children dead_lettered_child_fails_parent`
Expected: FAIL on the **second** assertion — `parent_status` is `Running`, not `Failed`. (The child correctly dead-letters to `Failed`, so the first assertion passes; the parent is never notified, so `pump` goes quiescent with the parent stuck `Running`.) This proves the bug exists and the test detects it.

- [ ] **Step 3: Commit the failing test**

```bash
git add crates/engine/tests/children.rs
git commit -m "test(engine): dead-lettered child must fail its parent (currently RED)"
```

---

### Task 2: Thread parent linkage into `dead_letter` and notify the parent

**Files:**
- Modify: `crates/engine/src/engine.rs` (`dead_letter` ~424-451; call sites ~276-283 and ~289-296)

- [ ] **Step 1: Replace the `dead_letter` method**

Replace the entire `dead_letter` method (engine.rs:424-451) with this version — it gains two parameters (`parent_run_id`, `parent_seq`) and builds `parent_notify`:

```rust
    /// Terminally fail a run that cannot make progress (unregistered type, replay
    /// divergence). Commits a `Failed` turn — which clears `runnable`, so the driver
    /// stops retrying — and fires the completion observer (spec §5.1, §14). Returns
    /// `Ok(true)` so the caller's loop continues without the error backoff.
    ///
    /// If the dead-lettered run is a child (`parent_run_id`/`parent_seq` set), it
    /// notifies the parent with `ChildResult::Failed` in the SAME transaction (spec
    /// §5.4): a dead-letter is terminal, so the parent's `ChildFuture` must resolve to
    /// Failed rather than park forever. This mirrors the terminal-failure notify in
    /// `process_one_runnable`. Safe from double-firing because `dead_letter` only runs
    /// for a `Running` run (its first and only terminal commit).
    async fn dead_letter(
        &self,
        run_id: &str,
        workflow_id: &str,
        parent_run_id: Option<&str>,
        parent_seq: Option<i64>,
        message: String,
    ) -> anyhow::Result<bool> {
        let err = workflow::Error::new(message);
        let result = Some(serde_json::to_vec(&err)?);
        let parent_notify = match (parent_run_id, parent_seq) {
            (Some(prid), Some(pseq)) => Some(ParentNotify {
                parent_run_id: prid.to_string(),
                event: workflow::Event::ChildCompleted {
                    seq: pseq as u64,
                    result: workflow::ChildResult::Failed(err.clone()),
                },
            }),
            _ => None,
        };
        let commit = TurnCommit {
            events: Vec::new(),
            new_tasks: Vec::new(),
            new_timers: Vec::new(),
            new_children: Vec::new(),
            parent_notify,
            status: ExecStatus::Failed,
            result: result.clone(),
        };
        self.history.commit_turn(run_id, &commit).await?;
        if let Some(obs) = &self.observer {
            obs(RunCompleted {
                run_id: run_id.to_string(),
                workflow_id: workflow_id.to_string(),
                status: ExecStatus::Failed,
                result,
            });
        }
        Ok(true)
    }
```

- [ ] **Step 2: Update the unregistered-type call site**

In `process_one_runnable` (engine.rs ~276-283), replace the `dead_letter` call in the `None => { ... }` arm of `match self.workflows.get(&meta.workflow_type)` with:

```rust
            None => {
                return self
                    .dead_letter(
                        &run_id,
                        &meta.workflow_id,
                        meta.parent_run_id.as_deref(),
                        meta.parent_seq,
                        format!("unregistered workflow {}", meta.workflow_type),
                    )
                    .await;
            }
```

- [ ] **Step 3: Update the divergence call site**

In `process_one_runnable` (engine.rs ~289-296), replace the `dead_letter` call in the `Err(e) => { ... }` arm of `match replay(info, &events)` with:

```rust
            Err(e) => {
                return self
                    .dead_letter(
                        &run_id,
                        &meta.workflow_id,
                        meta.parent_run_id.as_deref(),
                        meta.parent_seq,
                        format!("nondeterminism in {}: {e}", meta.workflow_type),
                    )
                    .await;
            }
```

(Both call sites borrow `meta` immutably — `&meta.workflow_id`, `meta.parent_run_id.as_deref()` — and copy `meta.parent_seq` (an `Option<i64>`), so they compose with no borrow conflict.)

- [ ] **Step 4: Run the Task-1 test — verify it now PASSES**

Run: `cargo test -p engine --test children dead_lettered_child_fails_parent`
Expected: PASS — the child dead-letters to `Failed` and the parent now observes `ChildResult::Failed` via its `?` and fails too.

- [ ] **Step 5: Run the full children suite — verify no regression**

Run: `cargo test -p engine --test children`
Expected: all 5 PASS — the original four (`parent_completes_when_child_does`, `child_info_parent_is_populated`, `child_failure_propagates_to_parent`, `cold_recovery_completes_parent_and_child`) plus the new `dead_lettered_child_fails_parent`.

- [ ] **Step 6: Commit**

```bash
git add crates/engine/src/engine.rs
git commit -m "fix(engine): dead-lettered child notifies parent so it fails instead of hanging"
```

---

### Task 3: Document the child-id reuse caveat for continue-as-new

**Files:**
- Modify: `crates/engine/src/engine.rs` (the `StartChild` arm, at the `child_workflow_id` derivation ~360)

This is a documentation-only change: the derived `child_workflow_id` is collision-free **today** but will collide once continue-as-new (or any workflow-id reuse policy) lets a parent keep its `workflow_id` while `seq` resets across runs. Record that assumption at the site so the next person doesn't have to re-derive it.

- [ ] **Step 1: Add the caveat comment**

In `process_one_runnable`, inside the `workflow::Command::StartChild { .. }` arm, immediately **before** the `new_children.push(NewChild {` line (engine.rs ~357), insert this comment:

```rust
                    // `child_workflow_id` is derived from the parent's workflow_id + the
                    // StartChild command seq. This is collision-free TODAY because
                    // `executions` enforces UNIQUE(workflow_id) (so a parent id is never
                    // reused) and StartChild is emit-once per seq — the same
                    // {parent_workflow_id, seq} pair never recurs, so the plain INSERT in
                    // `commit_turn` is safe. WARNING: once continue-as-new (or a
                    // workflow-id reuse policy) lands, a parent keeps its workflow_id while
                    // seq resets to 0 across runs, so this derivation WILL collide and the
                    // plain INSERT will abort the turn. Re-scope the id then (e.g. fold in
                    // the parent's run_id).
```

- [ ] **Step 2: Verify it compiles (comment-only, but confirm no stray edit)**

Run: `cargo build -p engine`
Expected: compiles clean.

- [ ] **Step 3: Commit**

```bash
git add crates/engine/src/engine.rs
git commit -m "docs(engine): flag child_workflow_id collision risk under continue-as-new"
```

---

### Task 4: Full verification gate

The roadmap's gate (every chunk) requires all three to pass.

- [ ] **Step 1:** Run `cargo test` — every crate's tests PASS (workspace green; the change is additive + one fixed behavior).
- [ ] **Step 2:** Run `cargo clippy --all-targets -- -D warnings` — clean. (No new imports were added — `ParentNotify`, `TurnCommit`, `workflow::Event`, `workflow::ChildResult` are all already in scope in `engine.rs`.)
- [ ] **Step 3:** Run `cargo fmt --all -- --check` — no drift. (Run `cargo fmt --all` first if it reports changes, then re-check.)
- [ ] **Step 4:** Commit any fmt drift if Step 3 required it:

```bash
git add -A
git commit -m "chore: fmt"
```

---

## Notes

- **One test covers both dead-letter paths.** The fix is inside `dead_letter`, and both call sites (unregistered type, replay divergence) funnel through it identically, passing the same `meta.parent_run_id` / `meta.parent_seq`. The unregistered-type test is the cheapest to construct (just omit a registration); a divergence test would add no coverage of the fix itself, so per YAGNI we don't add one.
- **No spec change needed.** Spec §5.4 says a child's *terminal status* writes `ChildCompleted` into the parent's history. A dead-lettered child is terminal (`Failed`), so notifying the parent is the spec-conformant behavior — this fix brings the dead-letter path in line with §5.4, it doesn't extend the spec.
- **Atomicity preserved (Invariant 5).** The `ParentNotify` rides inside the same `commit_turn` `TurnCommit` as the child's `Failed` status update, so the child's termination and the parent's notification commit or roll back together — a crash can't fail the child without arming the parent, or vice versa.

## Self-Review (completed during authoring)

- **Spec coverage:** §5.4 (terminal child → `ChildCompleted` into parent's history, parent re-marked runnable) is now honored on the dead-letter path, not just the normal-completion path — Task 2. Invariant 5 (atomic turn) preserved by reusing the single `commit_turn` — Task 2 Step 1. The continue-as-new id-reuse caveat (out of scope for Pass 4a, reserved for Pass 5+) is documented — Task 3.
- **Placeholder scan:** none — every step shows the full code or exact command + expected output.
- **Type consistency:** `dead_letter(&self, run_id: &str, workflow_id: &str, parent_run_id: Option<&str>, parent_seq: Option<i64>, message: String)` is used identically at both call sites; `ParentNotify { parent_run_id: String, event: Event }` and `workflow::Event::ChildCompleted { seq: u64, result: workflow::ChildResult }` with `ChildResult::Failed(workflow::Error)` match the canonical types in the ROADMAP and the existing terminal-notify in `process_one_runnable`. `find_execution` returns `(String, ExecStatus, Option<Vec<u8>>)`, matching the test's destructuring `let (_, status, _) = ...`.
