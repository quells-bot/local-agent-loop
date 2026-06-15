// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::time::Duration;

use crate::future::ActivityFuture;
use crate::{Command, CommandResult, Info, RetryPolicy};

/// Shared, single-threaded replay state. `Context` is a cheap handle to this.
pub(crate) struct ContextInner {
    pub(crate) info: Info,
    pub(crate) next_seq: Cell<u64>,
    pub(crate) results: RefCell<HashMap<u64, CommandResult>>, // seq -> recorded outcome
    pub(crate) scheduled: RefCell<HashSet<u64>>,              // seqs emitted this life
    pub(crate) commands: RefCell<Vec<Command>>,               // emitted this turn
    pub(crate) fired: RefCell<HashSet<u64>>,                  // timer seqs fired (no payload)
    // Futures spawned this turn, awaiting absorption by WorkflowState (spec §4.4).
    pub(crate) new_spawns: RefCell<Vec<Pin<Box<dyn Future<Output = ()>>>>>,
    // Inbound signal payloads, buffered per name (spec §6.2). Rebuilt identically on
    // every replay because history replays in event_id order (Invariant 10).
    pub(crate) signals: RefCell<HashMap<String, VecDeque<Vec<u8>>>>,
    // Child workflow outcomes, keyed by the parent's StartChild command `seq` (spec
    // §5.4). Resolves `ChildFuture` exactly like `results` resolves `ActivityFuture`.
    pub(crate) child_results: RefCell<HashMap<u64, Result<Vec<u8>, crate::Error>>>,
    // Change-version markers recorded in history (spec §14), seeded before driving so
    // `patched` is replay-stable. A present id means the patched path was taken.
    pub(crate) patches: RefCell<HashSet<String>>,
    // Markers emitted this life, so `patched` requests `RecordPatch` at most once.
    pub(crate) patches_emitted: RefCell<HashSet<String>>,
    // Frontier flag: true while recorded one-per-turn events still remain ahead of the
    // current replay position. The replay driver sets it each turn; `patched` reads it
    // to tell "replaying old history" (=> false) from "live, first run" (=> record).
    pub(crate) replaying: Cell<bool>,
}

#[derive(Clone)]
pub struct Context {
    inner: Rc<ContextInner>,
}

impl Context {
    pub fn new(info: Info) -> Self {
        Self {
            inner: Rc::new(ContextInner {
                info,
                next_seq: Cell::new(0),
                results: RefCell::new(HashMap::new()),
                scheduled: RefCell::new(HashSet::new()),
                commands: RefCell::new(Vec::new()),
                fired: RefCell::new(HashSet::new()),
                new_spawns: RefCell::new(Vec::new()),
                signals: RefCell::new(HashMap::new()),
                child_results: RefCell::new(HashMap::new()),
                patches: RefCell::new(HashSet::new()),
                patches_emitted: RefCell::new(HashSet::new()),
                replaying: Cell::new(false), // default: live frontier (used by unit tests)
            }),
        }
    }

    pub fn info(&self) -> &Info {
        &self.inner.info
    }

    /// Schedule an activity. `seq` is allocated HERE (creation time, spec §3/Inv 2).
    pub fn activity<A: activity::Definition>(&self, input: A::Input) -> ActivityFuture<A> {
        let seq = self.inner.next_seq.get();
        self.inner.next_seq.set(seq + 1);
        let bytes = serde_json::to_vec(&input).expect("activity input serializes");
        ActivityFuture::new(self.inner.clone(), seq, bytes, RetryPolicy::none())
    }

    /// Start a timer. `seq` is allocated HERE (creation time, Invariant 2). The
    /// duration is the deterministic, replay-checked datum; the engine converts it
    /// to an absolute fire time when it commits the TimerStarted event (spec §5.3).
    pub fn timer(&self, dur: Duration) -> crate::future::TimerFuture {
        let seq = self.inner.next_seq.get();
        self.inner.next_seq.set(seq + 1);
        crate::future::TimerFuture::new(self.inner.clone(), seq, dur.as_millis() as u64)
    }

    /// `workflow.Sleep` analog — await a timer for `dur`.
    pub fn sleep(&self, dur: Duration) -> crate::future::TimerFuture {
        self.timer(dur)
    }

    /// Driver/replay applies a recorded TimerFired before a poll (one event/turn).
    pub fn apply_timer_fired(&self, seq: u64) {
        self.inner.fired.borrow_mut().insert(seq);
    }

    /// Driver applies exactly one recorded outcome before each poll (spec §4.1).
    pub fn apply_result(&self, seq: u64, result: CommandResult) {
        self.inner.results.borrow_mut().insert(seq, result);
    }

    /// Start a child workflow (the `workflow.ExecuteChildWorkflow` analog, spec §9).
    /// `seq` is allocated HERE (creation time, spec §3/Inv 2). The returned future
    /// emits `StartChild` once and resolves to the child's typed output (or error).
    pub fn child_workflow<W: crate::Definition>(
        &self,
        input: W::Input,
    ) -> crate::future::ChildFuture<W> {
        let seq = self.inner.next_seq.get();
        self.inner.next_seq.set(seq + 1);
        let bytes = serde_json::to_vec(&input).expect("child input serializes");
        crate::future::ChildFuture::new(self.inner.clone(), seq, bytes)
    }

    /// Driver/replay applies one recorded child outcome before a poll (one event per
    /// turn, spec §4.1/§5.4): record it so the next poll resolves the `ChildFuture`.
    pub fn apply_child_result(&self, seq: u64, result: Result<Vec<u8>, crate::Error>) {
        self.inner.child_results.borrow_mut().insert(seq, result);
    }

    /// Driver drains commands emitted during the poll it just ran.
    pub fn drain_commands(&self) -> Vec<Command> {
        self.inner.commands.borrow_mut().drain(..).collect()
    }

    /// Spawn a detached branch (the `workflow.Go` analog, spec §4.4). The branch is
    /// polled every turn in creation order by `WorkflowState`; it allocates `seq`s
    /// from the shared counter exactly like inline code, so replay is deterministic.
    /// Returns an awaitable handle for its output. Allocates no command and no `seq`
    /// for the spawn itself.
    pub fn spawn<F, T>(&self, fut: F) -> crate::SpawnHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let slot = Rc::new(RefCell::new(None));
        let writer = slot.clone();
        let wrapped: Pin<Box<dyn Future<Output = ()>>> = Box::pin(async move {
            let v = fut.await;
            *writer.borrow_mut() = Some(v);
        });
        self.inner.new_spawns.borrow_mut().push(wrapped);
        crate::SpawnHandle { slot }
    }

    /// Get the signal channel for `name` (the `workflow.GetSignalChannel` analog,
    /// spec §6.3). Idempotent by name: every call returns a handle onto the same
    /// per-name buffer. Allocates no command and consumes no `seq`.
    pub fn signal_channel<T>(&self, name: &str) -> crate::SignalChannel<T> {
        crate::SignalChannel::new(self.inner.clone(), name.to_string())
    }

    /// `workflow.GetVersion`/`Patched` analog (spec §9.1, §14). Returns whether this
    /// run is on the patched code path for `change_id`, recording a marker the first
    /// time new code reaches it live. Synchronous (does not block); emits at most one
    /// `RecordPatch` per `change_id` per life. Allocates NO `seq`.
    pub fn patched(&self, change_id: &str) -> bool {
        // 1. Marker already recorded in history -> patched path, replay-stable.
        if self.inner.patches.borrow().contains(change_id) {
            return true;
        }
        // 2. No marker but recorded history still remains ahead -> old code wrote this
        //    history; take the OLD branch so it re-emits what history recorded.
        if self.inner.replaying.get() {
            return false;
        }
        // 3. Caught up to the live frontier, first time here -> record the marker once.
        if self
            .inner
            .patches_emitted
            .borrow_mut()
            .insert(change_id.to_string())
        {
            self.inner.commands.borrow_mut().push(Command::RecordPatch {
                change_id: change_id.to_string(),
            });
        }
        true
    }

    /// Driver/replay seeds a recorded change-version marker before driving (spec §14).
    /// Markers carry no `seq` and resolve synchronously, so — unlike one-per-turn
    /// completions — they are seeded up front, like recorded schedules.
    pub fn apply_patch(&self, change_id: String) {
        self.inner.patches.borrow_mut().insert(change_id);
    }

    /// Replay driver sets the frontier flag before each poll: true while recorded
    /// one-per-turn events remain ahead of the current position (spec §14).
    pub fn set_replaying(&self, replaying: bool) {
        self.inner.replaying.set(replaying);
    }

    /// Driver/replay applies one recorded inbound signal before a poll (one event per
    /// turn, spec §4.1/§6.2): push its payload onto the per-name buffer.
    pub fn apply_signal(&self, name: String, payload: Vec<u8>) {
        self.inner
            .signals
            .borrow_mut()
            .entry(name)
            .or_default()
            .push_back(payload);
    }

    /// WorkflowState drains freshly-spawned futures into its ordered poll list.
    pub(crate) fn drain_new_spawns(&self) -> Vec<Pin<Box<dyn Future<Output = ()>>>> {
        self.inner.new_spawns.borrow_mut().drain(..).collect()
    }

    /// Number of commands buffered (not drained). Used by `poll_turn` to detect that
    /// a future made progress (emitted a command) during a quiescence iteration.
    pub fn commands_len(&self) -> usize {
        self.inner.commands.borrow().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::Execution;

    fn info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "T".into(),
        }
    }

    #[test]
    fn apply_then_drain_are_independent() {
        let ctx = Context::new(info());
        ctx.apply_result(0, CommandResult::ActivityCompleted(b"x".to_vec()));
        // applying a result does not by itself emit a command
        assert!(ctx.drain_commands().is_empty());
        assert_eq!(ctx.info().execution.run_id, "r");
    }

    #[test]
    fn patched_new_execution_records_marker_and_returns_true() {
        let ctx = Context::new(info());
        // Fresh run, caught up to the live frontier (replaying defaults to false).
        assert!(ctx.patched("v2"), "new execution takes the patched path");
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(matches!(
            &cmds[0],
            Command::RecordPatch { change_id } if change_id == "v2"
        ));
        // Second call in the same life is idempotent: still true, no second command.
        assert!(ctx.patched("v2"));
        assert!(
            ctx.drain_commands().is_empty(),
            "marker is emitted at most once per life"
        );
    }

    #[test]
    fn patched_with_recorded_marker_returns_true_without_emitting() {
        let ctx = Context::new(info());
        ctx.apply_patch("v2".into()); // seeded from a recorded Event::Patched
        assert!(ctx.patched("v2"));
        assert!(
            ctx.drain_commands().is_empty(),
            "a recorded marker re-emits nothing on replay"
        );
    }

    #[test]
    fn patched_returns_false_while_replaying_older_history() {
        let ctx = Context::new(info());
        ctx.set_replaying(true); // recorded events still remain ahead of this point
        assert!(
            !ctx.patched("v2"),
            "no marker + still replaying older history => old branch"
        );
        assert!(
            ctx.drain_commands().is_empty(),
            "the old branch records no marker"
        );
    }
}
