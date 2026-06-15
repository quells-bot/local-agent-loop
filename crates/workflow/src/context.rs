use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
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
}
