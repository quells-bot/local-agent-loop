// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context as TaskContext, Poll};

use crate::context::ContextInner;
use crate::{Command, RetryPolicy};

/// Awaitable handle for one activity call. Resolves to the activity's typed
/// Output (deserialized) or its Error. `seq` identifies it in history (spec §3).
pub struct ActivityFuture<A: activity::Definition> {
    inner: Rc<ContextInner>,
    seq: u64,
    input: Vec<u8>,
    retry: RetryPolicy,
    // `fn() -> A` keeps the future `Unpin` and `Send`-agnostic regardless of `A`;
    // `A` is only a type tag here, never stored.
    _marker: PhantomData<fn() -> A>,
}

impl<A: activity::Definition> ActivityFuture<A> {
    pub(crate) fn new(
        inner: Rc<ContextInner>,
        seq: u64,
        input: Vec<u8>,
        retry: RetryPolicy,
    ) -> Self {
        Self {
            inner,
            seq,
            input,
            retry,
            _marker: PhantomData,
        }
    }

    /// Builder: attach a retry policy (spec §7). Default is single-attempt.
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = policy;
        self
    }
}

impl<A: activity::Definition> Future for ActivityFuture<A> {
    type Output = Result<A::Output, activity::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();

        // 1. Replay path: outcome already recorded -> resolve immediately.
        let recorded = me.inner.results.borrow().get(&me.seq).cloned();
        if let Some(recorded) = recorded {
            let bytes: Result<Vec<u8>, activity::Error> = recorded.into();
            return Poll::Ready(bytes.and_then(|b| {
                serde_json::from_slice::<A::Output>(&b).map_err(|e| {
                    activity::Error::fatal(format!("activity output deserialize: {e}"))
                })
            }));
        }

        // 2. First arrival: emit the command exactly once, then park (spec §3/Inv 4).
        if me.inner.scheduled.borrow_mut().insert(me.seq) {
            me.inner
                .commands
                .borrow_mut()
                .push(Command::ScheduleActivity {
                    seq: me.seq,
                    activity_type: A::TYPE.to_string(),
                    input: me.input.clone(),
                    retry: me.retry.clone(),
                });
        }
        Poll::Pending
    }
}

/// Awaitable handle for one timer. Resolves to `()` once its TimerFired event has
/// been applied. `seq` identifies it in history; the shared `scheduled` set means
/// it emits `StartTimer` exactly once across re-polls (spec §3, §5.3).
pub struct TimerFuture {
    inner: Rc<ContextInner>,
    seq: u64,
    duration_ms: u64,
}

impl TimerFuture {
    pub(crate) fn new(inner: Rc<ContextInner>, seq: u64, duration_ms: u64) -> Self {
        Self {
            inner,
            seq,
            duration_ms,
        }
    }
}

impl Future for TimerFuture {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<()> {
        let me = self.get_mut();

        // 1. Replay path: this timer's TimerFired has been applied -> resolve.
        if me.inner.fired.borrow().contains(&me.seq) {
            return Poll::Ready(());
        }

        // 2. First arrival: emit StartTimer exactly once, then park (Invariant 4).
        if me.inner.scheduled.borrow_mut().insert(me.seq) {
            me.inner.commands.borrow_mut().push(Command::StartTimer {
                seq: me.seq,
                duration_ms: me.duration_ms,
            });
        }
        Poll::Pending
    }
}

/// Awaitable handle for one child workflow (the `ExecuteChildWorkflow` analog, spec
/// §5.4, §9). Resolves to the child's typed Output or a `workflow::Error`. `seq`
/// identifies it in the parent's history; the shared `scheduled` set means it emits
/// `StartChild` exactly once across re-polls (Invariant 4).
pub struct ChildFuture<W: crate::Definition> {
    inner: Rc<ContextInner>,
    seq: u64,
    input: Vec<u8>,
    // `fn() -> W` keeps the future `Unpin` and `Send`-agnostic regardless of `W`;
    // `W` is only a type tag here, never stored.
    _marker: PhantomData<fn() -> W>,
}

impl<W: crate::Definition> ChildFuture<W> {
    pub(crate) fn new(inner: Rc<ContextInner>, seq: u64, input: Vec<u8>) -> Self {
        Self {
            inner,
            seq,
            input,
            _marker: PhantomData,
        }
    }
}

impl<W: crate::Definition> Future for ChildFuture<W> {
    type Output = Result<W::Output, crate::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();

        // 1. Replay path: child outcome already recorded -> resolve immediately.
        let recorded = me.inner.child_results.borrow().get(&me.seq).cloned();
        if let Some(recorded) = recorded {
            return Poll::Ready(recorded.and_then(|b| {
                serde_json::from_slice::<W::Output>(&b)
                    .map_err(|e| crate::Error::new(format!("child output deserialize: {e}")))
            }));
        }

        // 2. First arrival: emit StartChild exactly once, then park (Invariant 4).
        if me.inner.scheduled.borrow_mut().insert(me.seq) {
            me.inner.commands.borrow_mut().push(Command::StartChild {
                seq: me.seq,
                workflow_type: W::TYPE.to_string(),
                input: me.input.clone(),
            });
        }
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, CommandResult, Context, Info};
    use activity::{Definition, Error, Execution};

    struct Add;
    #[async_trait::async_trait]
    impl Definition for Add {
        type Input = (i64, i64);
        type Output = i64;
        const TYPE: &'static str = "Add";
        async fn run(_c: activity::Context, i: (i64, i64)) -> Result<i64, Error> {
            Ok(i.0 + i.1)
        }
    }

    fn ctx() -> Context {
        Context::new(Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "T".into(),
        })
    }

    fn poll<A: Definition>(f: &mut ActivityFuture<A>) -> Poll<Result<A::Output, Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        // ActivityFuture is Unpin (no self-referential fields).
        Pin::new(f).poll(&mut tcx)
    }

    #[test]
    fn first_poll_emits_one_schedule_command_then_pends() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(matches!(&cmds[0],
            Command::ScheduleActivity { seq: 0, activity_type, .. } if activity_type == "Add"));
    }

    #[test]
    fn re_poll_does_not_duplicate_the_command() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        let _ = ctx.drain_commands();
        assert!(poll(&mut f).is_pending());
        assert!(
            ctx.drain_commands().is_empty(),
            "in-flight seq must not re-emit"
        );
    }

    #[test]
    fn resolves_to_typed_output_when_result_applied() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        ctx.apply_result(
            0,
            CommandResult::ActivityCompleted(serde_json::to_vec(&5i64).unwrap()),
        );
        match poll(&mut f) {
            Poll::Ready(Ok(v)) => assert_eq!(v, 5),
            other => panic!("expected Ready(Ok(5)), got {other:?}"),
        }
    }

    #[test]
    fn surfaces_activity_failure() {
        let ctx = ctx();
        let mut f = ctx.activity::<Add>((2, 3));
        assert!(poll(&mut f).is_pending());
        ctx.apply_result(0, CommandResult::ActivityFailed(Error::fatal("nope")));
        match poll(&mut f) {
            Poll::Ready(Err(e)) => assert_eq!(e.message, "nope"),
            other => panic!("expected Ready(Err), got {other:?}"),
        }
    }

    // === ChildFuture tests ===

    struct Echo;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Echo {
        type Input = i64;
        type Output = i64;
        const TYPE: &'static str = "Echo";
        async fn run(_c: Context, i: i64) -> Result<i64, crate::Error> {
            Ok(i)
        }
    }

    fn poll_child<W: crate::Definition>(
        f: &mut crate::future::ChildFuture<W>,
    ) -> Poll<Result<W::Output, crate::Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        Pin::new(f).poll(&mut tcx)
    }

    #[test]
    fn child_first_poll_emits_one_start_child_then_pends() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7i64);
        assert!(poll_child(&mut f).is_pending());
        let cmds = ctx.drain_commands();
        assert_eq!(cmds.len(), 1);
        assert!(matches!(&cmds[0],
            Command::StartChild { seq: 0, workflow_type, .. } if workflow_type == "Echo"));
    }

    #[test]
    fn child_re_poll_does_not_duplicate_the_command() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7i64);
        assert!(poll_child(&mut f).is_pending());
        let _ = ctx.drain_commands();
        assert!(poll_child(&mut f).is_pending());
        assert!(
            ctx.drain_commands().is_empty(),
            "in-flight child seq must not re-emit"
        );
    }

    #[test]
    fn child_resolves_to_typed_output_when_result_applied() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7i64);
        assert!(poll_child(&mut f).is_pending());
        ctx.apply_child_result(0, Ok(serde_json::to_vec(&42i64).unwrap()));
        match poll_child(&mut f) {
            Poll::Ready(Ok(v)) => assert_eq!(v, 42),
            other => panic!("expected Ready(Ok(42)), got {other:?}"),
        }
    }

    #[test]
    fn child_surfaces_failure() {
        let ctx = ctx();
        let mut f = ctx.child_workflow::<Echo>(7i64);
        assert!(poll_child(&mut f).is_pending());
        ctx.apply_child_result(0, Err(crate::Error::new("child boom")));
        match poll_child(&mut f) {
            Poll::Ready(Err(e)) => assert_eq!(e.message, "child boom"),
            other => panic!("expected Ready(Err), got {other:?}"),
        }
    }
}
