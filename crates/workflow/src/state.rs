// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use std::future::Future;
use std::pin::Pin;
use std::task::{Context as TaskContext, Poll};

use crate::{Command, CommandResult, Context, Info};

/// A live workflow future plus its shared `Context`. The driver (chunk 1d) holds
/// one of these per cached run; here it is also the unit-test harness.
pub struct WorkflowState {
    ctx: Context,
    // Output is the JSON-encoded workflow result. !Send by construction.
    main: Pin<Box<dyn Future<Output = Result<Vec<u8>, crate::Error>>>>,
    // Detached spawned branches, polled every turn in creation order (spec §4.4).
    // `None` once a branch has completed (a completed future must not be re-polled).
    spawned: Vec<Option<Pin<Box<dyn Future<Output = ()>>>>>,
}

impl WorkflowState {
    /// Build a fresh future for workflow `W` with typed input.
    pub fn start<W: crate::Definition>(info: Info, input: W::Input) -> Self {
        let ctx = Context::new(info);
        let run_ctx = ctx.clone();
        let main = Box::pin(async move {
            let out = W::run(run_ctx, input).await?;
            serde_json::to_vec(&out).map_err(|e| crate::Error::new(e.to_string()))
        });
        Self {
            ctx,
            main,
            spawned: Vec::new(),
        }
    }

    pub fn context(&self) -> &Context {
        &self.ctx
    }

    /// Drive `main` and every live spawned branch to quiescence for this turn:
    /// re-poll until nothing makes progress (no Ready transition, no new command, no
    /// new spawn). No new event is applied here — the caller has already applied at
    /// most one (spec §4.1) — so determinism is preserved while detached branches and
    /// resolved `SpawnHandle`s still get observed within the same turn.
    pub fn poll_turn(&mut self) -> Poll<Result<Vec<u8>, crate::Error>> {
        let waker = futures::task::noop_waker();
        let mut tcx = TaskContext::from_waker(&waker);
        loop {
            let mut progressed = false;
            let before_cmds = self.ctx.commands_len();

            if let Poll::Ready(r) = self.main.as_mut().poll(&mut tcx) {
                return Poll::Ready(r);
            }

            // Absorb spawns created by `main` this iteration (creation order).
            for fut in self.ctx.drain_new_spawns() {
                self.spawned.push(Some(fut));
                progressed = true;
            }

            // Poll each live spawned branch once.
            for slot in self.spawned.iter_mut() {
                if let Some(fut) = slot.as_mut() {
                    if fut.as_mut().poll(&mut tcx).is_ready() {
                        *slot = None; // completed — do not poll again
                        progressed = true;
                    }
                }
            }

            // Absorb futures spawned by branches during this sweep — they are polled
            // on the next quiescence iteration (pushing one sets `progressed`).
            for fut in self.ctx.drain_new_spawns() {
                self.spawned.push(Some(fut));
                progressed = true;
            }

            if self.ctx.commands_len() != before_cmds {
                progressed = true;
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }

    pub fn drain_commands(&self) -> Vec<Command> {
        self.ctx.drain_commands()
    }

    pub fn apply_result(&self, seq: u64, result: CommandResult) {
        self.ctx.apply_result(seq, result);
    }

    pub fn apply_timer_fired(&self, seq: u64) {
        self.ctx.apply_timer_fired(seq);
    }

    pub fn apply_signal(&self, name: String, payload: Vec<u8>) {
        self.ctx.apply_signal(name, payload);
    }

    pub fn apply_child_result(&self, seq: u64, result: Result<Vec<u8>, crate::Error>) {
        self.ctx.apply_child_result(seq, result);
    }

    pub fn apply_patch(&self, change_id: String) {
        self.ctx.apply_patch(change_id);
    }

    pub fn set_replaying(&self, replaying: bool) {
        self.ctx.set_replaying(replaying);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Error};
    use activity::Execution;

    // Workflow: b = Add(Add(1,2), 10) == 13, via two sequential activities.
    struct Sum;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Sum {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Sum";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let a = ctx.activity::<Add>((1, 2)).await?;
            let b = ctx.activity::<Add>((a, 10)).await?;
            Ok(b)
        }
    }

    struct Add;
    #[async_trait::async_trait]
    impl activity::Definition for Add {
        type Input = (i64, i64);
        type Output = i64;
        const TYPE: &'static str = "Add";
        async fn run(_c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
            Ok(i.0 + i.1)
        }
    }

    fn info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "Sum".into(),
        }
    }

    #[test]
    fn drives_two_sequential_activities_to_completion() {
        let mut s = WorkflowState::start::<Sum>(info(), ());

        // Turn 1: schedules activity seq 0.
        assert!(s.poll_turn().is_pending());
        let c0 = s.drain_commands();
        assert!(matches!(&c0[0], Command::ScheduleActivity { seq: 0, .. }));

        // Feed result of seq 0 (=3), one event this turn.
        s.apply_result(
            0,
            CommandResult::ActivityCompleted(serde_json::to_vec(&3i64).unwrap()),
        );
        assert!(s.poll_turn().is_pending());
        let c1 = s.drain_commands();
        assert!(matches!(&c1[0], Command::ScheduleActivity { seq: 1, .. }));

        // Feed result of seq 1 (=13).
        s.apply_result(
            1,
            CommandResult::ActivityCompleted(serde_json::to_vec(&13i64).unwrap()),
        );
        match s.poll_turn() {
            Poll::Ready(Ok(bytes)) => {
                let out: i64 = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(out, 13);
            }
            other => panic!("expected Ready(Ok), got {other:?}"),
        }
    }
}
