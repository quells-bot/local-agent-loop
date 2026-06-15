use std::collections::HashMap;
use std::task::Poll;

use crate::{Command, CommandResult, Event, Info, WorkflowState};

/// Result of replaying a full history: the command stream the workflow produced
/// and, if it completed within the history, its JSON-encoded output.
#[derive(Debug, PartialEq, Eq)]
pub struct ReplayOutcome {
    pub commands: Vec<Command>,
    /// Some(Ok) = completed with output, Some(Err) = workflow returned an error
    /// (Failed), None = still running (history ended mid-flight). A *Failed*
    /// workflow is NOT nondeterminism — only schedule mismatches are.
    pub completion: Option<Result<Vec<u8>, crate::Error>>,
}

/// Replay diverged from recorded history (spec §12, Invariant 9).
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("nondeterminism at seq {seq}: {detail}")]
pub struct Nondeterminism {
    pub seq: u64,
    pub detail: String,
}

/// Re-execute workflow `W` from `history`, replaying recorded outcomes one per
/// turn (spec §3, §4.1). Used by the driver's cold path and the equivalence
/// guard (chunk 5a).
pub fn cold_replay<W: crate::Definition>(
    info: Info,
    history: &[Event],
) -> Result<ReplayOutcome, Nondeterminism> {
    // 1. Recover input from the first event.
    let input_bytes = match history.first() {
        Some(Event::WorkflowStarted { input }) => input.clone(),
        _ => {
            return Err(Nondeterminism {
                seq: 0,
                detail: "history must start with WorkflowStarted".into(),
            })
        }
    };
    let input: W::Input = serde_json::from_slice(&input_bytes).map_err(|e| Nondeterminism {
        seq: 0,
        detail: format!("input deserialize: {e}"),
    })?;

    // 2. Index recorded schedules (for divergence checks) and the ordered stream of
    //    things to apply one-per-turn (activity outcomes AND timer fires), in
    //    event_id order. Timers resolve with no payload, so they get their own
    //    apply variant rather than riding the CommandResult map.
    enum Applied {
        Result(u64, CommandResult),
        Timer(u64),
        Signal(String, Vec<u8>),
        Child(u64, Result<Vec<u8>, crate::Error>),
    }
    // One recorded command per seq, carrying its kind + payload, so the divergence
    // check (Invariant 9) catches a *kind* mismatch (activity-vs-timer-vs-child at the
    // same seq), not just a same-kind payload mismatch.
    #[derive(PartialEq, Eq)]
    enum RecordedCmd {
        Activity {
            activity_type: String,
            input: Vec<u8>,
        },
        Timer {
            duration_ms: u64,
        },
        Child {
            workflow_type: String,
            input: Vec<u8>,
        },
    }
    impl RecordedCmd {
        fn describe(&self) -> String {
            match self {
                RecordedCmd::Activity { activity_type, .. } => format!("activity {activity_type}"),
                RecordedCmd::Timer { duration_ms } => format!("timer {duration_ms}ms"),
                RecordedCmd::Child { workflow_type, .. } => format!("child {workflow_type}"),
            }
        }
    }
    // Map an emitted command to (seq, RecordedCmd) for comparison. Returns None for
    // commands that carry no seq and are divergence-exempt (none today; a later pass's
    // RecordPatch will add a None arm here).
    fn as_recorded(cmd: &Command) -> Option<(u64, RecordedCmd)> {
        match cmd {
            Command::ScheduleActivity {
                seq,
                activity_type,
                input,
                ..
            } => Some((
                *seq,
                RecordedCmd::Activity {
                    activity_type: activity_type.clone(),
                    input: input.clone(),
                },
            )),
            Command::StartTimer { seq, duration_ms } => Some((
                *seq,
                RecordedCmd::Timer {
                    duration_ms: *duration_ms,
                },
            )),
            Command::StartChild {
                seq,
                workflow_type,
                input,
            } => Some((
                *seq,
                RecordedCmd::Child {
                    workflow_type: workflow_type.clone(),
                    input: input.clone(),
                },
            )),
        }
    }

    let mut recorded_cmd: HashMap<u64, RecordedCmd> = HashMap::new();
    let mut applied: Vec<Applied> = Vec::new();
    for ev in history {
        match ev {
            Event::ActivityScheduled {
                seq,
                activity_type,
                input,
                ..
            } => {
                recorded_cmd.insert(
                    *seq,
                    RecordedCmd::Activity {
                        activity_type: activity_type.clone(),
                        input: input.clone(),
                    },
                );
            }
            Event::ActivityCompleted { seq, output } => {
                applied.push(Applied::Result(
                    *seq,
                    CommandResult::ActivityCompleted(output.clone()),
                ));
            }
            Event::ActivityFailed { seq, error } => {
                applied.push(Applied::Result(
                    *seq,
                    CommandResult::ActivityFailed(error.clone()),
                ));
            }
            Event::TimerStarted { seq, duration_ms } => {
                recorded_cmd.insert(
                    *seq,
                    RecordedCmd::Timer {
                        duration_ms: *duration_ms,
                    },
                );
            }
            Event::TimerFired { seq } => {
                applied.push(Applied::Timer(*seq));
            }
            Event::WorkflowStarted { .. } => {}
            Event::SignalReceived { name, payload } => {
                applied.push(Applied::Signal(name.clone(), payload.clone()));
            }
            Event::ChildScheduled {
                seq,
                workflow_type,
                input,
            } => {
                recorded_cmd.insert(
                    *seq,
                    RecordedCmd::Child {
                        workflow_type: workflow_type.clone(),
                        input: input.clone(),
                    },
                );
            }
            Event::ChildCompleted { seq, result } => {
                applied.push(Applied::Child(*seq, result.clone().into()));
            }
        }
    }

    // 3. Drive the workflow, applying one item per turn.
    let mut state = WorkflowState::start::<W>(info, input);
    let mut commands = Vec::new();
    let mut cursor = 0usize;
    loop {
        let poll = state.poll_turn();
        for cmd in state.drain_commands() {
            if let Some((seq, emitted)) = as_recorded(&cmd) {
                if let Some(rec) = recorded_cmd.get(&seq) {
                    if *rec != emitted {
                        return Err(Nondeterminism {
                            seq,
                            detail: format!(
                                "history recorded {} at seq {seq}, workflow emitted {}",
                                rec.describe(),
                                emitted.describe()
                            ),
                        });
                    }
                }
            }
            commands.push(cmd);
        }
        match poll {
            Poll::Ready(result) => {
                return Ok(ReplayOutcome {
                    commands,
                    completion: Some(result),
                });
            }
            Poll::Pending => {
                if cursor < applied.len() {
                    match &applied[cursor] {
                        Applied::Result(seq, r) => state.apply_result(*seq, r.clone()),
                        Applied::Timer(seq) => state.apply_timer_fired(*seq),
                        Applied::Signal(name, payload) => {
                            state.apply_signal(name.clone(), payload.clone())
                        }
                        Applied::Child(seq, r) => state.apply_child_result(*seq, r.clone()),
                    }
                    cursor += 1;
                } else {
                    return Ok(ReplayOutcome {
                        commands,
                        completion: None,
                    });
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Context, Error, RetryPolicy};
    use activity::Execution;

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

    fn add_input(a: i64, b: i64) -> Vec<u8> {
        serde_json::to_vec(&(a, b)).unwrap()
    }

    fn full_history() -> Vec<Event> {
        vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 0,
                output: serde_json::to_vec(&3i64).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(3, 10),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 1,
                output: serde_json::to_vec(&13i64).unwrap(),
            },
        ]
    }

    #[test]
    fn replays_full_history_to_same_output_and_commands() {
        let outcome = cold_replay::<Sum>(info(), &full_history()).unwrap();
        let bytes = outcome.completion.unwrap().unwrap();
        let out: i64 = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out, 13);
        assert_eq!(outcome.commands.len(), 2);
        assert!(matches!(
            &outcome.commands[0],
            Command::ScheduleActivity { seq: 0, .. }
        ));
        assert!(matches!(
            &outcome.commands[1],
            Command::ScheduleActivity { seq: 1, .. }
        ));
    }

    #[test]
    fn detects_divergent_activity_type() {
        // History claims seq 0 scheduled "Charge", but Sum schedules "Add".
        let mut h = full_history();
        h[1] = Event::ActivityScheduled {
            seq: 0,
            activity_type: "Charge".into(),
            input: add_input(1, 2),
            retry: RetryPolicy::none(),
        };
        let err = cold_replay::<Sum>(info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(err.detail.contains("Charge"));
    }

    #[test]
    fn detects_divergent_activity_input() {
        // History keeps activity_type "Add" for seq 0 but records a corrupted
        // input; Sum emits add_input(1, 2). The type half matches, so this
        // exercises the `rin != input` half of the divergence check. The detail
        // reports the type ("Add" on both sides), so we assert only on .seq.
        let mut h = full_history();
        h[1] = Event::ActivityScheduled {
            seq: 0,
            activity_type: "Add".into(),
            input: add_input(9, 9),
            retry: RetryPolicy::none(),
        };
        let err = cold_replay::<Sum>(info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
    }

    #[test]
    fn failed_activity_surfaces_as_workflow_error() {
        // seq 0 scheduled, then Failed. Sum's `?` converts the activity::Error
        // into a workflow::Error and returns it: the workflow future resolves to
        // Err. This is NOT nondeterminism — cold_replay returns Ok with a
        // completion of Some(Err(_)).
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
            Event::ActivityFailed {
                seq: 0,
                error: activity::Error::fatal("boom"),
            },
        ];
        let outcome = cold_replay::<Sum>(info(), &h).unwrap();
        match outcome.completion {
            Some(Err(e)) => assert_eq!(e.message, "boom"),
            other => panic!("expected Some(Err(boom)), got {other:?}"),
        }
    }

    // Workflow that sleeps, then runs one activity. Exercises a timer interleaved
    // with an activity under the one-event-per-turn rule.
    struct Nap;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Nap {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Nap";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            ctx.sleep(std::time::Duration::from_millis(500)).await;
            let a = ctx.activity::<Add>((1, 2)).await?;
            Ok(a)
        }
    }

    fn nap_info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "Nap".into(),
        }
    }

    #[test]
    fn replays_timer_then_activity() {
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::TimerStarted {
                seq: 0,
                duration_ms: 500,
            },
            Event::TimerFired { seq: 0 },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 1,
                output: serde_json::to_vec(&3i64).unwrap(),
            },
        ];
        let outcome = cold_replay::<Nap>(nap_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 3);
        // First command is the timer (seq 0), then the activity (seq 1).
        assert!(matches!(
            &outcome.commands[0],
            Command::StartTimer {
                seq: 0,
                duration_ms: 500
            }
        ));
        assert!(matches!(
            &outcome.commands[1],
            Command::ScheduleActivity { seq: 1, .. }
        ));
    }

    #[test]
    fn detects_divergent_timer_duration() {
        // History recorded a 500ms timer at seq 0; Nap emits 500ms, so mutate the
        // record to 999ms and expect a nondeterminism error at seq 0.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::TimerStarted {
                seq: 0,
                duration_ms: 999,
            },
            Event::TimerFired { seq: 0 },
        ];
        let err = cold_replay::<Nap>(nap_info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(err.detail.contains("timer"));
    }

    // Workflow that spawns a detached branch running one activity, then awaits it.
    // Exercises the ordered scheduler: the spawned task is polled though `main`
    // never polls it directly.
    struct Detached;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Detached {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Detached";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let ctx2 = ctx.clone();
            let h = ctx.spawn(async move { ctx2.activity::<Add>((3, 4)).await.unwrap() });
            let v = h.await;
            Ok(v)
        }
    }

    #[test]
    fn replays_spawned_branch() {
        let info = Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "Detached".into(),
        };
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            // The spawned branch's activity is the first (and only) seq allocated.
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(3, 4),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 0,
                output: serde_json::to_vec(&7i64).unwrap(),
            },
        ];
        let outcome = cold_replay::<Detached>(info, &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 7);
        assert_eq!(outcome.commands.len(), 1);
        assert!(matches!(
            &outcome.commands[0],
            Command::ScheduleActivity { seq: 0, .. }
        ));
    }

    // Fire-and-forget: a spawned branch is never awaited; `main` does its own
    // activity and returns. main completes without waiting on the branch.
    // Seq order: main is polled first and reaches its OWN activity (seq 0) before
    // the spawned branch is absorbed and polled (its activity is seq 1).
    struct FireAndForget;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for FireAndForget {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "FireAndForget";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let ctx2 = ctx.clone();
            let _detached = ctx.spawn(async move { ctx2.activity::<Add>((1, 1)).await.unwrap() });
            let v = ctx.activity::<Add>((10, 20)).await?;
            Ok(v)
        }
    }

    #[test]
    fn fire_and_forget_spawn_does_not_block_completion() {
        let info = Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "FireAndForget".into(),
        };
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(10, 20),
                retry: RetryPolicy::none(),
            },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(1, 1),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted {
                seq: 0,
                output: serde_json::to_vec(&30i64).unwrap(),
            },
            Event::ActivityCompleted {
                seq: 1,
                output: serde_json::to_vec(&2i64).unwrap(),
            },
        ];
        let outcome = cold_replay::<FireAndForget>(info, &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(
            out, 30,
            "main returns its own activity result regardless of the detached branch"
        );
        assert_eq!(
            outcome.commands.len(),
            2,
            "both the main and the detached activity are scheduled"
        );
    }

    // Two concurrent spawns whose activities complete OUT OF ORDER (branch B's
    // result is recorded before branch A's). The slot mechanism must still deliver
    // each branch its own result. Seq order: main spawns A then B, awaits A; both
    // branches are absorbed in creation order, so A's activity is seq 0, B's seq 1.
    struct TwoSpawns;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for TwoSpawns {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "TwoSpawns";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let c1 = ctx.clone();
            let c2 = ctx.clone();
            let a = ctx.spawn(async move { c1.activity::<Add>((1, 1)).await.unwrap() });
            let b = ctx.spawn(async move { c2.activity::<Add>((2, 2)).await.unwrap() });
            let va = a.await;
            let vb = b.await;
            Ok(va * 10 + vb)
        }
    }

    #[test]
    fn two_spawns_resolve_out_of_order() {
        let info = Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "TwoSpawns".into(),
        };
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 1),
                retry: RetryPolicy::none(),
            },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(2, 2),
                retry: RetryPolicy::none(),
            },
            // Branch B (seq 1) completes BEFORE branch A (seq 0).
            Event::ActivityCompleted {
                seq: 1,
                output: serde_json::to_vec(&4i64).unwrap(),
            },
            Event::ActivityCompleted {
                seq: 0,
                output: serde_json::to_vec(&2i64).unwrap(),
            },
        ];
        let outcome = cold_replay::<TwoSpawns>(info, &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(
            out, 24,
            "va=2 (A=1+1), vb=4 (B=2+2) -> 2*10+4 even though B resolved first"
        );
        assert_eq!(outcome.commands.len(), 2);
    }

    // --- Pass 3a: signals -------------------------------------------------

    // Workflow that blocks on a single signal, returning its bool payload.
    struct WaitApprove;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for WaitApprove {
        type Input = ();
        type Output = bool;
        const TYPE: &'static str = "WaitApprove";
        async fn run(ctx: Context, _i: ()) -> Result<bool, Error> {
            let approvals = ctx.signal_channel::<bool>("approve");
            let v = approvals.recv().await?;
            Ok(v)
        }
    }

    fn wait_info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "WaitApprove".into(),
        }
    }

    #[test]
    fn replays_signal_received() {
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::SignalReceived {
                name: "approve".into(),
                payload: serde_json::to_vec(&true).unwrap(),
            },
        ];
        let outcome = cold_replay::<WaitApprove>(wait_info(), &h).unwrap();
        let out: bool = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert!(out);
        assert!(
            outcome.commands.is_empty(),
            "signals are inbound: they allocate no command and no seq"
        );
    }

    #[test]
    fn signal_for_other_name_leaves_recv_pending() {
        // A signal for a DIFFERENT name must not resolve the "approve" recv.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::SignalReceived {
                name: "other".into(),
                payload: serde_json::to_vec(&true).unwrap(),
            },
        ];
        let outcome = cold_replay::<WaitApprove>(wait_info(), &h).unwrap();
        assert!(
            outcome.completion.is_none(),
            "a signal for a different name does not wake recv()"
        );
    }

    // Workflow that receives TWO signals of the same name, in order.
    struct TwoRecv;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for TwoRecv {
        type Input = ();
        type Output = (i64, i64);
        const TYPE: &'static str = "TwoRecv";
        async fn run(ctx: Context, _i: ()) -> Result<(i64, i64), Error> {
            let ch = ctx.signal_channel::<i64>("n");
            let a = ch.recv().await?;
            let b = ch.recv().await?;
            Ok((a, b))
        }
    }

    #[test]
    fn two_signals_resolve_in_order_one_per_turn() {
        let info = Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "TwoRecv".into(),
        };
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::SignalReceived {
                name: "n".into(),
                payload: serde_json::to_vec(&1i64).unwrap(),
            },
            Event::SignalReceived {
                name: "n".into(),
                payload: serde_json::to_vec(&2i64).unwrap(),
            },
        ];
        let outcome = cold_replay::<TwoRecv>(info, &h).unwrap();
        let out: (i64, i64) =
            serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(
            out,
            (1, 2),
            "the Nth recv pops the Nth buffered signal of that name"
        );
    }

    // Signal-or-timeout: race an "approve" signal (biased winner) against a sleep.
    // The interleaved history exercises the TimerStarted echo + SignalReceived inbound
    // event in one replay — the spec §6.3 motivating pattern.
    struct SignalOrTimeout;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for SignalOrTimeout {
        type Input = u64; // timeout in ms
        type Output = String;
        const TYPE: &'static str = "SignalOrTimeout";
        async fn run(ctx: Context, timeout_ms: u64) -> Result<String, Error> {
            use futures::{select_biased, FutureExt};
            let approvals = ctx.signal_channel::<bool>("approve");
            let recv = approvals.recv().fuse();
            let nap = ctx
                .sleep(std::time::Duration::from_millis(timeout_ms))
                .fuse();
            futures::pin_mut!(recv, nap);
            let out = select_biased! {
                a = recv => if a? { "approved" } else { "rejected" },
                _ = nap => "timed_out",
            };
            Ok(out.to_string())
        }
    }

    // Replay-determinism guard for the select-biased interleaving: a run that took the
    // signal branch live, cold-replayed from [WorkflowStarted, TimerStarted, SignalReceived],
    // must re-emit the SAME StartTimer command (no divergence), apply the inbound signal
    // one-per-turn, and re-take the signal branch — proving the TimerStarted *echo* is not
    // miscounted as an applied inbound event (Inv 3/9/10, §6.2).
    #[test]
    fn signal_or_timeout_replays_signal_branch_deterministically() {
        let info = Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "SignalOrTimeout".into(),
        };
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&86_400_000u64).unwrap(),
            },
            // The day-long timer the workflow started on turn 1 (a command echo, NOT an
            // applied inbound event).
            Event::TimerStarted {
                seq: 0,
                duration_ms: 86_400_000,
            },
            // The signal that resolved the biased `recv` branch.
            Event::SignalReceived {
                name: "approve".into(),
                payload: serde_json::to_vec(&true).unwrap(),
            },
        ];
        let outcome = cold_replay::<SignalOrTimeout>(info, &h).unwrap();
        let out: String = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(
            out, "approved",
            "the signal branch wins identically on replay"
        );
        assert_eq!(
            outcome.commands,
            vec![Command::StartTimer {
                seq: 0,
                duration_ms: 86_400_000
            }],
            "the StartTimer echo re-emits at the same seq; the signal allocates no command"
        );
    }

    // --- Pass 4a: child workflows -----------------------------------------

    // A child workflow: input passthrough.
    struct Child;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Child {
        type Input = i64;
        type Output = i64;
        const TYPE: &'static str = "Child";
        async fn run(_ctx: Context, i: i64) -> Result<i64, Error> {
            Ok(i)
        }
    }

    // A parent that starts one child with input 5 and returns child_output + 1.
    struct Parent;
    #[async_trait::async_trait(?Send)]
    impl crate::Definition for Parent {
        type Input = ();
        type Output = i64;
        const TYPE: &'static str = "Parent";
        async fn run(ctx: Context, _i: ()) -> Result<i64, Error> {
            let v = ctx.child_workflow::<Child>(5i64).await?;
            Ok(v + 1)
        }
    }

    fn parent_info() -> Info {
        Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "Parent".into(),
        }
    }

    #[test]
    fn replays_child_completed_to_parent_output() {
        // History: child scheduled, child completed with output 10 (=5*2).
        // Parent's ChildFuture resolves to Ok(10), adds 1 -> output 11.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Child".into(),
                input: serde_json::to_vec(&5i64).unwrap(),
            },
            Event::ChildCompleted {
                seq: 0,
                result: crate::ChildResult::Completed(serde_json::to_vec(&10i64).unwrap()),
            },
        ];
        let outcome = cold_replay::<Parent>(parent_info(), &h).unwrap();
        let out: i64 = serde_json::from_slice(&outcome.completion.unwrap().unwrap()).unwrap();
        assert_eq!(out, 11, "child returned 10 (=5*2); parent adds 1");
        assert_eq!(outcome.commands.len(), 1);
        assert!(matches!(
            &outcome.commands[0],
            Command::StartChild { seq: 0, .. }
        ));
    }

    #[test]
    fn child_failure_propagates_to_parent_error() {
        // History: child scheduled, child completed with Failure.
        // Parent's ChildFuture resolves to Err, ? turns it into workflow error.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Child".into(),
                input: serde_json::to_vec(&5i64).unwrap(),
            },
            Event::ChildCompleted {
                seq: 0,
                result: crate::ChildResult::Failed(Error::new("child died")),
            },
        ];
        let outcome = cold_replay::<Parent>(parent_info(), &h).unwrap();
        match outcome.completion {
            Some(Err(e)) => assert_eq!(e.message, "child died"),
            other => panic!("expected Some(Err(child died)), got {other:?}"),
        }
    }

    #[test]
    fn detects_divergent_child_type() {
        // History recorded a child of type "Other" at seq 0, but Parent emits "Child".
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Other".into(),
                input: serde_json::to_vec(&5i64).unwrap(),
            },
        ];
        let err = cold_replay::<Parent>(parent_info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(err.detail.contains("Other"));
    }

    #[test]
    fn detects_kind_divergence_activity_recorded_timer_in_history() {
        // History recorded a TIMER at seq 0, but Sum emits an ACTIVITY at seq 0.
        // Pre-hardening this was silent (the activity map had no seq-0 entry).
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::TimerStarted {
                seq: 0,
                duration_ms: 500,
            },
        ];
        let err = cold_replay::<Sum>(info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(
            err.detail.contains("timer") && err.detail.contains("activity"),
            "detail should name both the recorded kind and the emitted kind, got: {}",
            err.detail
        );
    }

    #[test]
    fn detects_kind_divergence_child_emitted_activity_recorded() {
        // History recorded an ACTIVITY at seq 0, but Parent emits a CHILD at seq 0.
        let h = vec![
            Event::WorkflowStarted {
                input: serde_json::to_vec(&()).unwrap(),
            },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
        ];
        let err = cold_replay::<Parent>(parent_info(), &h).unwrap_err();
        assert_eq!(err.seq, 0);
        assert!(
            err.detail.contains("activity") && err.detail.contains("child"),
            "detail should name both the recorded kind and the emitted kind, got: {}",
            err.detail
        );
    }
}
