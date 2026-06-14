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
    let input: W::Input = serde_json::from_slice(&input_bytes)
        .map_err(|e| Nondeterminism { seq: 0, detail: format!("input deserialize: {e}") })?;

    // 2. Index recorded schedules (for the divergence check) and ordered results.
    let mut recorded_sched: HashMap<u64, (String, Vec<u8>)> = HashMap::new();
    let mut results: Vec<(u64, CommandResult)> = Vec::new();
    for ev in history {
        match ev {
            Event::ActivityScheduled { seq, activity_type, input, .. } => {
                recorded_sched.insert(*seq, (activity_type.clone(), input.clone()));
            }
            Event::ActivityCompleted { seq, output } => {
                results.push((*seq, CommandResult::ActivityCompleted(output.clone())));
            }
            Event::ActivityFailed { seq, error } => {
                results.push((*seq, CommandResult::ActivityFailed(error.clone())));
            }
            Event::WorkflowStarted { .. } => {}
        }
    }

    // 3. Drive the workflow, applying one result per turn.
    let mut state = WorkflowState::start::<W>(info, input);
    let mut commands = Vec::new();
    let mut cursor = 0usize;
    loop {
        let poll = state.poll_turn();
        for cmd in state.drain_commands() {
            // Single-variant destructure; more Command variants arrive in later
            // passes (StartTimer, StartChild), at which point this becomes a
            // real match. The irrefutable-let lint is expected until then.
            #[allow(irrefutable_let_patterns)]
            let Command::ScheduleActivity { seq, activity_type, input, .. } = &cmd;
            if let Some((rty, rin)) = recorded_sched.get(seq) {
                if rty != activity_type || rin != input {
                    return Err(Nondeterminism {
                        seq: *seq,
                        detail: format!(
                            "history recorded schedule of {rty}, workflow emitted {activity_type}"
                        ),
                    });
                }
            }
            commands.push(cmd);
        }
        match poll {
            Poll::Ready(result) => {
                return Ok(ReplayOutcome { commands, completion: Some(result) });
            }
            Poll::Pending => {
                if cursor < results.len() {
                    let (seq, r) = results[cursor].clone();
                    state.apply_result(seq, r);
                    cursor += 1;
                } else {
                    return Ok(ReplayOutcome { commands, completion: None });
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
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "Sum".into(),
        }
    }

    fn add_input(a: i64, b: i64) -> Vec<u8> {
        serde_json::to_vec(&(a, b)).unwrap()
    }

    fn full_history() -> Vec<Event> {
        vec![
            Event::WorkflowStarted { input: serde_json::to_vec(&()).unwrap() },
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: add_input(1, 2),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted { seq: 0, output: serde_json::to_vec(&3i64).unwrap() },
            Event::ActivityScheduled {
                seq: 1,
                activity_type: "Add".into(),
                input: add_input(3, 10),
                retry: RetryPolicy::none(),
            },
            Event::ActivityCompleted { seq: 1, output: serde_json::to_vec(&13i64).unwrap() },
        ]
    }

    #[test]
    fn replays_full_history_to_same_output_and_commands() {
        let outcome = cold_replay::<Sum>(info(), &full_history()).unwrap();
        let bytes = outcome.completion.unwrap().unwrap();
        let out: i64 = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(out, 13);
        assert_eq!(outcome.commands.len(), 2);
        assert!(matches!(&outcome.commands[0], Command::ScheduleActivity { seq: 0, .. }));
        assert!(matches!(&outcome.commands[1], Command::ScheduleActivity { seq: 1, .. }));
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
}
