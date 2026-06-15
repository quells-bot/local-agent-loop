use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;

use crate::{ExecStatus, History, NewActivityTask, NewTimer, SignalOutcome, TaskQueue, TurnCommit};

/// Options for starting a workflow (spec §7.1). `id` is the dedup key.
#[derive(Default)]
pub struct StartOptions {
    pub id: String,
}

/// Emitted to the completion observer after a turn drives a run terminal (spec §7.3).
#[derive(Debug, Clone)]
pub struct RunCompleted {
    pub run_id: String,
    pub workflow_id: String,
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>,
}

/// Typed result of a host signal attempt (spec §6.1), so the IPC layer can forward a
/// meaningful outcome to the frontend. `WorkflowNotFound` / `NotRunning` are the
/// domain outcomes; `Internal` carries an unexpected backend failure.
#[derive(Debug, thiserror::Error)]
pub enum SignalError {
    #[error("no workflow with that id")]
    WorkflowNotFound,
    #[error("workflow is not running")]
    NotRunning,
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

/// Shared mapping from the trait-level outcome to the host-facing result.
fn outcome_to_result(outcome: SignalOutcome) -> Result<(), SignalError> {
    match outcome {
        SignalOutcome::Delivered => Ok(()),
        SignalOutcome::WorkflowNotFound => Err(SignalError::WorkflowNotFound),
        SignalOutcome::NotRunning => Err(SignalError::NotRunning),
    }
}

type ReplayFn = Arc<
    dyn Fn(
            workflow::Info,
            &[workflow::Event],
        ) -> Result<workflow::ReplayOutcome, workflow::Nondeterminism>
        + Send
        + Sync,
>;
type RunnerFn = Arc<
    dyn Fn(
            activity::Context,
            Vec<u8>,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, activity::Error>> + Send>>
        + Send
        + Sync,
>;
type Observer = Arc<dyn Fn(RunCompleted) + Send + Sync>;

pub struct Engine {
    history: Arc<dyn History>,
    queue: Arc<dyn TaskQueue>,
    workflows: HashMap<String, ReplayFn>,
    activities: HashMap<String, RunnerFn>,
    observer: Option<Observer>,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl Engine {
    pub fn new(history: Arc<dyn History>, queue: Arc<dyn TaskQueue>) -> Self {
        Self {
            history,
            queue,
            workflows: HashMap::new(),
            activities: HashMap::new(),
            observer: None,
        }
    }

    pub fn register_workflow<W: workflow::Definition>(&mut self) {
        self.workflows.insert(
            W::TYPE.to_string(),
            Arc::new(|info, events| workflow::cold_replay::<W>(info, events)),
        );
    }

    pub fn register_activity<A: activity::Definition>(&mut self) {
        self.activities.insert(
            A::TYPE.to_string(),
            Arc::new(|ctx, bytes| {
                Box::pin(async move {
                    let input: A::Input = serde_json::from_slice(&bytes).map_err(|e| {
                        activity::Error::fatal(format!("activity input deserialize: {e}"))
                    })?;
                    let out = A::run(ctx, input).await?;
                    serde_json::to_vec(&out).map_err(|e| {
                        activity::Error::fatal(format!("activity output serialize: {e}"))
                    })
                })
            }),
        );
    }

    pub fn on_run_completed<F: Fn(RunCompleted) + Send + Sync + 'static>(&mut self, f: F) {
        self.observer = Some(Arc::new(f));
    }
}

/// Durable handle to a started run (spec §7.1).
pub struct Handle {
    run_id: String,
    workflow_id: String,
    history: Arc<dyn History>,
}

impl Handle {
    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    /// Await durable completion, deserializing the workflow output (spec §9).
    pub async fn result<T: DeserializeOwned>(&self) -> anyhow::Result<T> {
        loop {
            match self.history.find_execution(&self.workflow_id).await? {
                Some((_, ExecStatus::Completed, Some(bytes))) => {
                    return Ok(serde_json::from_slice(&bytes)?);
                }
                Some((_, ExecStatus::Completed, None)) => anyhow::bail!("completed without result"),
                Some((_, ExecStatus::Failed, _)) => anyhow::bail!("workflow failed"),
                Some((_, ExecStatus::Running, _)) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                None => anyhow::bail!("no execution for workflow id {}", self.workflow_id),
            }
        }
    }

    /// Durably deliver a signal to this run (spec §6.1). Same contract as
    /// `Engine::signal_workflow`, scoped to the handle's `workflow_id`.
    pub async fn signal(&self, name: &str, payload: &[u8]) -> Result<(), SignalError> {
        let outcome = self
            .history
            .append_signal(&self.workflow_id, name, payload)
            .await?;
        outcome_to_result(outcome)
    }
}

impl Engine {
    /// Start a workflow, deduping by `opts.id` (spec §7.1).
    pub async fn start_workflow<W: workflow::Definition>(
        &self,
        input: W::Input,
        opts: StartOptions,
    ) -> anyhow::Result<Handle> {
        let input_bytes = serde_json::to_vec(&input)?;
        let candidate = uuid::Uuid::new_v4().to_string();
        let (_outcome, run_id) = self
            .history
            .create_execution(&candidate, &opts.id, W::TYPE, &input_bytes)
            .await?;
        Ok(Handle {
            run_id,
            workflow_id: opts.id,
            history: self.history.clone(),
        })
    }

    /// Durably deliver a signal to a running workflow by `workflow_id` (spec §7.2).
    /// Returns `Ok(())` only once the `SignalReceived` event is committed, so the
    /// caller (a Tauri command) can confirm to the frontend synchronously.
    pub async fn signal_workflow(
        &self,
        workflow_id: &str,
        name: &str,
        payload: &[u8],
    ) -> Result<(), SignalError> {
        let outcome = self.history.append_signal(workflow_id, name, payload).await?;
        outcome_to_result(outcome)
    }
}

use std::collections::HashSet;

impl Engine {
    /// Process one runnable workflow: cold-replay, persist newly-emitted commands,
    /// update status, fire the observer on terminal (spec §5.1). Returns false if
    /// nothing was runnable.
    pub async fn process_one_runnable(&self) -> anyhow::Result<bool> {
        let Some(run_id) = self.queue.next_runnable().await? else {
            return Ok(false);
        };
        let meta = self
            .history
            .load_run(&run_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("runnable run {run_id} has no execution row"))?;

        if meta.status != ExecStatus::Running {
            // Already terminal: a late completion re-marked it runnable. Preserve the
            // stored result and only clear the stale runnable flag — prevents the
            // observer double-firing (spec §7.3); the Inv 5 commit boundary still holds.
            let existing = self
                .history
                .find_execution(&meta.workflow_id)
                .await?
                .ok_or_else(|| anyhow::anyhow!("terminal run {run_id} has no execution row"))?;
            let result = existing.2;
            let commit = TurnCommit {
                events: Vec::new(),
                new_tasks: Vec::new(),
                new_timers: Vec::new(),
                status: meta.status,
                result,
            };
            self.history.commit_turn(&run_id, &commit).await?;
            return Ok(true);
        }

        let stored = self.history.read_history(&run_id).await?;
        let events: Vec<workflow::Event> = stored.into_iter().map(|s| s.event).collect();
        let recorded: HashSet<u64> = events
            .iter()
            .filter_map(|e| match e {
                workflow::Event::ActivityScheduled { seq, .. }
                | workflow::Event::TimerStarted { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect();

        let info = workflow::Info {
            execution: workflow::Execution {
                workflow_id: meta.workflow_id.clone(),
                run_id: run_id.clone(),
            },
            parent: None,
            workflow_type: meta.workflow_type.clone(),
        };
        let replay = match self.workflows.get(&meta.workflow_type) {
            Some(r) => r.clone(),
            None => {
                return self
                    .dead_letter(
                        &run_id,
                        &meta.workflow_id,
                        format!("unregistered workflow {}", meta.workflow_type),
                    )
                    .await;
            }
        };

        let outcome = match replay(info, &events) {
            Ok(o) => o,
            Err(e) => {
                return self
                    .dead_letter(
                        &run_id,
                        &meta.workflow_id,
                        format!("nondeterminism in {}: {e}", meta.workflow_type),
                    )
                    .await;
            }
        };

        // Persist only commands not already recorded in history.
        let mut new_events = Vec::new();
        let mut new_tasks = Vec::new();
        let mut new_timers = Vec::new();
        for cmd in &outcome.commands {
            match cmd {
                workflow::Command::ScheduleActivity {
                    seq,
                    activity_type,
                    input,
                    retry,
                } => {
                    if recorded.contains(seq) {
                        continue;
                    }
                    new_events.push(workflow::Event::ActivityScheduled {
                        seq: *seq,
                        activity_type: activity_type.clone(),
                        input: input.clone(),
                        retry: retry.clone(),
                    });
                    new_tasks.push(NewActivityTask {
                        seq: *seq as i64,
                        activity_type: activity_type.clone(),
                        input: input.clone(),
                        next_run_at: 0,
                    });
                }
                workflow::Command::StartTimer { seq, duration_ms } => {
                    if recorded.contains(seq) {
                        continue;
                    }
                    new_events.push(workflow::Event::TimerStarted {
                        seq: *seq,
                        duration_ms: *duration_ms,
                    });
                    // `fire_at` is wall-clock and deliberately NOT replayed: the
                    // duration is the deterministic, divergence-checked datum, and
                    // this absolute deadline must never feed back into replay (§5.3).
                    new_timers.push(NewTimer {
                        seq: *seq as i64,
                        fire_at: now_ms() + *duration_ms as i64,
                    });
                }
            }
        }

        let (status, result) = match &outcome.completion {
            Some(Ok(bytes)) => (ExecStatus::Completed, Some(bytes.clone())),
            Some(Err(err)) => (ExecStatus::Failed, Some(serde_json::to_vec(err)?)),
            None => (ExecStatus::Running, None),
        };

        let commit = TurnCommit {
            events: new_events,
            new_tasks,
            new_timers,
            status,
            result: result.clone(),
        };
        self.history.commit_turn(&run_id, &commit).await?;

        if status != ExecStatus::Running {
            if let Some(obs) = &self.observer {
                obs(RunCompleted {
                    run_id: run_id.clone(),
                    workflow_id: meta.workflow_id,
                    status,
                    result,
                });
            }
        }
        Ok(true)
    }
}

impl Engine {
    /// Terminally fail a run that cannot make progress (unregistered type, replay
    /// divergence). Commits a `Failed` turn — which clears `runnable`, so the driver
    /// stops retrying — and fires the completion observer (spec §5.1, §14). Returns
    /// `Ok(true)` so the caller's loop continues without the error backoff.
    async fn dead_letter(
        &self,
        run_id: &str,
        workflow_id: &str,
        message: String,
    ) -> anyhow::Result<bool> {
        let err = workflow::Error::new(message);
        let result = Some(serde_json::to_vec(&err)?);
        let commit = TurnCommit {
            events: Vec::new(),
            new_tasks: Vec::new(),
            new_timers: Vec::new(),
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
}

impl Engine {
    /// Lease one due activity task, run it, and record the outcome — completing on
    /// success/terminal failure, rescheduling with backoff otherwise (spec §5.2, §8).
    /// Returns false if nothing was due.
    pub async fn process_one_activity(&self) -> anyhow::Result<bool> {
        let Some(lease) = self.queue.lease_activity().await? else {
            return Ok(false);
        };

        let runner = match self.activities.get(&lease.activity_type) {
            Some(r) => r.clone(),
            None => {
                self.queue
                    .complete_activity(
                        &lease,
                        workflow::CommandResult::ActivityFailed(activity::Error::fatal(format!(
                            "unregistered activity {}",
                            lease.activity_type
                        ))),
                    )
                    .await?;
                return Ok(true);
            }
        };

        let ctx = activity::Context::new(activity::Info {
            execution: activity::Execution {
                workflow_id: lease.workflow_id.clone(),
                run_id: lease.run_id.clone(),
            },
            activity_id: lease.seq.to_string(),
            activity_type: lease.activity_type.clone(),
            attempt: lease.attempt,
        });

        match runner(ctx, lease.input.clone()).await {
            Ok(output) => {
                self.queue
                    .complete_activity(&lease, workflow::CommandResult::ActivityCompleted(output))
                    .await?;
            }
            Err(e) => {
                let exhausted = e.non_retryable || lease.attempt >= lease.retry.max_attempts;
                if exhausted {
                    self.queue
                        .complete_activity(&lease, workflow::CommandResult::ActivityFailed(e))
                        .await?;
                } else {
                    let delay = lease.retry.backoff_ms(lease.attempt + 1) as i64;
                    self.queue
                        .reschedule_activity(&lease, now_ms() + delay)
                        .await?;
                }
            }
        }
        Ok(true)
    }
}

impl Engine {
    /// Fire one due timer, if any (spec §5.3). Returns false if none was due.
    pub async fn process_one_timer(&self) -> anyhow::Result<bool> {
        self.queue.fire_due_timer().await
    }
}

impl Engine {
    /// Reclaim expired in-flight activity leases (spec §5.2). Returns the count.
    pub async fn reclaim_expired_activities(&self) -> anyhow::Result<u64> {
        self.queue.reclaim_expired_activities().await
    }
}

impl Engine {
    /// Spawn the driver and activity-worker loops as background tokio tasks and
    /// return a shared handle. Use the `process_one_*` methods directly in tests
    /// for deterministic stepping.
    pub fn start(self) -> Arc<Engine> {
        let engine = Arc::new(self);

        let driver = engine.clone();
        tokio::spawn(async move {
            loop {
                match driver.process_one_runnable().await {
                    Ok(true) => {}
                    Ok(false) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(err) => {
                        eprintln!("driver error: {err:#}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        let worker = engine.clone();
        tokio::spawn(async move {
            loop {
                match worker.process_one_activity().await {
                    Ok(true) => {}
                    Ok(false) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(err) => {
                        eprintln!("worker error: {err:#}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        let timers = engine.clone();
        tokio::spawn(async move {
            loop {
                match timers.process_one_timer().await {
                    Ok(true) => {}
                    Ok(false) => tokio::time::sleep(Duration::from_millis(5)).await,
                    Err(err) => {
                        eprintln!("timer error: {err:#}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        let sweeper = engine.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = sweeper.reclaim_expired_activities().await {
                    eprintln!("lease sweep error: {err:#}");
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });

        engine
    }
}
