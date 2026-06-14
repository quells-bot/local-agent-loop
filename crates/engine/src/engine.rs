use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;

use crate::{ExecStatus, History, NewActivityTask, TaskQueue, TurnCommit};

/// Options for starting a workflow (spec §7.1). `id` is the dedup key.
pub struct StartOptions {
    pub id: String,
}
impl Default for StartOptions {
    fn default() -> Self {
        Self { id: String::new() }
    }
}

/// Emitted to the completion observer after a turn drives a run terminal (spec §7.3).
#[derive(Debug, Clone)]
pub struct RunCompleted {
    pub run_id: String,
    pub workflow_id: String,
    pub status: ExecStatus,
    pub result: Option<Vec<u8>>,
}

type ReplayFn = Arc<
    dyn Fn(workflow::Info, &[workflow::Event]) -> Result<workflow::ReplayOutcome, workflow::Nondeterminism>
        + Send
        + Sync,
>;
type RunnerFn = Arc<
    dyn Fn(activity::Context, Vec<u8>) -> Pin<Box<dyn Future<Output = Result<Vec<u8>, activity::Error>> + Send>>
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

impl Engine {
    pub fn new(history: Arc<dyn History>, queue: Arc<dyn TaskQueue>) -> Self {
        Self { history, queue, workflows: HashMap::new(), activities: HashMap::new(), observer: None }
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
                    let input: A::Input = serde_json::from_slice(&bytes)
                        .map_err(|e| activity::Error::fatal(format!("activity input deserialize: {e}")))?;
                    let out = A::run(ctx, input).await?;
                    serde_json::to_vec(&out)
                        .map_err(|e| activity::Error::fatal(format!("activity output serialize: {e}")))
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
                Some((_, ExecStatus::Running, _)) => tokio::time::sleep(Duration::from_millis(5)).await,
                None => anyhow::bail!("no execution for workflow id {}", self.workflow_id),
            }
        }
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
        Ok(Handle { run_id, workflow_id: opts.id, history: self.history.clone() })
    }
}
