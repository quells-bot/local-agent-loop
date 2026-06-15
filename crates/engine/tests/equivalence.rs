// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec: docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use std::sync::Arc;
use std::time::Duration;

use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
use persist::Sqlite;
use workflow::{cold_replay, Event, Execution, Info, ReplayOutcome};

// Activity: Add(a, b) -> a + b.
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

// Activity-only workflow: Add(Add(1, 2), 10) == 13.
struct Sum;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Sum {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Sum";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let a = ctx.activity::<Add>((1, 2)).await?;
        let b = ctx.activity::<Add>((a, 10)).await?;
        Ok(b)
    }
}

// Timer + activity: exercises TimerStarted/TimerFired interleaved with an activity.
struct Nap;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Nap {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Nap";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        ctx.sleep(Duration::from_millis(0)).await;
        let a = ctx.activity::<Add>((4, 5)).await?;
        Ok(a)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Sum>();
    e.register_workflow::<Nap>();
    e.register_activity::<Add>();
    e
}

/// Pump driver + worker + timer turns until quiescent (deterministic; no background
/// loops). Mirrors end_to_end.rs but also fires due timers so Nap can finish.
async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        let timed = engine.process_one_timer().await?;
        if !drove && !worked && !timed {
            return Ok(());
        }
    }
}

/// The standing CI guard: a workflow that ran to completion through the engine, when
/// cold-replayed from its PERSISTED history, must (1) not diverge, (2) complete with
/// the same result the engine stored, and (3) replay idempotently (spec §12, §14).
async fn assert_live_matches_replay<W: workflow::Definition>(
    db: &Sqlite,
    workflow_type: &str,
    workflow_id: &str,
) {
    let (run_id, status, stored_result) = db.find_execution(workflow_id).await.unwrap().unwrap();
    assert_eq!(
        status,
        ExecStatus::Completed,
        "{workflow_id} should complete"
    );
    let stored_result = stored_result.expect("completed run has a result");

    let events: Vec<Event> = db
        .read_history(&run_id)
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.event)
        .collect();

    let info = Info {
        execution: Execution {
            workflow_id: workflow_id.to_string(),
            run_id: run_id.clone(),
        },
        parent: None,
        workflow_type: workflow_type.to_string(),
    };

    let first: ReplayOutcome = cold_replay::<W>(info.clone(), &events)
        .expect("cold replay of a real history must not diverge");
    assert_eq!(
        first.completion,
        Some(Ok(stored_result)),
        "replayed completion must equal the engine's stored result"
    );

    let second = cold_replay::<W>(info, &events).unwrap();
    assert_eq!(first, second, "cold replay must be idempotent");
}

#[tokio::test]
async fn activity_workflow_live_matches_cold_replay() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<Sum>(
            (),
            StartOptions {
                id: "eq-sum".into(),
            },
        )
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    assert_live_matches_replay::<Sum>(&db, "Sum", "eq-sum").await;
}

#[tokio::test]
async fn timer_workflow_live_matches_cold_replay() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<Nap>(
            (),
            StartOptions {
                id: "eq-nap".into(),
            },
        )
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    assert_live_matches_replay::<Nap>(&db, "Nap", "eq-nap").await;
}
