use std::sync::Arc;

use engine::{Engine, ExecStatus, History, StartOptions};
use persist::Sqlite;

// Activity: Add(a, b) -> a + b.
struct Add;
#[async_trait::async_trait]
impl activity::Definition for Add {
    type Input = (i64, i64);
    type Output = i64;
    const TYPE: &'static str = "Add";
    async fn run(&self, _c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
        Ok(i.0 + i.1)
    }
}

// Workflow: Sum() -> Add(Add(1, 2), 10) == 13, via two sequential activities.
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

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Sum>();
    e.register_activity(Add);
    e
}

/// Pump driver+worker turns until quiescent (deterministic; no background loops).
async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        if !drove && !worked {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn activity_workflow_runs_to_completion() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Sum>((), StartOptions { id: "sum-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 13);
}

#[tokio::test]
async fn cold_recovery_completes_identically() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn + one activity, then drop the engine (crash).
    {
        let engine = build(&db);
        engine
            .start_workflow::<Sum>((), StartOptions { id: "sum-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // schedules Add #0
        assert!(engine.process_one_activity().await.unwrap()); // completes Add #0
                                                               // engine dropped here; only the shared `db` connection survives
    }

    // Phase 2: a fresh engine with NO in-memory state cold-replays and finishes.
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("sum-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 13);
}

#[tokio::test]
async fn completion_observer_fires_on_terminal() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let db = Sqlite::open_in_memory().unwrap();
    let mut engine = build(&db);
    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    engine.on_run_completed(move |ev| {
        if matches!(ev.status, ExecStatus::Completed) {
            f.store(true, Ordering::SeqCst);
        }
    });
    engine
        .start_workflow::<Sum>((), StartOptions { id: "sum-3".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    assert!(
        fired.load(Ordering::SeqCst),
        "observer should fire on completion"
    );
}

#[tokio::test]
async fn start_is_idempotent_by_id() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let h1 = engine
        .start_workflow::<Sum>((), StartOptions { id: "dup".into() })
        .await
        .unwrap();
    let h2 = engine
        .start_workflow::<Sum>((), StartOptions { id: "dup".into() })
        .await
        .unwrap();
    assert_eq!(h1.run_id(), h2.run_id(), "same id returns the existing run");
}
