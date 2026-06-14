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
    async fn run(_c: activity::Context, i: (i64, i64)) -> Result<i64, activity::Error> {
        Ok(i.0 + i.1)
    }
}

// Workflow: sleep, then Add(1, 2) == 3. A 0ms timer is due immediately, so the
// timer service fires it on the first pass — the test stays deterministic while
// still exercising the full StartTimer -> TimerStarted -> TimerFired path.
struct Nap;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Nap {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Nap";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        ctx.sleep(std::time::Duration::from_millis(0)).await;
        let a = ctx.activity::<Add>((1, 2)).await?;
        Ok(a)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Nap>();
    e.register_activity::<Add>();
    e
}

/// Pump driver + worker + timer turns until quiescent (deterministic).
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

#[tokio::test]
async fn timer_workflow_runs_to_completion() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Nap>((), StartOptions { id: "nap-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 3);
}

#[tokio::test]
async fn timer_cold_recovery_completes_identically() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn (schedules the timer), then crash.
    {
        let engine = build(&db);
        engine
            .start_workflow::<Nap>((), StartOptions { id: "nap-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // emits StartTimer seq 0
        // engine dropped here; only the shared `db` survives. The timer row and the
        // TimerStarted event are durable; TimerFired has not been written yet.
    }

    // Phase 2: a fresh engine fires the timer and completes by cold replay.
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("nap-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 3);
}
