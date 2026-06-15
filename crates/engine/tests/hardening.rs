use std::sync::atomic::{AtomicBool, Ordering};
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

// Two structs sharing one workflow TYPE but emitting different command streams.
// V1 schedules Add(1, 2) at seq 0. V2 (registered on the "restart") schedules
// Add(5, 5) at the same seq 0 -> the recorded ActivityScheduled at seq 0 had a
// different input, so cold replay flags a genuine nondeterminism divergence.
struct SumV1;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for SumV1 {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "VersionedSum";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let a = ctx.activity::<Add>((1, 2)).await?;
        Ok(a)
    }
}
struct SumV2;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for SumV2 {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "VersionedSum";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        // Schedules Add at seq 0 but with a DIFFERENT input than history recorded
        // for V1 -> divergence at seq 0 (history: Add(1,2), workflow: Add(5,5)).
        let a = ctx.activity::<Add>((5, 5)).await?;
        Ok(a)
    }
}

fn engine_with<W: workflow::Definition>(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<W>();
    e.register_activity::<Add>();
    e
}

async fn lease_one(db: &Sqlite) -> engine::ActivityLease {
    use engine::TaskQueue;
    db.lease_activity().await.unwrap().expect("a task is due")
}

#[tokio::test]
async fn unregistered_workflow_is_dead_lettered() {
    let db = Sqlite::open_in_memory().unwrap();
    // Start a run of type "VersionedSum" but build an engine that registers NO
    // workflow of that type.
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut engine = Engine::new(h, q);
    engine.register_activity::<Add>(); // deliberately no register_workflow

    let fired = Arc::new(AtomicBool::new(false));
    let f = fired.clone();
    engine.on_run_completed(move |ev| {
        if matches!(ev.status, ExecStatus::Failed) {
            f.store(true, Ordering::SeqCst);
        }
    });

    engine
        .start_workflow::<SumV1>(
            (),
            StartOptions {
                id: "dead-1".into(),
            },
        )
        .await
        .unwrap();

    // One driver turn dead-letters instead of erroring/spinning.
    assert!(engine.process_one_runnable().await.unwrap());
    // The run is no longer runnable (would-be spin is gone).
    assert!(!engine.process_one_runnable().await.unwrap());

    let (_, status, _) = db.find_execution("dead-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Failed);
    assert!(
        fired.load(Ordering::SeqCst),
        "observer fires on dead-letter"
    );
}

#[tokio::test]
async fn nondeterminism_is_dead_lettered_not_spun() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: run V1 one turn + one activity so history records ActivityScheduled
    // (Add(1,2)) and ActivityCompleted at seq 0.
    {
        let engine = engine_with::<SumV1>(&db);
        engine
            .start_workflow::<SumV1>((), StartOptions { id: "nd-1".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // schedules Add seq 0
        assert!(engine.process_one_activity().await.unwrap()); // completes Add seq 0
    }

    // Phase 2: "restart" with V2 registered under the same TYPE. Cold replay emits
    // a schedule at seq 0 with a different input than history recorded ->
    // divergence -> dead-letter.
    let engine2 = engine_with::<SumV2>(&db);
    assert!(engine2.process_one_runnable().await.unwrap()); // dead-letters
    assert!(
        !engine2.process_one_runnable().await.unwrap(),
        "no spin: run cleared from runnable"
    );

    let (_, status, _) = db.find_execution("nd-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Failed);
}

#[tokio::test]
async fn crashed_activity_lease_is_reclaimed_and_completes() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = engine_with::<SumV1>(&db);
    engine
        .start_workflow::<SumV1>(
            (),
            StartOptions {
                id: "lease-1".into(),
            },
        )
        .await
        .unwrap();
    assert!(engine.process_one_runnable().await.unwrap()); // schedules Add seq 0

    // Simulate a worker that leased the task and then crashed: lease it, do NOT
    // complete it, and force its lease to expire.
    let lease = lease_one(&db).await;
    db.expire_lease_for_test(&lease.run_id, lease.seq).unwrap();

    // Sweep reclaims it; a fresh worker run then completes the workflow.
    assert_eq!(engine.reclaim_expired_activities().await.unwrap(), 1);
    assert!(engine.process_one_activity().await.unwrap()); // re-leases + runs Add
    assert!(engine.process_one_runnable().await.unwrap()); // drives to completion

    let (_, status, result) = db.find_execution("lease-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 3);
}
