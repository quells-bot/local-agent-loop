// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec: docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use std::sync::Arc;
use std::time::Duration;

use engine::{Engine, ExecStatus, History, StartOptions};
use futures::{select_biased, FutureExt};
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

// join!: two concurrent activity branches, summed. First poll emits BOTH schedule
// commands in one turn; completions are applied one-per-turn (spec §4.1).
struct Pair;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Pair {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Pair";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let (a, b) = futures::join!(ctx.activity::<Add>((1, 2)), ctx.activity::<Add>((10, 20)),);
        Ok(a? + b?)
    }
}

// select_biased!: an activity races a one-day sleep. The activity always wins (the
// timer never reaches its fire_at in the test), and the losing timer branch is just
// dropped — its `timers` row is never consumed (spec §4.3).
struct Pick;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Pick {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Pick";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let act = ctx.activity::<Add>((7, 8)).fuse();
        let nap = ctx.sleep(Duration::from_secs(86_400)).fuse();
        futures::pin_mut!(act, nap);
        let r = select_biased! {
            x = act => x?,
            _ = nap => -1,
        };
        Ok(r)
    }
}

// ctx.spawn: a detached branch runs an activity; main awaits its handle.
struct Detached;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Detached {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Detached";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let ctx2 = ctx.clone();
        let h = ctx.spawn(async move { ctx2.activity::<Add>((3, 4)).await.unwrap() });
        let v = h.await;
        Ok(v)
    }
}

fn build<W: workflow::Definition>(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<W>();
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
async fn join_runs_concurrent_branches() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<Pair>(&db);
    let handle = engine
        .start_workflow::<Pair>(
            (),
            StartOptions {
                id: "pair-1".into(),
            },
        )
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 3 + 30);
}

#[tokio::test]
async fn join_branches_replay_across_cold_recovery() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: start, drive one turn (schedules BOTH activities), run one activity,
    // then crash with the other still pending.
    {
        let engine = build::<Pair>(&db);
        engine
            .start_workflow::<Pair>(
                (),
                StartOptions {
                    id: "pair-2".into(),
                },
            )
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // emits two ScheduleActivity
        assert!(engine.process_one_activity().await.unwrap()); // completes one branch
    }
    // Phase 2: cold-recover and finish.
    let engine2 = build::<Pair>(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("pair-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 33);
}

#[tokio::test]
async fn select_biased_activity_beats_timer() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<Pick>(&db);
    let handle = engine
        .start_workflow::<Pick>(
            (),
            StartOptions {
                id: "pick-1".into(),
            },
        )
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let out: i64 = handle.result().await.unwrap();
    assert_eq!(
        out, 15,
        "the activity branch wins; the day-long timer never fires"
    );
}

#[tokio::test]
async fn select_biased_replays_across_cold_recovery() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: start, drive one turn (schedules the activity AND the timer), run the
    // activity, then crash before the workflow is finally driven.
    {
        let engine = build::<Pick>(&db);
        engine
            .start_workflow::<Pick>(
                (),
                StartOptions {
                    id: "pick-2".into(),
                },
            )
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // ScheduleActivity seq0 + StartTimer seq1
        assert!(engine.process_one_activity().await.unwrap()); // completes the activity
    }
    // Phase 2: cold-recover — the activity result is in history; the biased select
    // re-resolves to the same winner.
    let engine2 = build::<Pick>(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("pick-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 15);
}

#[tokio::test]
async fn spawn_detached_branch_completes() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<Detached>(&db);
    let handle = engine
        .start_workflow::<Detached>(
            (),
            StartOptions {
                id: "spawn-1".into(),
            },
        )
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 7);
}

#[tokio::test]
async fn spawn_replays_across_cold_recovery() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: start, drive one turn (the spawned branch schedules its activity),
    // run the activity, then crash before main observes the handle.
    {
        let engine = build::<Detached>(&db);
        engine
            .start_workflow::<Detached>(
                (),
                StartOptions {
                    id: "spawn-2".into(),
                },
            )
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // spawned branch emits ScheduleActivity seq0
        assert!(engine.process_one_activity().await.unwrap()); // completes it
    }
    // Phase 2: cold-recover; the spawned branch is re-created, polled, resolves, and
    // main's `h.await` completes.
    let engine2 = build::<Detached>(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("spawn-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 7);
}
