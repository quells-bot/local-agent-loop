use std::sync::Arc;

use engine::{Engine, ExecStatus, History, StartOptions};
use persist::Sqlite;

// Activity: Double(n) -> n * 2.
struct Double;
#[async_trait::async_trait]
impl activity::Definition for Double {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Double";
    async fn run(_c: activity::Context, n: i64) -> Result<i64, activity::Error> {
        Ok(n * 2)
    }
}

// Child: runs one activity (Double) so cold recovery of the child is exercised too.
struct Child;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Child {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Child";
    async fn run(ctx: workflow::Context, n: i64) -> Result<i64, workflow::Error> {
        let v = ctx.activity::<Double>(n).await?;
        Ok(v)
    }
}

// Parent: starts Child(input), returns child_output + 1.
struct Parent;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Parent {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Parent";
    async fn run(ctx: workflow::Context, n: i64) -> Result<i64, workflow::Error> {
        let v = ctx.child_workflow::<Child>(n).await?;
        Ok(v + 1)
    }
}

// Child that returns its parent's workflow_id, to prove info.parent is populated.
struct ParentIdChild;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for ParentIdChild {
    type Input = ();
    type Output = String;
    const TYPE: &'static str = "ParentIdChild";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<String, workflow::Error> {
        let parent = ctx
            .info()
            .parent
            .as_ref()
            .map(|p| p.workflow_id.clone())
            .unwrap_or_else(|| "<none>".into());
        Ok(parent)
    }
}
struct ParentOfIdChild;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for ParentOfIdChild {
    type Input = ();
    type Output = String;
    const TYPE: &'static str = "ParentOfIdChild";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<String, workflow::Error> {
        let id = ctx.child_workflow::<ParentIdChild>(()).await?;
        Ok(id)
    }
}

// Child that always fails; parent propagates via `?`.
struct FailChild;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for FailChild {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "FailChild";
    async fn run(_ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        Err(workflow::Error::new("child failed on purpose"))
    }
}
struct ParentOfFail;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for ParentOfFail {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "ParentOfFail";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let v = ctx.child_workflow::<FailChild>(()).await?;
        Ok(v)
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Parent>();
    e.register_workflow::<Child>();
    e.register_workflow::<ParentOfIdChild>();
    e.register_workflow::<ParentIdChild>();
    e.register_workflow::<ParentOfFail>();
    e.register_workflow::<FailChild>();
    e.register_activity::<Double>();
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
async fn parent_completes_when_child_does() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(5, StartOptions { id: "p-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 11, "child Double(5)=10, parent adds 1");

    // The child execution exists, completed, and links back to the parent.
    let (child_run, status, _) = db.find_execution("p-1::child::0").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let meta = db.load_run(&child_run).await.unwrap().unwrap();
    assert_eq!(meta.parent_seq, Some(0));
}

#[tokio::test]
async fn child_info_parent_is_populated() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<ParentOfIdChild>((), StartOptions { id: "pid-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: String = handle.result().await.unwrap();
    assert_eq!(
        out, "pid-1",
        "the child observed its parent's workflow_id via ctx.info().parent"
    );
}

#[tokio::test]
async fn child_failure_propagates_to_parent() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<ParentOfFail>((), StartOptions { id: "pf-1".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    // The child failed; the parent's `?` turned that into a workflow failure.
    let (_, child_status, _) = db.find_execution("pf-1::child::0").await.unwrap().unwrap();
    assert_eq!(child_status, ExecStatus::Failed);
    let (_, parent_status, _) = db.find_execution("pf-1").await.unwrap().unwrap();
    assert_eq!(parent_status, ExecStatus::Failed);
}

#[tokio::test]
async fn cold_recovery_completes_parent_and_child() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start the parent, drive a few turns (parent starts the child, child
    // schedules its activity), then drop the engine — simulating a crash mid-flight.
    {
        let engine = build(&db);
        engine
            .start_workflow::<Parent>(5, StartOptions { id: "p-2".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // parent: StartChild, child created
        assert!(engine.process_one_runnable().await.unwrap()); // child: schedules Double activity
                                                               // engine dropped here; only the shared `db` survives.
    }

    // Phase 2: a fresh engine with no in-memory state cold-replays both runs and
    // finishes — child completes, notifies the parent, parent completes (spec §13).
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("p-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 11);
}
