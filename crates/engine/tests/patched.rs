use std::sync::Arc;

use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
use persist::Sqlite;

// New-code workflow: takes the patched branch, returns 1; old path would return 0.
struct Branch;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for Branch {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "Branch";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        if ctx.patched("v2") {
            Ok(1)
        } else {
            Ok(0)
        }
    }
}

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Branch>();
    e
}

async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        if !drove {
            return Ok(());
        }
    }
}

#[tokio::test]
async fn patched_workflow_runs_and_persists_marker() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Branch>(
            (),
            StartOptions {
                id: "patch-1".into(),
            },
        )
        .await
        .unwrap();
    pump(&engine).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 1, "new execution takes the patched branch");

    // The marker was persisted as a history event.
    let (run_id, status, _) = db.find_execution("patch-1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let kinds: Vec<&'static str> = db
        .read_history(&run_id)
        .await
        .unwrap()
        .iter()
        .map(|s| s.event.kind())
        .collect();
    assert!(
        kinds.contains(&"Patched"),
        "history should contain a Patched marker, got {kinds:?}"
    );
}

#[tokio::test]
async fn patched_cold_recovery_completes_identically() {
    let db = Sqlite::open_in_memory().unwrap();
    // Phase 1: run to completion, then drop the engine.
    {
        let engine = build(&db);
        engine
            .start_workflow::<Branch>(
                (),
                StartOptions {
                    id: "patch-2".into(),
                },
            )
            .await
            .unwrap();
        pump(&engine).await.unwrap();
    }
    // Phase 2: a fresh engine cold-replays the persisted history (marker included) and
    // sees the same result — the marker makes patched() stable on replay.
    let engine2 = build(&db);
    pump(&engine2).await.unwrap();
    let (_, status, result) = db.find_execution("patch-2").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: i64 = serde_json::from_slice(&result.unwrap()).unwrap();
    assert_eq!(out, 1);
}
