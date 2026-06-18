use std::sync::Arc;

use demo::{Parent, ParentParams, ParentResult, Parse, SumActivity, SumChild};
use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
use persist::Sqlite;

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Parent>();
    e.register_workflow::<SumChild>();
    e.register_activity(Parse);
    e.register_activity(SumActivity);
    e
}

/// Pump driver + worker turns until quiescent (deterministic; no background loops).
async fn pump(engine: &Engine) {
    loop {
        let drove = engine.process_one_runnable().await.unwrap();
        let worked = engine.process_one_activity().await.unwrap();
        if !drove && !worked {
            return;
        }
    }
}

#[tokio::test]
async fn sums_space_separated_integers() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(
            ParentParams { text: "1 2 3".into() },
            StartOptions { id: "calc-1".into() },
        )
        .await
        .unwrap();

    pump(&engine).await;

    let out: ParentResult = handle.result().await.unwrap();
    assert_eq!(out, ParentResult { total: 6 });
}

#[tokio::test]
async fn empty_input_sums_to_zero() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(
            ParentParams { text: "".into() },
            StartOptions { id: "calc-empty".into() },
        )
        .await
        .unwrap();
    pump(&engine).await;
    let out: ParentResult = handle.result().await.unwrap();
    assert_eq!(out, ParentResult { total: 0 });
}

#[tokio::test]
async fn negative_integers_sum() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    let handle = engine
        .start_workflow::<Parent>(
            ParentParams { text: "-5 10".into() },
            StartOptions { id: "calc-neg".into() },
        )
        .await
        .unwrap();
    pump(&engine).await;
    let out: ParentResult = handle.result().await.unwrap();
    assert_eq!(out, ParentResult { total: 5 });
}

#[tokio::test]
async fn parse_failure_fails_the_workflow_with_a_message() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build(&db);
    engine
        .start_workflow::<Parent>(
            ParentParams { text: "1 two 3".into() },
            StartOptions { id: "calc-err".into() },
        )
        .await
        .unwrap();
    pump(&engine).await;

    let (_run_id, status, result) = db.find_execution("calc-err").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Failed);
    let err: workflow::Error = serde_json::from_slice(&result.unwrap()).unwrap();
    assert!(err.message.contains("two"), "got: {}", err.message);
}
