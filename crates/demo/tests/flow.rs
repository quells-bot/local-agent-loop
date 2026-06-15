use std::sync::Arc;

use demo::{Parent, ParentParams, ParentResult, Parse, SumActivity, SumChild};
use engine::{Engine, History, StartOptions, TaskQueue};
use persist::Sqlite;

fn build(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<Parent>();
    e.register_workflow::<SumChild>();
    e.register_activity::<Parse>();
    e.register_activity::<SumActivity>();
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
