use std::sync::Arc;

use engine::{Engine, History, StartOptions, TaskQueue};
use persist::Sqlite;

// Activity instance carrying an injected dependency: a constant addend.
struct AddConst {
    addend: i64,
}

#[async_trait::async_trait]
impl activity::Definition for AddConst {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "AddConst";
    async fn run(&self, _c: activity::Context, n: i64) -> Result<i64, activity::Error> {
        Ok(n + self.addend) // uses the injected field
    }
}

// Workflow: calls AddConst(5); the result depends on the injected addend.
struct UseAddConst;

#[async_trait::async_trait(?Send)]
impl workflow::Definition for UseAddConst {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "UseAddConst";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let r = ctx.activity::<AddConst>(5).await?;
        Ok(r)
    }
}

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
async fn injected_dependency_is_used() {
    let db = Sqlite::open_in_memory().unwrap();
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<UseAddConst>();
    e.register_activity(AddConst { addend: 100 }); // inject 100, by value

    let handle = e
        .start_workflow::<UseAddConst>((), StartOptions { id: "di-1".into() })
        .await
        .unwrap();
    pump(&e).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 105, "5 + injected addend 100");
}
