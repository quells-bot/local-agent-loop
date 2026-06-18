use std::sync::Arc;

use engine::{Engine, History, StartOptions, TaskQueue};
use persist::Sqlite;

// The "real" activity: identity.
struct RealEcho;

#[async_trait::async_trait]
impl activity::Definition for RealEcho {
    type Input = i64;
    type Output = i64;
    const TYPE: &'static str = "Echo";
    async fn run(&self, _c: activity::Context, n: i64) -> Result<i64, activity::Error> {
        Ok(n)
    }
}

// A fake claiming the SAME type, with the key linked at compile time (spec §4
// discipline) and the real public Input/Output types reused. Returns a canned value.
struct FakeEcho;

#[async_trait::async_trait]
impl activity::Definition for FakeEcho {
    type Input = i64; // reuse the real Input/Output (here primitives) for compatibility
    type Output = i64;
    const TYPE: &'static str = RealEcho::TYPE; // compile-time-linked registry key
    async fn run(&self, _c: activity::Context, _n: i64) -> Result<i64, activity::Error> {
        Ok(999) // canned reply
    }
}

// Workflow references the REAL activity type; whatever is registered under
// "Echo" actually runs.
struct CallEcho;

#[async_trait::async_trait(?Send)]
impl workflow::Definition for CallEcho {
    type Input = ();
    type Output = i64;
    const TYPE: &'static str = "CallEcho";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<i64, workflow::Error> {
        let r = ctx.activity::<RealEcho>(7).await?;
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
async fn fake_registered_under_real_type_overrides_it() {
    let db = Sqlite::open_in_memory().unwrap();
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<CallEcho>();
    e.register_activity(RealEcho); // real first
    e.register_activity(FakeEcho); // fake registered last, under the same TYPE -> wins

    let handle = e
        .start_workflow::<CallEcho>((), StartOptions { id: "echo-1".into() })
        .await
        .unwrap();
    pump(&e).await.unwrap();

    let out: i64 = handle.result().await.unwrap();
    assert_eq!(out, 999, "the fake registered under the real type runs");
}
