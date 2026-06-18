use crate::{Context, Error};
use serde::{de::DeserializeOwned, Serialize};

// Activities run on the parallel worker pool, so their futures must be Send,
// and the shared instance (with its injected deps) must be Send + Sync.
#[async_trait::async_trait]
pub trait Definition: Send + Sync + 'static {
    type Input: Serialize + DeserializeOwned + Send + 'static;
    type Output: Serialize + DeserializeOwned + Send + 'static;
    const TYPE: &'static str;

    async fn run(&self, ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Execution, Info};

    struct Add;

    #[async_trait::async_trait]
    impl Definition for Add {
        type Input = (i64, i64);
        type Output = i64;
        const TYPE: &'static str = "Add";
        async fn run(&self, _ctx: Context, input: (i64, i64)) -> Result<i64, Error> {
            Ok(input.0 + input.1)
        }
    }

    #[tokio::test]
    async fn sample_activity_runs() {
        let ctx = Context::new(Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            activity_id: "1".into(),
            activity_type: Add::TYPE.into(),
            attempt: 1,
        });
        assert_eq!(Add.run(ctx, (2, 3)).await.unwrap(), 5);
        assert_eq!(Add::TYPE, "Add");
    }
}
