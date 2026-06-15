// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use crate::{Context, Error};
use serde::{de::DeserializeOwned, Serialize};

// Workflow futures hold Rc/RefCell (single-threaded decision loop, spec §5.1),
// so they are NOT Send — hence `?Send`. Activities are the Send half.
#[async_trait::async_trait(?Send)]
pub trait Definition: 'static {
    type Input: Serialize + DeserializeOwned + 'static;
    type Output: Serialize + DeserializeOwned + 'static;
    const TYPE: &'static str;

    async fn run(ctx: Context, input: Self::Input) -> Result<Self::Output, Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Info;
    use activity::Execution;

    struct Echo;

    #[async_trait::async_trait(?Send)]
    impl Definition for Echo {
        type Input = String;
        type Output = String;
        const TYPE: &'static str = "Echo";
        async fn run(_ctx: Context, input: String) -> Result<String, Error> {
            Ok(input)
        }
    }

    #[tokio::test]
    async fn sample_workflow_runs() {
        let ctx = Context::new(Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: Echo::TYPE.into(),
        });
        assert_eq!(Echo::run(ctx, "hi".into()).await.unwrap(), "hi");
    }
}
