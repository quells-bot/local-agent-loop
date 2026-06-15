use crate::Info;

#[derive(Clone)]
pub struct Context {
    info: Info,
}

impl Context {
    pub fn new(info: Info) -> Self {
        Self { info }
    }

    pub fn info(&self) -> &Info {
        &self.info
    }

    /// Stable across retries/redeliveries: "{run_id}:{activity_id}" (spec §8).
    pub fn idempotency_key(&self) -> String {
        format!("{}:{}", self.info.execution.run_id, self.info.activity_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Execution;

    fn ctx() -> Context {
        Context::new(Info {
            execution: Execution {
                workflow_id: "order-1".into(),
                run_id: "run-9".into(),
            },
            activity_id: "3".into(),
            activity_type: "Charge".into(),
            attempt: 2,
        })
    }

    #[test]
    fn idempotency_key_is_run_id_colon_activity_id() {
        assert_eq!(ctx().idempotency_key(), "run-9:3");
    }

    #[test]
    fn info_is_accessible() {
        assert_eq!(ctx().info().attempt, 2);
    }
}
