use crate::Info;

/// Workflow-side context. Minimal in 1a; replay machinery added in 1b.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::Execution;

    #[test]
    fn exposes_info() {
        let ctx = Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            parent: None,
            workflow_type: "OrderWorkflow".into(),
        });
        assert_eq!(ctx.info().workflow_type, "OrderWorkflow");
    }
}
