use activity::Execution;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub execution: Execution,
    pub parent: Option<Execution>,
    pub workflow_type: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_defaults_to_none_for_root() {
        let i = Info {
            execution: Execution {
                workflow_id: "w".into(),
                run_id: "r".into(),
            },
            parent: None,
            workflow_type: "OrderWorkflow".into(),
        };
        assert!(i.parent.is_none());
    }
}
