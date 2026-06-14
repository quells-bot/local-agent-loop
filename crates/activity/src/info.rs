use crate::Execution;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Info {
    pub execution: Execution,
    pub activity_id: String,
    pub activity_type: String,
    pub attempt: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holds_fields() {
        let i = Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            activity_id: "7".into(),
            activity_type: "Charge".into(),
            attempt: 1,
        };
        assert_eq!(i.activity_type, "Charge");
        assert_eq!(i.execution.run_id, "r");
    }
}
