use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Execution {
    pub workflow_id: String,
    pub run_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_json() {
        let e = Execution { workflow_id: "order-123".into(), run_id: "run-abc".into() };
        let json = serde_json::to_string(&e).unwrap();
        let back: Execution = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }
}
