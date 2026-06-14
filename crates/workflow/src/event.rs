use crate::RetryPolicy;
use serde::{Deserialize, Serialize};

/// One row of history (spec §11). Pass 2 adds TimerFired; Pass 3 SignalReceived;
/// Pass 4 ChildCompleted; WorkflowCancelRequested is reserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    WorkflowStarted { input: Vec<u8> },
    ActivityScheduled { seq: u64, activity_type: String, input: Vec<u8>, retry: RetryPolicy },
    ActivityCompleted { seq: u64, output: Vec<u8> },
    ActivityFailed { seq: u64, error: activity::Error },
}

impl Event {
    /// Discriminant string stored in `history.kind`.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::WorkflowStarted { .. } => "WorkflowStarted",
            Event::ActivityScheduled { .. } => "ActivityScheduled",
            Event::ActivityCompleted { .. } => "ActivityCompleted",
            Event::ActivityFailed { .. } => "ActivityFailed",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kind_matches_variant() {
        assert_eq!(Event::WorkflowStarted { input: vec![] }.kind(), "WorkflowStarted");
        assert_eq!(
            Event::ActivityCompleted { seq: 1, output: vec![] }.kind(),
            "ActivityCompleted"
        );
    }

    #[test]
    fn round_trips_through_json() {
        let e = Event::ActivityFailed { seq: 2, error: activity::Error::fatal("x") };
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
}
