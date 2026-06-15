// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use crate::RetryPolicy;
use serde::{Deserialize, Serialize};

/// One row of history (spec §11). Pass 3 adds SignalReceived;
/// Pass 4 ChildCompleted; WorkflowCancelRequested is reserved.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    WorkflowStarted {
        input: Vec<u8>,
    },
    ActivityScheduled {
        seq: u64,
        activity_type: String,
        input: Vec<u8>,
        retry: RetryPolicy,
    },
    ActivityCompleted {
        seq: u64,
        output: Vec<u8>,
    },
    ActivityFailed {
        seq: u64,
        error: activity::Error,
    },
    TimerStarted {
        seq: u64,
        duration_ms: u64,
    },
    TimerFired {
        seq: u64,
    },
    /// Inbound event (spec §6): externally-injected, carries NO `seq`. Its payload
    /// is recorded once and replayed in `event_id` order (Invariant 10).
    SignalReceived {
        name: String,
        payload: Vec<u8>,
    },
    /// Parent-side echo that a child workflow was started (spec §5.4). The analog of
    /// `ActivityScheduled` / `TimerStarted`: it carries the command's `seq` for the
    /// divergence check and tells replay this child is already in flight.
    ChildScheduled {
        seq: u64,
        workflow_type: String,
        input: Vec<u8>,
    },
    /// A child workflow reached a terminal status; written into the PARENT's history
    /// (spec §5.4). `seq` is the parent's `StartChild` command seq.
    ChildCompleted {
        seq: u64,
        result: crate::result::ChildResult,
    },
    /// Change-version marker (spec §14). Written the first time new code runs a
    /// `ctx.patched(id)` path; carries NO `seq` and is divergence-exempt (Invariant 9),
    /// like `SignalReceived`. Seeded back into `ctx` on replay so `patched` is stable.
    Patched {
        change_id: String,
    },
}

impl Event {
    /// Discriminant string stored in `history.kind`.
    pub fn kind(&self) -> &'static str {
        match self {
            Event::WorkflowStarted { .. } => "WorkflowStarted",
            Event::ActivityScheduled { .. } => "ActivityScheduled",
            Event::ActivityCompleted { .. } => "ActivityCompleted",
            Event::ActivityFailed { .. } => "ActivityFailed",
            Event::TimerStarted { .. } => "TimerStarted",
            Event::TimerFired { .. } => "TimerFired",
            Event::SignalReceived { .. } => "SignalReceived",
            Event::ChildScheduled { .. } => "ChildScheduled",
            Event::ChildCompleted { .. } => "ChildCompleted",
            Event::Patched { .. } => "Patched",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn patched_kind_and_round_trip() {
        let e = Event::Patched {
            change_id: "ship-v2".into(),
        };
        assert_eq!(e.kind(), "Patched");
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn kind_matches_variant() {
        assert_eq!(
            Event::WorkflowStarted { input: vec![] }.kind(),
            "WorkflowStarted"
        );
        assert_eq!(
            Event::ActivityCompleted {
                seq: 1,
                output: vec![]
            }
            .kind(),
            "ActivityCompleted"
        );
        assert_eq!(
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "T".into(),
                input: vec![],
                retry: RetryPolicy::none(),
            }
            .kind(),
            "ActivityScheduled"
        );
        assert_eq!(
            Event::ActivityFailed {
                seq: 3,
                error: activity::Error::fatal("x")
            }
            .kind(),
            "ActivityFailed"
        );
        assert_eq!(
            Event::TimerStarted {
                seq: 0,
                duration_ms: 500
            }
            .kind(),
            "TimerStarted"
        );
        assert_eq!(Event::TimerFired { seq: 0 }.kind(), "TimerFired");
        assert_eq!(
            Event::SignalReceived {
                name: "x".into(),
                payload: vec![]
            }
            .kind(),
            "SignalReceived"
        );
        assert_eq!(
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "Ship".into(),
                input: vec![],
            }
            .kind(),
            "ChildScheduled"
        );
        assert_eq!(
            Event::ChildCompleted {
                seq: 0,
                result: crate::result::ChildResult::Completed(vec![]),
            }
            .kind(),
            "ChildCompleted"
        );
    }

    #[test]
    fn round_trips_through_json() {
        let e = Event::ActivityFailed {
            seq: 2,
            error: activity::Error::fatal("x"),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn signal_received_round_trips_through_json() {
        let e = Event::SignalReceived {
            name: "approve".into(),
            payload: b"true".to_vec(),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn child_events_round_trip_through_json() {
        let s = Event::ChildScheduled {
            seq: 2,
            workflow_type: "Ship".into(),
            input: b"[1]".to_vec(),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(s, back);

        let c = Event::ChildCompleted {
            seq: 2,
            result: crate::result::ChildResult::Failed(crate::Error::new("nope")),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
    }

    #[test]
    fn activity_scheduled_round_trips_with_nested_retry() {
        let e = Event::ActivityScheduled {
            seq: 1,
            activity_type: "Add".into(),
            input: b"[1,2]".to_vec(),
            retry: RetryPolicy::exponential(3),
        };
        let back: Event = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
}
