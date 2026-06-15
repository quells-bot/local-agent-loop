// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use crate::RetryPolicy;
use serde::{Deserialize, Serialize};

/// Issued by workflow futures, drained by the driver each turn (spec §3).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    ScheduleActivity {
        seq: u64,
        activity_type: String,
        input: Vec<u8>,
        retry: RetryPolicy,
    },
    StartTimer {
        seq: u64,
        duration_ms: u64,
    },
    /// Start a child workflow (spec §5.4, §9). Allocates a `seq`; the driver records
    /// it as `Event::ChildScheduled` and creates the child execution.
    StartChild {
        seq: u64,
        workflow_type: String,
        input: Vec<u8>,
    },
    /// Request to record a change-version marker (spec §14, the `GetVersion` analog).
    /// Carries NO `seq` — it is divergence-exempt like an inbound event; the driver
    /// records it as `Event::Patched` deduped by `change_id`.
    RecordPatch {
        change_id: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_patch_round_trips_through_json() {
        let p = Command::RecordPatch {
            change_id: "ship-v2".into(),
        };
        let back: Command = serde_json::from_str(&serde_json::to_string(&p).unwrap()).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn round_trips_through_json() {
        let c = Command::ScheduleActivity {
            seq: 1,
            activity_type: "Charge".into(),
            input: b"{}".to_vec(),
            retry: RetryPolicy::exponential(3),
        };
        let back: Command = serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);

        let t = Command::StartTimer {
            seq: 2,
            duration_ms: 500,
        };
        let back: Command = serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
        assert_eq!(t, back);

        let child = Command::StartChild {
            seq: 3,
            workflow_type: "Ship".into(),
            input: b"{}".to_vec(),
        };
        let back: Command = serde_json::from_str(&serde_json::to_string(&child).unwrap()).unwrap();
        assert_eq!(child, back);
    }
}
