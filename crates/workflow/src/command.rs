use crate::RetryPolicy;
use serde::{Deserialize, Serialize};

/// Issued by workflow futures, drained by the driver each turn (spec §3).
/// Pass 4 adds StartChild.
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let t = Command::StartTimer { seq: 2, duration_ms: 500 };
        let back: Command = serde_json::from_str(&serde_json::to_string(&t).unwrap()).unwrap();
        assert_eq!(t, back);
    }
}
