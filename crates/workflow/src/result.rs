use serde::{Deserialize, Serialize};

/// A child workflow's terminal outcome, recorded once in the parent's history as the
/// payload of `Event::ChildCompleted` (spec §5.4). Mirrors `CommandResult`'s shape:
/// success carries the child's JSON-encoded output, failure carries the child's
/// `workflow::Error`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChildResult {
    Completed(Vec<u8>),
    Failed(crate::Error),
}

impl From<ChildResult> for Result<Vec<u8>, crate::Error> {
    fn from(r: ChildResult) -> Self {
        match r {
            ChildResult::Completed(output) => Ok(output),
            ChildResult::Failed(error) => Err(error),
        }
    }
}

/// The recorded outcome the driver applies into `ContextInner.results` (spec §3).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandResult {
    ActivityCompleted(Vec<u8>),
    ActivityFailed(activity::Error),
}

impl From<CommandResult> for Result<Vec<u8>, activity::Error> {
    fn from(r: CommandResult) -> Self {
        match r {
            CommandResult::ActivityCompleted(output) => Ok(output),
            CommandResult::ActivityFailed(error) => Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_converts_to_ok() {
        let r: Result<Vec<u8>, activity::Error> =
            CommandResult::ActivityCompleted(b"hi".to_vec()).into();
        assert_eq!(r.unwrap(), b"hi");
    }

    #[test]
    fn failed_converts_to_err() {
        let r: Result<Vec<u8>, activity::Error> =
            CommandResult::ActivityFailed(activity::Error::fatal("boom")).into();
        assert_eq!(r.unwrap_err().message, "boom");
    }

    #[test]
    fn child_completed_converts_to_ok_and_failed_to_err() {
        let ok: Result<Vec<u8>, crate::Error> =
            ChildResult::Completed(b"hi".to_vec()).into();
        assert_eq!(ok.unwrap(), b"hi");

        let err: Result<Vec<u8>, crate::Error> =
            ChildResult::Failed(crate::Error::new("boom")).into();
        assert_eq!(err.unwrap_err().message, "boom");
    }

    #[test]
    fn child_result_round_trips_through_json() {
        let c = ChildResult::Completed(b"42".to_vec());
        let back: ChildResult =
            serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();
        assert_eq!(c, back);
    }
}
