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
}
