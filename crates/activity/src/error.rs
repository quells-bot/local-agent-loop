use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct Error {
    pub message: String,
    pub non_retryable: bool,
}

impl Error {
    pub fn retryable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: false,
        }
    }
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            non_retryable: true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ctors_set_retryability() {
        assert!(!Error::retryable("boom").non_retryable);
        assert!(Error::fatal("boom").non_retryable);
    }

    #[test]
    fn displays_message() {
        assert_eq!(Error::fatal("nope").to_string(), "nope");
    }

    #[test]
    fn round_trips_through_json() {
        let e = Error::fatal("x");
        let back: Error = serde_json::from_str(&serde_json::to_string(&e).unwrap()).unwrap();
        assert_eq!(e, back);
    }
}
