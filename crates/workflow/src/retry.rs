use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_ms: u64,
    pub multiplier: u32,
}

impl RetryPolicy {
    pub fn exponential(max_attempts: u32) -> Self {
        Self { max_attempts, initial_ms: 100, multiplier: 2 }
    }

    pub fn none() -> Self {
        Self { max_attempts: 1, initial_ms: 0, multiplier: 1 }
    }

    /// Delay before the given 1-based attempt. Attempt 1 (first try) has no delay.
    pub fn backoff_ms(&self, attempt: u32) -> u64 {
        debug_assert!(self.multiplier >= 1, "RetryPolicy.multiplier must be >= 1");
        if attempt <= 1 {
            return 0;
        }
        self.initial_ms
            .saturating_mul((self.multiplier as u64).saturating_pow(attempt - 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exponential_backoff_schedule() {
        let p = RetryPolicy::exponential(5);
        assert_eq!(p.backoff_ms(1), 0);    // first attempt: immediate
        assert_eq!(p.backoff_ms(2), 100);  // initial
        assert_eq!(p.backoff_ms(3), 200);  // *2
        assert_eq!(p.backoff_ms(4), 400);  // *2
    }

    #[test]
    fn none_means_single_attempt() {
        assert_eq!(RetryPolicy::none().max_attempts, 1);
    }
}
