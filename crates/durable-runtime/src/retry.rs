use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total attempts, including the first.
    pub max_attempts: u32,
    pub initial_delay: Duration,
    pub factor: u32,
    pub max_delay: Duration,
}

impl RetryPolicy {
    pub fn none() -> Self {
        Self::fixed(1, Duration::ZERO)
    }

    pub fn exponential(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            initial_delay: Duration::from_secs(1),
            factor: 2,
            max_delay: Duration::from_secs(300),
        }
    }

    pub fn fixed(max_attempts: u32, delay: Duration) -> Self {
        Self {
            max_attempts,
            initial_delay: delay,
            factor: 1,
            max_delay: delay,
        }
    }

    pub fn initial_delay(mut self, d: Duration) -> Self {
        self.initial_delay = d;
        self
    }

    pub fn max_delay(mut self, d: Duration) -> Self {
        self.max_delay = d;
        self
    }

    /// Whether a failure of (1-based) `attempt` may be retried.
    pub fn allows_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }

    /// Backoff after the (1-based) `attempt` failed.
    pub fn delay_after(&self, attempt: u32) -> Duration {
        let exp = attempt.saturating_sub(1);
        let mult = (self.factor as u128).checked_pow(exp).unwrap_or(u128::MAX);
        let nanos = self.initial_delay.as_nanos().saturating_mul(mult);
        let capped = nanos.min(self.max_delay.as_nanos());
        Duration::from_nanos(capped.min(u64::MAX as u128) as u64)
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(3)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const S: fn(u64) -> Duration = Duration::from_secs;

    #[test]
    fn exponential_doubles_from_one_second() {
        let p = RetryPolicy::exponential(5);
        let d: Vec<_> = (1..=4).map(|a| p.delay_after(a)).collect();
        assert_eq!(d, [S(1), S(2), S(4), S(8)]);
    }

    #[test]
    fn delay_is_capped() {
        let p = RetryPolicy::exponential(30).max_delay(S(10));
        assert_eq!(p.delay_after(20), S(10));
        // Must not overflow for huge attempt numbers.
        assert_eq!(p.delay_after(u32::MAX), S(10));
    }

    #[test]
    fn attempts_are_bounded() {
        let p = RetryPolicy::exponential(3);
        assert!(p.allows_retry(1) && p.allows_retry(2));
        assert!(!p.allows_retry(3));
        assert!(!RetryPolicy::none().allows_retry(1));
    }

    #[test]
    fn fixed_delay_is_constant() {
        let p = RetryPolicy::fixed(4, S(7));
        assert_eq!((p.delay_after(1), p.delay_after(3)), (S(7), S(7)));
    }
}
