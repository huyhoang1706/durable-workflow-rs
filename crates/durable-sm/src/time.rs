use std::time::Duration;

use chrono::{DateTime, Utc};

/// The latest instant the framework schedules anything at (9999-12-31T23:59:59Z).
/// Later instants overflow `chrono` arithmetic or the backends' timestamp types.
pub fn far_future() -> DateTime<Utc> {
    DateTime::from_timestamp(253_402_300_799, 0).expect("in range")
}

/// `t + d`, clamped to [`far_future`] instead of panicking on overflow. Use it
/// for any delay that comes from user code or a remote peer.
pub fn saturating_add(t: DateTime<Utc>, d: Duration) -> DateTime<Utc> {
    chrono::Duration::from_std(d)
        .ok()
        .and_then(|d| t.checked_add_signed(d))
        .map_or_else(far_future, |sum| sum.min(far_future()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn saturating_add_clamps_instead_of_panicking() {
        let now = Utc::now();
        assert_eq!(
            saturating_add(now, Duration::from_secs(1)),
            now + chrono::Duration::seconds(1)
        );
        assert_eq!(saturating_add(now, Duration::MAX), far_future());
        assert_eq!(
            saturating_add(now, Duration::from_secs(10_000_000_000_000)),
            far_future()
        );
        assert_eq!(
            saturating_add(far_future(), Duration::from_secs(1)),
            far_future()
        );
    }
}
