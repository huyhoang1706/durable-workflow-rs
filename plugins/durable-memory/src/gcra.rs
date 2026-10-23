use std::time::Duration;

use chrono::{DateTime, Utc};

/// Generic cell rate algorithm: `limit` events per `period`, allowing `burst`
/// back-to-back. Returns the new theoretical arrival time, or the instant to
/// retry at.
pub fn gcra(
    tat: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
    limit: u32,
    period: Duration,
    burst: u32,
) -> Result<DateTime<Utc>, DateTime<Utc>> {
    let period = chrono::Duration::from_std(period).unwrap_or(chrono::Duration::MAX);
    let emission = period / limit.max(1) as i32;
    let tolerance = emission * (burst.max(1) as i32 - 1);
    let tat = tat.map_or(now, |t| t.max(now));
    let allow_at = tat - tolerance;
    if now >= allow_at {
        Ok(tat + emission)
    } else {
        Err(allow_at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(s: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(1_900_000_000 + s, 0).unwrap()
    }

    #[test]
    fn allows_a_burst_then_spaces_out() {
        let p = Duration::from_secs(10);
        let mut tat = None;
        for _ in 0..2 {
            tat = Some(gcra(tat, t(0), 2, p, 2).unwrap());
        }
        assert_eq!(gcra(tat, t(0), 2, p, 2), Err(t(5)));
        assert!(gcra(tat, t(5), 2, p, 2).is_ok());
    }
}
