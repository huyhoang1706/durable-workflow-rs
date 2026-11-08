//! Conformance suites and test helpers for plugin authors.
//!
//! ```ignore
//! durable_testkit::store_conformance!(async { Arc::new(MyStore::new()) as Arc<dyn Store> });
//! durable_testkit::queue_conformance!(async { Arc::new(MyQueue::new()) as Arc<dyn Queue> });
//! durable_testkit::stream_conformance!(|ttl| async move { StreamHarness::real_time(Arc::new(MyStreams::new(ttl))) });
//! ```
//!
//! [`corpus`] holds the replay conformance corpus every language SDK must pass.

mod clock;
pub mod corpus;
pub mod queue;
pub mod services;
pub mod store;
pub mod streams;

pub use clock::ManualClock;

use chrono::{DateTime, TimeZone, Utc};

/// Fixed base instant so suites are deterministic.
pub fn t0() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0).unwrap()
}

pub fn secs(n: i64) -> chrono::Duration {
    chrono::Duration::seconds(n)
}
