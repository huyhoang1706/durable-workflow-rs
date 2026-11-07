use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use durable_spi::Clock;
use parking_lot::Mutex;
use tokio::sync::Notify;

/// A clock that only moves when told to.
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
    advanced: Notify,
}

impl ManualClock {
    pub fn new(start: DateTime<Utc>) -> Arc<Self> {
        Arc::new(Self {
            now: Mutex::new(start),
            advanced: Notify::new(),
        })
    }

    pub fn advance(&self, by: Duration) {
        *self.now.lock() += by;
        self.advanced.notify_waiters();
    }
}

#[async_trait]
impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock()
    }

    async fn sleep_until(&self, t: DateTime<Utc>) {
        loop {
            let notified = self.advanced.notified();
            tokio::pin!(notified);
            // Register before checking so an advance in between is not lost.
            notified.as_mut().enable();
            if self.now() >= t {
                return;
            }
            notified.await;
        }
    }
}
