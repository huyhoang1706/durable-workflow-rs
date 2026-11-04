use async_trait::async_trait;
use chrono::{DateTime, Utc};

#[async_trait]
pub trait Clock: Send + Sync + 'static {
    fn now(&self) -> DateTime<Utc>;
    /// Resolve once `now() >= t`.
    async fn sleep_until(&self, t: DateTime<Utc>);
}

pub use durable_sm::{far_future, saturating_add};

#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }

    async fn sleep_until(&self, t: DateTime<Utc>) {
        if let Ok(d) = (t - Utc::now()).to_std() {
            tokio::time::sleep(d).await;
        }
    }
}
