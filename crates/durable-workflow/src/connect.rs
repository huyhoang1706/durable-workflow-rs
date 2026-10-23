//! Build a [`Backend`] from connection URLs: Postgres for the store, and
//! Redis (when configured) for the queue, pub/sub, cache and rate limiter.
//!
//! Without a Redis URL the dispatch index falls back to in-memory slots. That
//! is fine for a short-lived tool such as the CLI, and also for a single
//! process: Postgres stays the source of truth and the reconciler rebuilds the
//! queue from `wake_at` on boot.

use durable_core::{Backend, Error};

#[derive(Debug, Clone)]
pub struct BackendConfig {
    pub database_url: String,
    pub redis_url: Option<String>,
    pub namespace: String,
    /// Postgres schema holding the durable tables.
    pub schema: String,
    /// Run migrations while building (safe from every process at boot).
    pub migrate: bool,
}

impl BackendConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            redis_url: None,
            namespace: "default".into(),
            schema: "durable".into(),
            migrate: true,
        }
    }

    /// `DURABLE_DATABASE_URL` (or `DATABASE_URL`), `DURABLE_REDIS_URL` (or
    /// `REDIS_URL`), `DURABLE_NAMESPACE`, `DURABLE_SCHEMA`, `DURABLE_MIGRATE`.
    pub fn from_env() -> Result<Self, Error> {
        let get = |a: &str, b: &str| std::env::var(a).or_else(|_| std::env::var(b)).ok();
        let database_url = get("DURABLE_DATABASE_URL", "DATABASE_URL")
            .ok_or_else(|| Error::Config("set DURABLE_DATABASE_URL (or DATABASE_URL)".into()))?;
        let mut c = Self::new(database_url);
        c.redis_url = get("DURABLE_REDIS_URL", "REDIS_URL");
        if let Ok(ns) = std::env::var("DURABLE_NAMESPACE") {
            c.namespace = ns;
        }
        if let Ok(schema) = std::env::var("DURABLE_SCHEMA") {
            c.schema = schema;
        }
        if let Ok(m) = std::env::var("DURABLE_MIGRATE") {
            c.migrate = !matches!(m.as_str(), "0" | "false" | "no");
        }
        Ok(c)
    }

    pub async fn build(&self) -> Result<Backend, Error> {
        let store = durable_postgres::PostgresStore::connect(&self.database_url)
            .await?
            .schema(&self.schema);
        if self.migrate {
            store.migrate().await?;
        }
        let builder = Backend::builder().namespace(&self.namespace).store(store);

        #[cfg(feature = "redis")]
        if let Some(url) = &self.redis_url {
            let r = durable_redis::connect(url, "dw").await?;
            return builder
                .queue(r.queue)
                .pubsub(r.pubsub)
                .cache(r.cache)
                .rate_limiter(r.rate_limiter)
                .build();
        }
        #[cfg(not(feature = "redis"))]
        if self.redis_url.is_some() {
            return Err(Error::Config(
                "a Redis URL was given but the `redis` feature is off".into(),
            ));
        }

        #[cfg(feature = "memory")]
        return builder
            .queue(durable_memory::MemoryQueue::new())
            .pubsub(durable_memory::MemoryPubSub::new())
            .cache(durable_memory::MemoryCache::new())
            .build();
        #[cfg(not(feature = "memory"))]
        Err(Error::Config(
            "no Redis URL given and the `memory` feature is off".into(),
        ))
    }
}
