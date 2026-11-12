//! Build a [`Backend`] from connection URLs: Postgres for the store, and
//! Redis (when configured) for the queue, pub/sub, cache, rate limiter and
//! transient user streams.
//!
//! Without a Redis URL the dispatch index and the streams fall back to
//! in-memory slots. That is fine for a short-lived tool such as the CLI, and
//! also for a single process: Postgres stays the source of truth and the
//! reconciler rebuilds the queue from `wake_at` on boot. Streams written in
//! one process are only visible in that process; use Redis to tail them
//! from elsewhere.

use std::time::Duration;

use durable_spi::{Backend, Error};

#[derive(Clone)]
pub struct BackendConfig {
    pub database_url: String,
    pub redis_url: Option<String>,
    pub namespace: String,
    /// Postgres schema holding the durable tables.
    pub schema: String,
    /// Run migrations while building (safe from every process at boot).
    pub migrate: bool,
    /// How long user streams are kept after their last chunk (default 24 h).
    pub stream_ttl: Option<Duration>,
    /// How long to wait for a Postgres connection, at boot and later from the
    /// pool (default 30 s).
    pub connect_timeout: Option<Duration>,
}

impl std::fmt::Debug for BackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BackendConfig")
            .field("database_url", &redact_url(&self.database_url))
            .field("redis_url", &self.redis_url.as_deref().map(redact_url))
            .field("namespace", &self.namespace)
            .field("schema", &self.schema)
            .field("migrate", &self.migrate)
            .field("stream_ttl", &self.stream_ttl)
            .field("connect_timeout", &self.connect_timeout)
            .finish()
    }
}

impl BackendConfig {
    pub fn new(database_url: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            redis_url: None,
            namespace: "default".into(),
            schema: "durable".into(),
            migrate: true,
            stream_ttl: None,
            connect_timeout: None,
        }
    }

    /// `DURABLE_DATABASE_URL` (or `DATABASE_URL`), `DURABLE_REDIS_URL` (or
    /// `REDIS_URL`), `DURABLE_NAMESPACE`, `DURABLE_SCHEMA`, `DURABLE_MIGRATE`,
    /// `DURABLE_STREAM_TTL_SECS`.
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
        if let Ok(v) = std::env::var("DURABLE_STREAM_TTL_SECS") {
            let secs: u64 = v.trim().parse().ok().filter(|s| *s > 0).ok_or_else(|| {
                Error::Config(format!(
                    "DURABLE_STREAM_TTL_SECS=`{v}` must be a positive number of seconds"
                ))
            })?;
            c.stream_ttl = Some(Duration::from_secs(secs));
        }
        Ok(c)
    }

    pub async fn build(&self) -> Result<Backend, Error> {
        let store = match self.connect_timeout {
            Some(t) => {
                durable_postgres::PostgresStore::connect_with_timeout(&self.database_url, t).await?
            }
            None => durable_postgres::PostgresStore::connect(&self.database_url).await?,
        }
        .schema(&self.schema);
        if self.migrate {
            store.migrate().await?;
        }
        let builder = Backend::builder().namespace(&self.namespace).store(store);

        #[cfg(feature = "redis")]
        if let Some(url) = &self.redis_url {
            let r = durable_redis::connect(url, "dw").await?;
            let streams = match self.stream_ttl {
                Some(ttl) => r.streams.ttl(ttl),
                None => r.streams,
            };
            return builder
                .queue(r.queue)
                .pubsub(r.pubsub)
                .cache(r.cache)
                .rate_limiter(r.rate_limiter)
                .streams(streams)
                .build();
        }
        #[cfg(not(feature = "redis"))]
        if self.redis_url.is_some() {
            return Err(Error::Config(
                "a Redis URL was given but the `redis` feature is off".into(),
            ));
        }

        #[cfg(feature = "memory")]
        return self.with_memory_slots(builder);
        #[cfg(not(feature = "memory"))]
        Err(Error::Config(
            "no Redis URL given and the `memory` feature is off".into(),
        ))
    }
}

impl BackendConfig {
    /// Queue, pub/sub, cache and streams in this process.
    #[cfg(feature = "memory")]
    fn with_memory_slots(&self, builder: durable_spi::BackendBuilder) -> Result<Backend, Error> {
        let mut streams =
            durable_memory::MemoryStreams::new(std::sync::Arc::new(durable_spi::SystemClock));
        if let Some(ttl) = self.stream_ttl {
            streams = streams.ttl(ttl);
        }
        builder
            .queue(durable_memory::MemoryQueue::new())
            .pubsub(durable_memory::MemoryPubSub::new())
            .cache(durable_memory::MemoryCache::new())
            .streams(streams)
            .build()
    }
}

/// `scheme://user:secret@host/...` with the password replaced, so connection
/// URLs can be logged.
fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return "<redacted>".into();
    };
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    match authority.rsplit_once('@') {
        Some((userinfo, host)) => {
            let user = userinfo.split_once(':').map_or(userinfo, |(u, _)| u);
            let secret = if userinfo.contains(':') { ":***" } else { "" };
            format!("{scheme}://{user}{secret}@{host}{tail}")
        }
        None => url.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_output_never_contains_passwords() {
        let mut c = BackendConfig::new("postgres://app:hunter2@db:5432/app?sslmode=require");
        c.redis_url = Some("redis://:s3cret@cache:6379/0".into());
        let shown = format!("{c:?}");
        assert!(
            !shown.contains("hunter2") && !shown.contains("s3cret"),
            "{shown}"
        );
        assert!(
            shown.contains("postgres://app:***@db:5432/app?sslmode=require"),
            "{shown}"
        );
        assert!(shown.contains("redis://:***@cache:6379/0"), "{shown}");
        assert_eq!(redact_url("postgres://db/app"), "postgres://db/app");
        assert_eq!(redact_url("not a url"), "<redacted>");
    }
}
