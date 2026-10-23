use std::sync::Arc;

use crate::{Cache, Clock, Error, PubSub, Queue, RateLimiter, Result, Store, SystemClock};

/// Bundles the pluggable infrastructure slots
#[derive(Clone)]
pub struct Backend {
    pub namespace: String,
    pub store: Arc<dyn Store>,
    pub queue: Arc<dyn Queue>,
    pub pubsub: Arc<dyn PubSub>,
    pub cache: Option<Arc<dyn Cache>>,
    pub rate_limiter: Option<Arc<dyn RateLimiter>>,
    pub clock: Arc<dyn Clock>,
}

impl Backend {
    pub fn builder() -> BackendBuilder {
        BackendBuilder::default()
    }
}

#[derive(Default)]
pub struct BackendBuilder {
    namespace: Option<String>,
    store: Option<Arc<dyn Store>>,
    queue: Option<Arc<dyn Queue>>,
    pubsub: Option<Arc<dyn PubSub>>,
    cache: Option<Arc<dyn Cache>>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
    clock: Option<Arc<dyn Clock>>,
}

impl BackendBuilder {
    #[inline]
    pub fn namespace(mut self, ns: impl Into<String>) -> Self {
        self.namespace = Some(ns.into());
        self
    }
    #[inline]
    pub fn store(mut self, v: impl Store) -> Self {
        self.store = Some(Arc::new(v));
        self
    }
    #[inline]
    pub fn queue(mut self, v: impl Queue) -> Self {
        self.queue = Some(Arc::new(v));
        self
    }
    #[inline]
    pub fn pubsub(mut self, v: impl PubSub) -> Self {
        self.pubsub = Some(Arc::new(v));
        self
    }
    #[inline]
    pub fn cache(mut self, v: impl Cache) -> Self {
        self.cache = Some(Arc::new(v));
        self
    }
    #[inline]
    pub fn rate_limiter(mut self, v: impl RateLimiter) -> Self {
        self.rate_limiter = Some(Arc::new(v));
        self
    }
    #[inline]
    pub fn clock(mut self, v: Arc<dyn Clock>) -> Self {
        self.clock = Some(v);
        self
    }

    pub fn build(self) -> Result<Backend> {
        let missing = |what: &str| Error::Config(format!("backend is missing a {what}"));
        Ok(Backend {
            namespace: self.namespace.unwrap_or_else(|| "default".into()),
            store: self.store.ok_or_else(|| missing("store"))?,
            queue: self.queue.ok_or_else(|| missing("queue"))?,
            pubsub: self.pubsub.ok_or_else(|| missing("pubsub"))?,
            cache: self.cache,
            rate_limiter: self.rate_limiter,
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
        })
    }
}
