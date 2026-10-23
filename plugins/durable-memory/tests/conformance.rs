use std::sync::Arc;

use durable_core::{Queue, Store};
use durable_memory::{MemoryQueue, MemoryStore};

async fn make_store() -> Arc<dyn Store> {
    Arc::new(MemoryStore::new())
}

async fn make_queue() -> Arc<dyn Queue> {
    Arc::new(MemoryQueue::new())
}

durable_testkit::store_conformance!(make_store());
durable_testkit::queue_conformance!(make_queue());

use durable_core::{Cache, PubSub, RateLimiter};
use durable_memory::{MemoryCache, MemoryPubSub, MemoryRateLimiter};

async fn make_cache() -> Arc<dyn Cache> {
    Arc::new(MemoryCache::new())
}

async fn make_pubsub() -> Arc<dyn PubSub> {
    Arc::new(MemoryPubSub::new())
}

async fn make_limiter() -> Arc<dyn RateLimiter> {
    Arc::new(MemoryRateLimiter::new())
}

durable_testkit::cache_conformance!(make_cache());
durable_testkit::pubsub_conformance!(make_pubsub());
durable_testkit::rate_limiter_conformance!(make_limiter());
