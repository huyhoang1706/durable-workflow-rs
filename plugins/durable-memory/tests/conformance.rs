use std::sync::Arc;

use durable_memory::{MemoryQueue, MemoryStore};
use durable_spi::{Queue, Store};

async fn make_store() -> Arc<dyn Store> {
    Arc::new(MemoryStore::new())
}

async fn make_queue() -> Arc<dyn Queue> {
    Arc::new(MemoryQueue::new())
}

durable_testkit::store_conformance!(make_store());
durable_testkit::queue_conformance!(make_queue());

use durable_memory::{MemoryCache, MemoryPubSub, MemoryRateLimiter};
use durable_spi::{Cache, PubSub, RateLimiter};

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

use durable_memory::MemoryStreams;
use durable_spi::StreamStore;
use durable_testkit::{ManualClock, t0};

async fn make_streams(ttl: std::time::Duration) -> durable_testkit::streams::StreamHarness {
    let clock = ManualClock::new(t0());
    let store: Arc<dyn StreamStore> = Arc::new(MemoryStreams::new(clock.clone()).ttl(ttl));
    durable_testkit::streams::StreamHarness {
        store,
        elapse: Box::new(move |d| {
            clock.advance(chrono::Duration::from_std(d).unwrap());
            Box::pin(async {})
        }),
    }
}

durable_testkit::stream_conformance!(make_streams);
