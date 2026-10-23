mod common;

use std::sync::Arc;

use durable_core::{Cache, PubSub, Queue, RateLimiter};
use durable_redis::{RedisCache, RedisPubSub, RedisQueue, RedisRateLimiter};

async fn make_queue() -> Arc<dyn Queue> {
    Arc::new(RedisQueue::new(common::conn().await).prefix(common::unique_prefix()))
}

async fn make_cache() -> Arc<dyn Cache> {
    Arc::new(RedisCache::new(common::conn().await).prefix(common::unique_prefix()))
}

async fn make_pubsub() -> Arc<dyn PubSub> {
    Arc::new(
        RedisPubSub::new(common::conn().await, common::client().await)
            .prefix(common::unique_prefix()),
    )
}

async fn make_limiter() -> Arc<dyn RateLimiter> {
    Arc::new(RedisRateLimiter::new(common::conn().await).prefix(common::unique_prefix()))
}

durable_testkit::queue_conformance!(make_queue());
durable_testkit::cache_conformance!(make_cache());
durable_testkit::pubsub_conformance!(make_pubsub());
durable_testkit::rate_limiter_conformance!(make_limiter());
