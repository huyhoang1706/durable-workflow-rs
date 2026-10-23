#![allow(dead_code)]

use std::sync::Arc;

use durable_workflow::{
    Backend, Client,
    postgres::PostgresStore,
    redis::{RedisCache, RedisPubSub, RedisQueue, RedisRateLimiter},
};
use sqlx::postgres::PgPoolOptions;
use testcontainers_modules::{
    postgres::Postgres,
    redis::Redis,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::sync::OnceCell;

pub struct PgServer {
    _c: ContainerAsync<Postgres>,
    pub url: String,
}

pub struct RedisServer {
    _c: ContainerAsync<Redis>,
    pub url: String,
}

static PG: OnceCell<PgServer> = OnceCell::const_new();
static REDIS: OnceCell<RedisServer> = OnceCell::const_new();

pub async fn start_pg() -> PgServer {
    let c = Postgres::default()
        .with_tag("17")
        .start()
        .await
        .expect("postgres container");
    let port = c.get_host_port_ipv4(5432).await.unwrap();
    PgServer {
        url: format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
        _c: c,
    }
}

pub async fn start_redis() -> RedisServer {
    let c = Redis::default()
        .with_tag("7")
        .start()
        .await
        .expect("redis container");
    let port = c.get_host_port_ipv4(6379).await.unwrap();
    RedisServer {
        url: format!("redis://127.0.0.1:{port}"),
        _c: c,
    }
}

pub async fn shared_pg() -> &'static PgServer {
    PG.get_or_init(start_pg).await
}

pub async fn shared_redis() -> &'static RedisServer {
    REDIS.get_or_init(start_redis).await
}

pub fn unique(prefix: &str) -> String {
    format!("{prefix}_{}", uuid::Uuid::now_v7().simple())
}

/// A backend on real Postgres and Redis, isolated by schema/prefix/namespace.
pub async fn backend(pg_url: &str, redis_url: &str) -> Backend {
    let pool = {
        let mut attempt = 0;
        loop {
            match PgPoolOptions::new()
                .max_connections(24)
                .connect(pg_url)
                .await
            {
                Ok(p) => break p,
                Err(e) if attempt < 50 => {
                    attempt += 1;
                    let _ = e;
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                Err(e) => panic!("postgres unreachable: {e}"),
            }
        }
    };
    let schema = unique("e2e");
    let store = PostgresStore::from_pool(pool).schema(&schema);
    store.migrate().await.unwrap();

    let client = redis::Client::open(redis_url).unwrap();
    let conn = redis::aio::ConnectionManager::new(client.clone())
        .await
        .unwrap();
    let prefix = unique("p");
    Backend::builder()
        .namespace(unique("ns"))
        .store(store)
        .queue(RedisQueue::new(conn.clone()).prefix(prefix.clone()))
        .pubsub(RedisPubSub::new(conn.clone(), client).prefix(prefix.clone()))
        .cache(RedisCache::new(conn.clone()).prefix(prefix.clone()))
        .rate_limiter(RedisRateLimiter::new(conn).prefix(prefix))
        .build()
        .unwrap()
}

pub fn client(b: &Backend) -> Client {
    Client::new(b.clone())
}

pub fn arc<T>(v: T) -> Arc<T> {
    Arc::new(v)
}
