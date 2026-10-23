#![allow(dead_code)]

use redis::aio::ConnectionManager;
use testcontainers_modules::{
    redis::Redis,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::sync::OnceCell;

struct Server {
    // Held for the process lifetime; testcontainers' reaper removes it at exit.
    _container: ContainerAsync<Redis>,
    url: String,
}

static SERVER: OnceCell<Server> = OnceCell::const_new();

pub async fn url() -> &'static str {
    &SERVER
        .get_or_init(|| async {
            let container = Redis::default()
                .with_tag("7")
                .start()
                .await
                .expect("start redis container (is docker running?)");
            let port = container.get_host_port_ipv4(6379).await.unwrap();
            Server {
                url: format!("redis://127.0.0.1:{port}"),
                _container: container,
            }
        })
        .await
        .url
}

pub fn client_blocking_url() -> String {
    // Callers must have awaited `url()` once.
    SERVER.get().expect("server started").url.clone()
}

pub async fn client() -> redis::Client {
    redis::Client::open(url().await).unwrap()
}

pub async fn conn() -> ConnectionManager {
    ConnectionManager::new(client().await)
        .await
        .expect("connect to redis")
}

/// Unique key prefix so every test starts empty without FLUSHALL.
pub fn unique_prefix() -> String {
    format!("t{}", uuid::Uuid::now_v7().simple())
}
