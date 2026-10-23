#![allow(dead_code)]

use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner},
};
use tokio::sync::OnceCell;

struct Server {
    // Held for the process lifetime; testcontainers' reaper removes it at exit.
    _container: ContainerAsync<Postgres>,
    url: String,
}

static SERVER: OnceCell<Server> = OnceCell::const_new();

pub async fn url() -> &'static str {
    &SERVER
        .get_or_init(|| async {
            let container = Postgres::default()
                .with_tag("17")
                .start()
                .await
                .expect("start postgres container (is docker running?)");
            let port = container.get_host_port_ipv4(5432).await.unwrap();
            Server {
                url: format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres"),
                _container: container,
            }
        })
        .await
        .url
}

pub async fn pool() -> PgPool {
    let url = url().await;
    // The container may accept connections a moment after the port opens.
    for _ in 0..50 {
        if let Ok(p) = PgPoolOptions::new().max_connections(8).connect(url).await {
            return p;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    panic!("postgres did not become reachable");
}

/// A unique schema name so every test starts empty.
pub fn unique_schema() -> String {
    format!("t_{}", uuid::Uuid::now_v7().simple())
}
