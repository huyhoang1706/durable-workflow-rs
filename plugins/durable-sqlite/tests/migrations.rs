mod common;

use durable_sqlite::SqliteStore;

async fn versions(store: &SqliteStore) -> Vec<i64> {
    sqlx::query_scalar("SELECT version FROM schema_migrations ORDER BY version")
        .fetch_all(&mut *store.acquire().await.unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn migrate_is_idempotent_and_records_versions() {
    let dir = common::TempDir::new();
    let store = SqliteStore::open(dir.file("d.db")).await.unwrap();
    store.migrate().await.unwrap();
    store.migrate().await.unwrap();
    assert_eq!(versions(&store).await, [1]);
}

#[tokio::test]
async fn in_memory_migrates_on_first_use() {
    use durable_spi::Store;
    let store = SqliteStore::in_memory();
    assert!(store.get_run(uuid::Uuid::now_v7()).await.unwrap().is_none());
    // An explicit migrate afterwards is a no-op.
    store.migrate().await.unwrap();
    assert_eq!(versions(&store).await, [1]);
}

#[tokio::test]
async fn connect_accepts_urls_and_creates_the_file() {
    let dir = common::TempDir::new();
    let path = dir.file("url.db");
    let store = SqliteStore::connect(&format!("sqlite://{}", path.display()))
        .await
        .unwrap();
    store.migrate().await.unwrap();
    assert!(path.exists());
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *store.acquire().await.unwrap())
        .await
        .unwrap();
    assert_eq!(mode, "wal");

    let memory = SqliteStore::connect("sqlite::memory:").await.unwrap();
    memory.migrate().await.unwrap();
    assert_eq!(versions(&memory).await, [1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_migrations_from_several_pools_are_serialized() {
    let dir = common::TempDir::new();
    let path = dir.file("shared.db");
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let path = path.clone();
            tokio::spawn(async move {
                let store = SqliteStore::open(path).await?;
                store.migrate().await?;
                Ok::<_, durable_spi::Error>(store)
            })
        })
        .collect();
    for t in tasks {
        let store = t.await.unwrap().expect("every concurrent migrate succeeds");
        assert_eq!(versions(&store).await, [1]);
    }
}
