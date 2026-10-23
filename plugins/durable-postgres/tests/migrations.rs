mod common;

use durable_postgres::PostgresStore;
use sqlx::Row;

#[tokio::test]
async fn migrate_is_idempotent_and_records_versions() {
    let pool = common::pool().await;
    let schema = common::unique_schema();
    let store = PostgresStore::from_pool(pool.clone()).schema(&schema);
    store.migrate().await.unwrap();
    store.migrate().await.unwrap();
    let versions: Vec<i32> = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT version FROM {schema}.schema_migrations ORDER BY version"
    )))
    .fetch_all(&pool)
    .await
    .unwrap()
    .iter()
    .map(|r| r.get(0))
    .collect();
    assert_eq!(versions, [1]);
}

#[tokio::test]
async fn does_not_touch_the_host_apps_migration_table() {
    let pool = common::pool().await;
    sqlx::query("CREATE TABLE IF NOT EXISTS public._sqlx_migrations (version bigint primary key)")
        .execute(&pool)
        .await
        .unwrap();
    let before: i64 = sqlx::query("SELECT count(*) FROM public._sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    PostgresStore::from_pool(pool.clone())
        .schema(&common::unique_schema())
        .migrate()
        .await
        .unwrap();
    let after: i64 = sqlx::query("SELECT count(*) FROM public._sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap()
        .get(0);
    assert_eq!(before, after);
}

#[tokio::test]
async fn concurrent_migrations_are_serialized() {
    let pool = common::pool().await;
    let schema = common::unique_schema();
    let tasks: Vec<_> = (0..4)
        .map(|_| {
            let store = PostgresStore::from_pool(pool.clone()).schema(&schema);
            tokio::spawn(async move { store.migrate().await })
        })
        .collect();
    for t in tasks {
        t.await.unwrap().expect("every concurrent migrate succeeds");
    }
}

#[tokio::test]
async fn rejects_unsafe_schema_names() {
    let pool = sqlx::PgPool::connect_lazy("postgres://x@localhost/x").unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        PostgresStore::from_pool(pool).schema("bad; DROP TABLE runs")
    }));
    assert!(result.is_err());
}
