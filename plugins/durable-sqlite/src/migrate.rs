use durable_spi::Result;
use sqlx::SqlitePool;

use crate::sql::be;

/// Embedded migrations: `(version, sql)`.
const MIGRATIONS: &[(i64, &str)] = &[(1, include_str!("../migrations/0001_init.sql"))];

/// Apply pending migrations. `BEGIN IMMEDIATE` takes the database's single
/// write lock, so concurrent migrations from several processes serialize.
pub(crate) async fn run(pool: &SqlitePool) -> Result<()> {
    let mut tx = pool.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS schema_migrations (
            version    INTEGER PRIMARY KEY,
            applied_at INTEGER NOT NULL DEFAULT (unixepoch())
        ) STRICT",
    )
    .execute(&mut *tx)
    .await
    .map_err(be)?;
    for (version, sql) in MIGRATIONS {
        let applied: bool =
            sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = ?)")
                .bind(version)
                .fetch_one(&mut *tx)
                .await
                .map_err(be)?;
        if applied {
            continue;
        }
        sqlx::raw_sql(sqlx::AssertSqlSafe(*sql))
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        sqlx::query("INSERT INTO schema_migrations (version) VALUES (?)")
            .bind(version)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
    }
    tx.commit().await.map_err(be)
}
