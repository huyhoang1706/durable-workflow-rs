use durable_core::{Error, Result};
use sqlx::PgPool;

use crate::sql::{q, q_scalar, raw};

/// Embedded migrations: `(version, sql)`. `{schema}` is substituted.
const MIGRATIONS: &[(i32, &str)] = &[(1, include_str!("../migrations/0001_init.sql"))];

fn be(e: sqlx::Error) -> Error {
    Error::Backend(e.to_string())
}

/// Apply pending migrations, serialized across processes by an advisory lock.
pub(crate) async fn run(pool: &PgPool, schema: &str) -> Result<()> {
    let mut tx = pool.begin().await.map_err(be)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(format!("durable-workflow-migrate:{schema}"))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    raw(format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    raw(format!(
        "CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
            version int PRIMARY KEY,
            applied_at timestamptz NOT NULL DEFAULT now()
        )"
    ))
    .execute(&mut *tx)
    .await
    .map_err(be)?;
    for (version, sql) in MIGRATIONS {
        let applied: bool = q_scalar(format!(
            "SELECT EXISTS (SELECT 1 FROM {schema}.schema_migrations WHERE version = $1)"
        ))
        .bind(version)
        .fetch_one(&mut *tx)
        .await
        .map_err(be)?;
        if applied {
            continue;
        }
        raw(sql.replace("{schema}", schema))
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        q(format!(
            "INSERT INTO {schema}.schema_migrations (version) VALUES ($1)"
        ))
        .bind(version)
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    }
    tx.commit().await.map_err(be)
}
