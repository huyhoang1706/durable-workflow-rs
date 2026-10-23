//! sqlx 0.9 requires dynamic SQL to be explicitly marked as audited.
//!
//! Audit: the only interpolated parts are (a) the schema name, validated to
//! `[A-Za-z_][A-Za-z0-9_]*` by `PostgresStore::schema`, and (b) static table
//! names and constants in this crate. Every value is a bind parameter.

use sqlx::{
    AssertSqlSafe, Postgres,
    postgres::{PgArguments, PgRow},
    query::{Query, QueryScalar},
};

pub(crate) fn q(sql: String) -> Query<'static, Postgres, PgArguments> {
    sqlx::query(AssertSqlSafe(sql))
}

pub(crate) fn q_scalar<O>(sql: String) -> QueryScalar<'static, Postgres, O, PgArguments>
where
    (O,): for<'r> sqlx::FromRow<'r, PgRow>,
{
    sqlx::query_scalar(AssertSqlSafe(sql))
}

pub(crate) fn raw(sql: String) -> sqlx::RawSql {
    sqlx::raw_sql(AssertSqlSafe(sql))
}
