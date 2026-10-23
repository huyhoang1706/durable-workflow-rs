//! PostgreSQL implementation of the durable `Store`.
//!
//! Postgres is the source of truth: every state change is committed here
//! first. The runner brings its own small migration table so it never
//! collides with the host application's `_sqlx_migrations`.

mod migrate;
mod sql;
mod store;

pub use store::PostgresStore;
