//! SQLite implementation of the durable `Store`.
//!
//! One file holds the runs, so durable state needs no server: runs survive a
//! restart, and several processes (the CLI next to a worker) can share the
//! file. [`SqliteStore::in_memory`] gives a private database for tests and the
//! memory backend.
//!
//! The schema mirrors `durable-postgres`: ids are hyphenated UUID text,
//! timestamps are Unix microseconds and JSON is stored as text, so the rows
//! read well in `sqlite3`.

mod migrate;
mod sql;
mod store;

pub use store::SqliteStore;
