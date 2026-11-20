# durable-sqlite

The durable `Store` on SQLite: runs, their event history, signals, hooks and workflow definitions in one file, with no
server to run. Runs survive a restart, and several processes (a worker next to the CLI) can share the file.

It is a store only. Pair it with a queue, pub/sub and cache from `durable-memory` (one process) or `durable-redis`.

## Use

```rust
use durable_sqlite::SqliteStore;

let store = SqliteStore::open("durable.db").await?;   // or SqliteStore::connect("sqlite://durable.db")
store.migrate().await?;                               // required: connect/open do not create the tables

let backend = durable_memory::builder().store(store).build()?;
```

| Constructor | Database | Migration |
| --- | --- | --- |
| `SqliteStore::open(path)` | a file, created if missing | call `migrate()` |
| `SqliteStore::connect(url)` | `sqlite://path`, `sqlite:path`, plus sqlx's query options (`?mode=ro`, ...) | call `migrate()` |
| `SqliteStore::from_pool(pool)` | your own `SqlitePool` | call `migrate()` |
| `SqliteStore::in_memory()` | private, gone when the store is dropped | automatic, on first use |

**Call `migrate()` before the first store call.** Without it a file database has no tables and every call fails with
`no such table`. It is safe to call from every process at boot: migrations run under the database's write lock, and
applied versions are recorded in `schema_migrations`. Only `in_memory()` migrates by itself, because nothing else can
reach its connection first.

`connect("sqlite::memory:")` and `connect("sqlite://...?mode=memory")` return `in_memory()`. `mode=memory` counts only
as a query parameter; a file whose name contains it is a file.

## What to know

- **File permissions.** A new database file is created with Unix mode `0600` (owner only), and SQLite gives the `-wal`
  and `-shm` files next to it the same mode. The file holds workflow inputs and outputs, signal payloads and hook
  tokens, and a hook token is enough to resume a run. An existing file keeps its permissions, so check a file you
  created another way. Nothing is changed on Windows, where the file inherits the directory's ACL.
- **Durability.** The file runs in WAL mode with `synchronous = NORMAL`. A process crash loses nothing. A power loss or
  an operating-system crash can lose the last few commits (never corrupt the file), and a step whose result was lost
  runs again. Pass your own pool to `from_pool` for `synchronous = FULL`.
- **Writers take turns.** SQLite has one writer at a time. Every write opens with `BEGIN IMMEDIATE` and waits up to 10
  seconds for the lock. With `from_pool`, set a busy timeout and `foreign_keys` on the pool yourself.
- **In-memory.** One connection, so store calls take turns, and a connection from `acquire()` blocks the store until it
  is dropped.
- **Event filters** are matched in Rust with the containment rules of Postgres' `jsonb @>`, with one difference:
  numbers compare as JSON values, so `1` does not match `1.0`.

## Layout

Ids are hyphenated UUID text, timestamps are Unix microseconds and JSON is text, so the rows read well in `sqlite3`.
The schema mirrors `durable-postgres`; see `migrations/0001_init.sql`.

## Tests

```bash
cargo test -p durable-sqlite    # store conformance on a file and in memory, migrations, persistence
```
