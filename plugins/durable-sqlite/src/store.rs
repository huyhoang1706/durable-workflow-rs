use std::{
    ops::{Deref, DerefMut},
    path::Path,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_spi::*;
use serde_json::Value;
use sqlx::{
    ConnectOptions, Connection, QueryBuilder, Row, Sqlite, SqliteConnection, SqlitePool,
    pool::PoolConnection,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteRow, SqliteSynchronous,
    },
};
use tokio::sync::{MappedMutexGuard, Mutex, MutexGuard};
use uuid::Uuid;

use crate::{
    migrate,
    sql::{be, from_json, from_us, id, json, opt_from_json, opt_from_us, parse_id, us},
};

/// How long a writer waits for another connection (or process) to release
/// the database's write lock before failing with `SQLITE_BUSY`.
const BUSY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct SqliteStore {
    source: Source,
}

#[derive(Clone)]
enum Source {
    /// A file database: readers run concurrently under WAL.
    Pool(SqlitePool),
    /// [`SqliteStore::in_memory`]: one connection, opened and migrated on
    /// first use. Every connection to `:memory:` is its own database, and a
    /// pool would cost a task spawn per release, so it is held directly.
    Memory(Arc<Mutex<Option<SqliteConnection>>>),
}

/// A connection for one store call: pooled, or the in-memory one, held
/// exclusively until dropped.
enum Conn<'a> {
    Pool(PoolConnection<Sqlite>),
    Memory(MappedMutexGuard<'a, SqliteConnection>),
}

impl Deref for Conn<'_> {
    type Target = SqliteConnection;

    fn deref(&self) -> &SqliteConnection {
        match self {
            Conn::Pool(c) => c,
            Conn::Memory(c) => c,
        }
    }
}

impl DerefMut for Conn<'_> {
    fn deref_mut(&mut self) -> &mut SqliteConnection {
        match self {
            Conn::Pool(c) => c,
            Conn::Memory(c) => c,
        }
    }
}

impl SqliteStore {
    /// A file database: created if missing, WAL, `synchronous = NORMAL`, a
    /// busy timeout, foreign keys on. Accepts `sqlite://path`, `sqlite:path`
    /// and the query options of [`SqliteConnectOptions`]; `sqlite::memory:`
    /// gives [`in_memory`](Self::in_memory).
    ///
    /// This does not create the tables: call [`migrate`](Self::migrate)
    /// before the first store call, or it fails with "no such table". (Only
    /// [`in_memory`](Self::in_memory) migrates by itself.)
    ///
    /// A new file is created readable and writable by its owner only (Unix
    /// mode `0600`): it holds workflow inputs, outputs and hook tokens. An
    /// existing file keeps its permissions.
    pub async fn connect(url: &str) -> Result<Self> {
        if is_memory_url(url) {
            return Ok(Self::in_memory());
        }
        let options = SqliteConnectOptions::from_str(url).map_err(be)?;
        if !is_read_only_url(url) {
            create_private(options.get_filename());
        }
        Self::connect_with(options).await
    }

    /// As [`connect`](Self::connect), from a file path. Call
    /// [`migrate`](Self::migrate) before use.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self> {
        create_private(path.as_ref());
        Self::connect_with(SqliteConnectOptions::new().filename(path)).await
    }

    async fn connect_with(options: SqliteConnectOptions) -> Result<Self> {
        let options = options
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(BUSY_TIMEOUT)
            .foreign_keys(true);
        // Switching a new file to WAL takes an exclusive lock without going
        // through the busy handler, so processes opening the same new file at
        // once can see `SQLITE_BUSY` here. Retry until the busy timeout.
        let deadline = tokio::time::Instant::now() + BUSY_TIMEOUT;
        let mut backoff = Duration::from_millis(5);
        loop {
            match SqlitePoolOptions::new().connect_with(options.clone()).await {
                Ok(pool) => return Ok(Self::from_pool(pool)),
                Err(e) if is_busy(&e) && tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_millis(100));
                }
                Err(e) => return Err(be(e)),
            }
        }
    }

    /// Reuse an existing pool. Call [`migrate`](Self::migrate) before use.
    ///
    /// Every write opens its transaction with `BEGIN IMMEDIATE`; give the
    /// pool a busy timeout so concurrent writers wait instead of failing.
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self {
            source: Source::Pool(pool),
        }
    }

    /// A private in-memory database; connects and migrates on first use.
    ///
    /// Synchronous so it can be built outside a Tokio runtime. The data lives
    /// as long as the store (and its clones). Store calls take turns on its
    /// single connection.
    pub fn in_memory() -> Self {
        Self {
            source: Source::Memory(Arc::new(Mutex::new(None))),
        }
    }

    /// The pool of a file database; `None` for [`in_memory`](Self::in_memory)
    /// (use [`acquire`](Self::acquire)).
    pub fn pool(&self) -> Option<&SqlitePool> {
        match &self.source {
            Source::Pool(pool) => Some(pool),
            Source::Memory(_) => None,
        }
    }

    /// A connection for raw SQL. An in-memory store has one, so the store
    /// waits until this is dropped.
    pub async fn acquire(&self) -> Result<impl DerefMut<Target = SqliteConnection> + '_> {
        self.conn().await
    }

    /// Create or upgrade the tables. Safe to call from every process at boot.
    pub async fn migrate(&self) -> Result<()> {
        migrate::run(&mut *self.conn().await?).await
    }

    async fn conn(&self) -> Result<Conn<'_>> {
        match &self.source {
            Source::Pool(pool) => Ok(Conn::Pool(pool.acquire().await.map_err(be)?)),
            Source::Memory(cell) => {
                let mut guard = cell.lock().await;
                if guard.is_none() {
                    let mut conn = SqliteConnectOptions::new()
                        .in_memory(true)
                        .foreign_keys(true)
                        .connect()
                        .await
                        .map_err(be)?;
                    migrate::run(&mut conn).await?;
                    *guard = Some(conn);
                }
                Ok(Conn::Memory(MutexGuard::map(guard, |c| {
                    c.as_mut().expect("connected above")
                })))
            }
        }
    }
}

/// `SQLITE_BUSY` or `SQLITE_LOCKED`, including their extended codes.
fn is_busy(e: &sqlx::Error) -> bool {
    let code = match e {
        sqlx::Error::Database(db) => db.code().and_then(|c| c.parse::<i32>().ok()),
        _ => None,
    };
    matches!(code.map(|c| c & 0xff), Some(5 | 6))
}

/// The path and the query parameters of a `sqlite:` URL.
fn split_url(url: &str) -> (&str, impl Iterator<Item = (&str, &str)>) {
    let rest = url
        .strip_prefix("sqlite://")
        .or_else(|| url.strip_prefix("sqlite:"))
        .unwrap_or(url);
    let (path, query) = rest.split_once('?').unwrap_or((rest, ""));
    let params = query
        .split('&')
        .map(|p| p.split_once('=').unwrap_or((p, "")));
    (path, params)
}

fn is_memory_url(url: &str) -> bool {
    let (path, mut params) = split_url(url);
    path == ":memory:" || params.any(|p| p == ("mode", "memory"))
}

/// `mode=ro` or `immutable`: SQLite will not create the file.
fn is_read_only_url(url: &str) -> bool {
    split_url(url).1.any(|(k, v)| match k {
        "mode" => v == "ro",
        "immutable" => matches!(v, "true" | "1"),
        _ => false,
    })
}

/// Create the database file, if it is missing, for its owner only. SQLite
/// would create it with the process umask (usually world-readable), and it
/// holds workflow inputs, outputs and hook tokens. The `-wal` and `-shm`
/// files take the permissions of the database file.
///
/// Best effort: an existing file is left as it is, and any other failure
/// (a missing directory, say) is reported by the connect that follows.
fn create_private(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path);
    }
    #[cfg(not(unix))]
    let _ = path;
}

fn run_from_row(r: &SqliteRow) -> std::result::Result<Run, sqlx::Error> {
    let status: &str = r.try_get("status")?;
    Ok(Run {
        id: parse_id(r.try_get("id")?)?,
        namespace: r.try_get("namespace")?,
        workflow: r.try_get("workflow")?,
        idempotency_key: r.try_get("idempotency_key")?,
        status: status
            .parse()
            .map_err(|e: String| sqlx::Error::Decode(e.into()))?,
        input: from_json(r.try_get("input")?)?,
        output: opt_from_json(r.try_get("output")?)?,
        error: r.try_get("error")?,
        last_seq: r.try_get::<i64, _>("last_seq")? as u64,
        wake_at: opt_from_us(r.try_get("wake_at")?)?,
        awaiting: from_json(r.try_get("awaiting")?)?,
        concurrency_key: r.try_get("concurrency_key")?,
        throttle_key: r.try_get("throttle_key")?,
        deadline_at: opt_from_us(r.try_get("deadline_at")?)?,
        cancel_requested_at: opt_from_us(r.try_get("cancel_requested_at")?)?,
        cancel_reason: r.try_get("cancel_reason")?,
        last_error: r.try_get("last_error")?,
        trace_context: opt_from_json(r.try_get("trace_context")?)?,
        created_at: from_us(r.try_get("created_at")?)?,
        updated_at: from_us(r.try_get("updated_at")?)?,
    })
}

async fn load_run(tx: &mut SqliteConnection, run_id: RunId) -> Result<Run> {
    let row = sqlx::query("SELECT * FROM runs WHERE id = ?")
        .bind(id(run_id))
        .fetch_optional(&mut *tx)
        .await
        .map_err(be)?
        .ok_or_else(|| Error::NotFound(format!("run {run_id}")))?;
    run_from_row(&row).map_err(be)
}

async fn load_live_run(tx: &mut SqliteConnection, run_id: RunId) -> Result<Run> {
    let run = load_run(tx, run_id).await?;
    if run.status.is_terminal() {
        return Err(Error::InvalidState(format!(
            "run {run_id} is {}",
            run.status.as_str()
        )));
    }
    Ok(run)
}

/// Append events `(seq, kind as JSON)` in one statement.
async fn insert_events(
    tx: &mut SqliteConnection,
    run_id: RunId,
    at: DateTime<Utc>,
    events: Vec<(i64, String)>,
) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }
    let (run_id, at) = (id(run_id), us(at));
    QueryBuilder::<Sqlite>::new("INSERT INTO run_events (run_id, seq, at, kind) ")
        .push_values(events, |mut b, (seq, kind)| {
            b.push_bind(run_id.clone())
                .push_bind(seq)
                .push_bind(at)
                .push_bind(kind);
        })
        .build()
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    Ok(())
}

/// `DELETE FROM {table} WHERE run_id = ? AND {column} IN (…)`.
async fn delete_in(
    tx: &mut SqliteConnection,
    table: &str,
    column: &str,
    run_id: RunId,
    values: &[&str],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let mut q = QueryBuilder::<Sqlite>::new(format!("DELETE FROM {table} WHERE run_id = "));
    q.push_bind(id(run_id)).push(format!(" AND {column} IN ("));
    let mut list = q.separated(", ");
    for v in values {
        list.push_bind(v.to_string());
    }
    q.push(")").build().execute(&mut *tx).await.map_err(be)?;
    Ok(())
}

/// Put a signal in the inbox under the write lock; report whether to wake.
async fn deliver_tx(
    tx: &mut SqliteConnection,
    run: &Run,
    name: &str,
    payload: Value,
    at: DateTime<Utc>,
) -> Result<Option<QueueItem>> {
    sqlx::query(
        "INSERT INTO signals (id, run_id, name, target_key, payload, created_at)
         VALUES (?, ?, ?, NULL, ?, ?)",
    )
    .bind(id(Uuid::now_v7()))
    .bind(id(run.id))
    .bind(name)
    .bind(json(&payload)?)
    .bind(us(at))
    .execute(&mut *tx)
    .await
    .map_err(be)?;

    if run.status != RunStatus::Waiting {
        return Ok(None);
    }
    let waits = sqlx::query("SELECT kind, name FROM waits WHERE run_id = ?")
        .bind(id(run.id))
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?;
    let satisfied = waits
        .iter()
        .any(|w| inbox_name(w.get("kind"), w.get("name")) == name);
    if !satisfied {
        return Ok(None);
    }
    let wake = run.wake_at.map_or(at, |w| w.min(at));
    sqlx::query("UPDATE runs SET wake_at = ?, updated_at = ? WHERE id = ?")
        .bind(us(wake))
        .bind(us(at))
        .bind(id(run.id))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    Ok(Some(run.queue_item(wake)))
}

/// Make the stored open waits equal `waits`. Most commits (step results,
/// timers) leave them unchanged, so they are only rewritten on a change.
async fn sync_waits(tx: &mut SqliteConnection, run_id: RunId, waits: &[WaitReg]) -> Result<()> {
    let stored =
        sqlx::query("SELECT key, kind, name, filter, timeout_at FROM waits WHERE run_id = ?")
            .bind(id(run_id))
            .fetch_all(&mut *tx)
            .await
            .map_err(be)?
            .iter()
            .map(|r| {
                Ok(WaitRow {
                    key: r.try_get("key")?,
                    kind: r.try_get("kind")?,
                    name: r.try_get("name")?,
                    filter: opt_from_json(r.try_get("filter")?)?,
                    timeout_us: r.try_get("timeout_at")?,
                })
            })
            .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
            .map_err(be)?;
    let wanted: Vec<WaitRow> = waits.iter().map(WaitRow::from).collect();
    if same_waits(stored, wanted) {
        return Ok(());
    }
    sqlx::query("DELETE FROM waits WHERE run_id = ?")
        .bind(id(run_id))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    if waits.is_empty() {
        return Ok(());
    }
    let rows = waits
        .iter()
        .map(|w| {
            Ok((
                w.key.clone(),
                wait_kind_str(w.kind),
                w.name.clone(),
                w.filter.as_ref().map(json).transpose()?,
                w.timeout_at.map(us),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let run_id = id(run_id);
    QueryBuilder::<Sqlite>::new("INSERT INTO waits (run_id, key, kind, name, filter, timeout_at) ")
        .push_values(rows, |mut b, (key, kind, name, filter, timeout)| {
            b.push_bind(run_id.clone())
                .push_bind(key)
                .push_bind(kind)
                .push_bind(name)
                .push_bind(filter)
                .push_bind(timeout);
        })
        .build()
        .execute(&mut *tx)
        .await
        .map_err(be)?;
    Ok(())
}

/// A commit that registers no wait or hook and does not end the run: most
/// step results and timers. It can neither consume a signal nor generate an
/// event, so its outcome is known before reading the run.
fn is_plain(c: &Commit) -> bool {
    c.waits.is_empty()
        && c.hooks.is_empty()
        && !c.events.iter().any(|k| {
            matches!(
                k,
                EventKind::RunSucceeded { .. }
                    | EventKind::RunFailed { .. }
                    | EventKind::RunCancelled { .. }
                    | EventKind::HookDisposed { .. }
            )
        })
}

/// [`is_plain`] commits in four round trips to sqlx's connection thread
/// (begin, update, insert, commit) instead of seven: it does not read the
/// run, its waits or its inbox. The version check moves into the `UPDATE`.
async fn commit_plain(tx: &mut SqliteConnection, c: &Commit) -> Result<CommitOutcome> {
    let first = c.expected_seq as i64 + 1;
    let last_seq = c.expected_seq as i64 + c.events.len() as i64;
    let row = sqlx::query(
        "UPDATE runs SET status = ?, last_seq = ?, wake_at = ?, awaiting = ?, last_error = ?,
                         updated_at = ?
         WHERE id = ? AND last_seq = ? AND status NOT IN ('succeeded', 'failed', 'cancelled')
         RETURNING cancel_requested_at, has_waits",
    )
    .bind(c.status.as_str())
    .bind(last_seq)
    .bind(c.wake_at.map(us))
    .bind(json(&c.awaiting)?)
    .bind(&c.last_error)
    .bind(us(c.at))
    .bind(id(c.run_id))
    .bind(c.expected_seq as i64)
    .fetch_optional(&mut *tx)
    .await
    .map_err(be)?;
    let Some(row) = row else {
        let exists: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM runs WHERE id = ?)")
            .bind(id(c.run_id))
            .fetch_one(&mut *tx)
            .await
            .map_err(be)?;
        return Err(if exists {
            Error::Conflict
        } else {
            Error::NotFound(format!("run {}", c.run_id))
        });
    };
    let cancel_requested = row
        .try_get::<Option<i64>, _>("cancel_requested_at")
        .map_err(be)?
        .is_some();
    // The waits this commit no longer lists: rare, right after one resolved.
    if row.try_get::<bool, _>("has_waits").map_err(be)? {
        for sql in [
            "DELETE FROM waits WHERE run_id = ?",
            "UPDATE runs SET has_waits = 0 WHERE id = ?",
        ] {
            sqlx::query(sql)
                .bind(id(c.run_id))
                .execute(&mut *tx)
                .await
                .map_err(be)?;
        }
    }
    let events = c
        .events
        .iter()
        .zip(first..)
        .map(|(kind, seq)| Ok((seq, json(kind)?)))
        .collect::<Result<Vec<_>>>()?;
    insert_events(tx, c.run_id, c.at, events).await?;
    Ok(CommitOutcome {
        last_seq: last_seq as u64,
        generated: vec![],
        cancel_requested,
    })
}

/// A wait as stored. Timestamps compare at microsecond precision.
#[derive(PartialEq)]
struct WaitRow {
    key: String,
    kind: String,
    name: String,
    filter: Option<Value>,
    timeout_us: Option<i64>,
}

impl From<&WaitReg> for WaitRow {
    fn from(w: &WaitReg) -> Self {
        Self {
            key: w.key.clone(),
            kind: wait_kind_str(w.kind).into(),
            name: w.name.clone(),
            filter: w.filter.clone(),
            timeout_us: w.timeout_at.map(us),
        }
    }
}

fn same_waits(mut a: Vec<WaitRow>, mut b: Vec<WaitRow>) -> bool {
    a.sort_by(|x, y| x.key.cmp(&y.key));
    b.sort_by(|x, y| x.key.cmp(&y.key));
    a == b
}

fn inbox_name(kind: &str, name: &str) -> String {
    match kind {
        "hook" => format!("hook:{name}"),
        "event" => format!("event:{name}"),
        _ => name.to_string(),
    }
}

fn wait_kind_str(k: WaitKind) -> &'static str {
    match k {
        WaitKind::Signal => "signal",
        WaitKind::Hook => "hook",
        WaitKind::Event => "event",
    }
}

/// JSON containment, as Postgres' `data @> filter`: objects match by subset,
/// arrays when every filter element is contained in some data element.
fn contains(data: &Value, filter: &Value) -> bool {
    match (data, filter) {
        (Value::Object(d), Value::Object(f)) => f
            .iter()
            .all(|(k, fv)| d.get(k).is_some_and(|dv| contains(dv, fv))),
        (Value::Array(d), Value::Array(f)) => {
            f.iter().all(|fv| d.iter().any(|dv| contains(dv, fv)))
        }
        (d, f) => d == f,
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn create_run(&self, new: NewRun) -> Result<CreateOutcome> {
        let run_id = Uuid::now_v7();
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        let inserted = sqlx::query(
            "INSERT INTO runs (id, namespace, workflow, idempotency_key, status, input, last_seq,
                               wake_at, concurrency_key, throttle_key, deadline_at, trace_context,
                               created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, 'pending', ?5, 1, ?6, ?7, ?8, ?9, ?10, ?6, ?6)
             ON CONFLICT (namespace, workflow, idempotency_key) WHERE idempotency_key IS NOT NULL
             DO NOTHING
             RETURNING *",
        )
        .bind(id(run_id))
        .bind(&new.namespace)
        .bind(&new.workflow)
        .bind(&new.idempotency_key)
        .bind(json(&new.input)?)
        .bind(us(new.at))
        .bind(&new.concurrency_key)
        .bind(&new.throttle_key)
        .bind(new.deadline_at.map(us))
        .bind(new.trace_context.as_ref().map(json).transpose()?)
        .fetch_optional(&mut *tx)
        .await
        .map_err(be)?;

        match inserted {
            Some(row) => {
                let run = run_from_row(&row).map_err(be)?;
                let started = EventKind::RunStarted { input: new.input };
                insert_events(&mut tx, run_id, new.at, vec![(1, json(&started)?)]).await?;
                tx.commit().await.map_err(be)?;
                Ok(CreateOutcome::Created(run))
            }
            None => {
                let row = sqlx::query(
                    "SELECT * FROM runs WHERE namespace = ? AND workflow = ? AND idempotency_key = ?",
                )
                .bind(&new.namespace)
                .bind(&new.workflow)
                .bind(&new.idempotency_key)
                .fetch_one(&mut *tx)
                .await
                .map_err(be)?;
                Ok(CreateOutcome::Existing(run_from_row(&row).map_err(be)?))
            }
        }
    }

    async fn get_run(&self, run_id: RunId) -> Result<Option<Run>> {
        let row = sqlx::query("SELECT * FROM runs WHERE id = ?")
            .bind(id(run_id))
            .fetch_optional(&mut *self.conn().await?)
            .await
            .map_err(be)?;
        row.map(|r| run_from_row(&r).map_err(be)).transpose()
    }

    async fn list_runs(&self, f: RunFilter) -> Result<Vec<Run>> {
        let mut q = QueryBuilder::<Sqlite>::new("SELECT * FROM runs WHERE 1");
        if let Some(ns) = &f.namespace {
            q.push(" AND namespace = ").push_bind(ns.clone());
        }
        if let Some(wf) = &f.workflow {
            q.push(" AND workflow = ").push_bind(wf.clone());
        }
        if let Some(s) = f.status {
            q.push(" AND status = ").push_bind(s.as_str());
        }
        if let Some(after) = f.after {
            q.push(" AND (created_at, id) < (SELECT created_at, id FROM runs WHERE id = ")
                .push_bind(id(after))
                .push(")");
        }
        q.push(" ORDER BY created_at DESC, id DESC");
        if let Some(limit) = f.limit {
            q.push(" LIMIT ").push_bind(limit as i64);
        }
        let rows = q
            .build()
            .fetch_all(&mut *self.conn().await?)
            .await
            .map_err(be)?;
        rows.iter().map(|r| run_from_row(r).map_err(be)).collect()
    }

    async fn load_events(&self, run_id: RunId, after_seq: u64) -> Result<Vec<Event>> {
        let rows = sqlx::query(
            "SELECT seq, at, kind FROM run_events WHERE run_id = ? AND seq > ? ORDER BY seq",
        )
        .bind(id(run_id))
        .bind(after_seq as i64)
        .fetch_all(&mut *self.conn().await?)
        .await
        .map_err(be)?;
        if rows.is_empty() && self.get_run(run_id).await?.is_none() {
            return Err(Error::NotFound(format!("run {run_id}")));
        }
        rows.iter()
            .map(|r| {
                Ok(Event {
                    run_id,
                    seq: r.try_get::<i64, _>("seq")? as u64,
                    at: from_us(r.try_get("at")?)?,
                    kind: from_json(r.try_get("kind")?)?,
                })
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(be)
    }

    async fn commit(&self, c: Commit) -> Result<CommitOutcome> {
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        if is_plain(&c) {
            let out = commit_plain(&mut tx, &c).await?;
            tx.commit().await.map_err(be)?;
            return Ok(out);
        }
        let run = load_run(&mut tx, c.run_id).await?;
        if run.last_seq != c.expected_seq || run.status.is_terminal() {
            return Err(Error::Conflict);
        }
        let mut seq = run.last_seq as i64;
        let mut terminal: Option<RunStatus> = None;
        let (mut output, mut error) = (run.output.clone(), run.error.clone());
        // Every appended event, written in one statement at the end.
        let mut appended: Vec<(i64, String)> = Vec::with_capacity(c.events.len());
        let mut disposed: Vec<&str> = vec![];

        for kind in &c.events {
            match kind {
                EventKind::RunSucceeded { output: o } => {
                    output = Some(o.clone());
                    terminal = Some(RunStatus::Succeeded);
                }
                EventKind::RunFailed { error: e } => {
                    error = Some(e.clone());
                    terminal = Some(RunStatus::Failed);
                }
                EventKind::RunCancelled { .. } => terminal = Some(RunStatus::Cancelled),
                EventKind::HookDisposed { token, .. } => disposed.push(token),
                _ => {}
            }
            seq += 1;
            appended.push((seq, json(kind)?));
        }
        delete_in(&mut tx, "hooks", "token", run.id, &disposed).await?;

        let mut generated = vec![];
        let mut push_generated = |seq: i64, kind: EventKind| {
            generated.push(Event {
                run_id: run.id,
                seq: seq as u64,
                at: c.at,
                kind,
            });
        };

        for h in &c.hooks {
            let held: Option<String> = sqlx::query_scalar(
                "INSERT INTO hooks (namespace, token, run_id) VALUES (?, ?, ?)
                 ON CONFLICT (namespace, token) DO UPDATE SET run_id = excluded.run_id
                 WHERE hooks.run_id = excluded.run_id
                    OR EXISTS (SELECT 1 FROM runs r WHERE r.id = hooks.run_id
                                 AND r.status IN ('succeeded', 'failed', 'cancelled'))
                 RETURNING run_id",
            )
            .bind(&run.namespace)
            .bind(&h.token)
            .bind(id(run.id))
            .fetch_optional(&mut *tx)
            .await
            .map_err(be)?;
            let kind = EventKind::HookCreated {
                key: h.key.clone(),
                token: h.token.clone(),
                conflict: held.is_none(),
            };
            seq += 1;
            appended.push((seq, json(&kind)?));
            push_generated(seq, kind);
        }

        if terminal.is_none() {
            sync_waits(&mut tx, run.id, &c.waits).await?;

            // Consume matching inbox signals, oldest first.
            let mut pending: Vec<(String, String, Option<String>, String)> = sqlx::query(
                "SELECT id, name, target_key, payload FROM signals
                 WHERE run_id = ? AND consumed_seq IS NULL ORDER BY created_at, id",
            )
            .bind(id(run.id))
            .fetch_all(&mut *tx)
            .await
            .map_err(be)?
            .iter()
            .map(|r| {
                Ok((
                    r.try_get("id")?,
                    r.try_get("name")?,
                    r.try_get("target_key")?,
                    r.try_get("payload")?,
                ))
            })
            .collect::<std::result::Result<_, sqlx::Error>>()
            .map_err(be)?;

            let mut closed_keys = vec![];
            for w in &c.waits {
                let found = pending
                    .iter()
                    .position(|(_, name, target, _)| match w.kind {
                        WaitKind::Event => target.as_deref() == Some(w.key.as_str()),
                        _ => target.is_none() && *name == w.inbox_name(),
                    });
                let Some(i) = found else { continue };
                let (signal_id, _, _, payload) = pending.remove(i);
                seq += 1;
                sqlx::query("UPDATE signals SET consumed_seq = ? WHERE id = ?")
                    .bind(seq)
                    .bind(&signal_id)
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
                closed_keys.push(w.key.as_str());
                let kind = EventKind::SignalReceived {
                    key: w.key.clone(),
                    signal_id,
                    payload: from_json(&payload).map_err(be)?,
                };
                appended.push((seq, json(&kind)?));
                push_generated(seq, kind);
            }
            delete_in(&mut tx, "waits", "key", run.id, &closed_keys).await?;
        } else {
            for sql in [
                "DELETE FROM waits WHERE run_id = ?",
                "DELETE FROM hooks WHERE run_id = ?",
                "DELETE FROM signals WHERE run_id = ? AND consumed_seq IS NULL",
            ] {
                sqlx::query(sql)
                    .bind(id(run.id))
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
            }
        }

        insert_events(&mut tx, run.id, c.at, appended).await?;

        let (status, wake_at, awaiting): (RunStatus, Option<DateTime<Utc>>, Vec<String>) =
            match terminal {
                Some(s) => (s, None, vec![]),
                None if c.status == RunStatus::Waiting && !generated.is_empty() => {
                    // Something was just delivered: the engine keeps going.
                    (RunStatus::Running, None, vec![])
                }
                None => (c.status, c.wake_at, c.awaiting.clone()),
            };
        sqlx::query(
            "UPDATE runs SET status = ?, last_seq = ?, wake_at = ?, awaiting = ?, output = ?,
                             error = ?, last_error = ?, updated_at = ?,
                             has_waits = EXISTS (SELECT 1 FROM waits WHERE run_id = ?)
             WHERE id = ?",
        )
        .bind(status.as_str())
        .bind(seq)
        .bind(wake_at.map(us))
        .bind(json(&awaiting)?)
        .bind(output.as_ref().map(json).transpose()?)
        .bind(&error)
        .bind(&c.last_error)
        .bind(us(c.at))
        .bind(id(run.id))
        .bind(id(run.id))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(CommitOutcome {
            last_seq: seq as u64,
            generated,
            cancel_requested: run.cancel_requested_at.is_some(),
        })
    }

    async fn deliver_signal(
        &self,
        run_id: RunId,
        name: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        let run = load_live_run(&mut tx, run_id).await?;
        let item = deliver_tx(&mut tx, &run, name, payload, at).await?;
        tx.commit().await.map_err(be)?;
        Ok(item)
    }

    async fn resolve_hook(
        &self,
        namespace: &str,
        token: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let not_found = || Error::NotFound(format!("hook {token}"));
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        let run_id: String =
            sqlx::query_scalar("SELECT run_id FROM hooks WHERE namespace = ? AND token = ?")
                .bind(namespace)
                .bind(token)
                .fetch_optional(&mut *tx)
                .await
                .map_err(be)?
                .ok_or_else(not_found)?;
        let run_id = parse_id(&run_id).map_err(be)?;
        let run = load_live_run(&mut tx, run_id)
            .await
            .map_err(|_| not_found())?;
        let item = deliver_tx(&mut tx, &run, &format!("hook:{token}"), payload, at).await?;
        tx.commit().await.map_err(be)?;
        Ok(item)
    }

    async fn publish_event(
        &self,
        namespace: &str,
        name: &str,
        data: Value,
        at: DateTime<Utc>,
    ) -> Result<Vec<QueueItem>> {
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        // SQL narrows the waits by name; the filter is matched here.
        let open: Vec<(String, String)> = sqlx::query(
            "SELECT w.run_id, w.key, w.filter FROM waits w JOIN runs r ON r.id = w.run_id
             WHERE w.kind = 'event' AND w.name = ? AND r.namespace = ?
               AND r.status NOT IN ('succeeded', 'failed', 'cancelled')
             ORDER BY w.run_id, w.key",
        )
        .bind(name)
        .bind(namespace)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?
        .iter()
        .map(|r| {
            let filter: Option<Value> = opt_from_json(r.try_get("filter")?)?;
            Ok(filter
                .is_none_or(|f| contains(&data, &f))
                .then(|| Ok::<_, sqlx::Error>((r.try_get("run_id")?, r.try_get("key")?))))
        })
        .collect::<std::result::Result<Vec<_>, sqlx::Error>>()
        .map_err(be)?
        .into_iter()
        .flatten()
        .collect::<std::result::Result<_, _>>()
        .map_err(be)?;
        if open.is_empty() {
            return Ok(vec![]);
        }

        let (payload, created_at) = (json(&data)?, us(at));
        let signal_name = format!("event:{name}");
        QueryBuilder::<Sqlite>::new(
            "INSERT INTO signals (id, run_id, name, target_key, payload, created_at) ",
        )
        .push_values(&open, |mut b, (run_id, key)| {
            b.push_bind(id(Uuid::now_v7()))
                .push_bind(run_id.clone())
                .push_bind(signal_name.clone())
                .push_bind(key.clone())
                .push_bind(payload.clone())
                .push_bind(created_at);
        })
        .build()
        .execute(&mut *tx)
        .await
        .map_err(be)?;

        // Wake the waiting runs, once each; report one item per satisfied wait.
        let mut items = vec![];
        let mut run_ids: Vec<&str> = open.iter().map(|(run_id, _)| run_id.as_str()).collect();
        run_ids.dedup();
        for run_id in run_ids {
            let run = load_run(&mut tx, parse_id(run_id).map_err(be)?).await?;
            if run.status != RunStatus::Waiting {
                continue;
            }
            let wake = run.wake_at.map_or(at, |w| w.min(at));
            sqlx::query("UPDATE runs SET wake_at = ?, updated_at = ? WHERE id = ?")
                .bind(us(wake))
                .bind(us(at))
                .bind(run_id)
                .execute(&mut *tx)
                .await
                .map_err(be)?;
            let satisfied = open.iter().filter(|(r, _)| r == run_id).count();
            items.extend(std::iter::repeat_n(run.queue_item(wake), satisfied));
        }
        tx.commit().await.map_err(be)?;
        Ok(items)
    }

    async fn request_cancel(
        &self,
        run_id: RunId,
        reason: Option<String>,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        let run = load_live_run(&mut tx, run_id).await?;
        let wake = matches!(run.status, RunStatus::Waiting | RunStatus::Pending)
            .then(|| run.wake_at.map_or(at, |w| w.min(at)));
        sqlx::query(
            "UPDATE runs SET cancel_requested_at = COALESCE(cancel_requested_at, ?1),
                             cancel_reason = ?2, wake_at = COALESCE(?3, wake_at), updated_at = ?1
             WHERE id = ?4",
        )
        .bind(us(at))
        .bind(&reason)
        .bind(wake.map(us))
        .bind(id(run_id))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(wake.map(|w| run.queue_item(w)))
    }

    async fn retry_run(&self, run_id: RunId, at: DateTime<Utc>) -> Result<QueueItem> {
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        let run = load_run(&mut tx, run_id).await?;
        if run.status != RunStatus::Failed {
            return Err(Error::InvalidState(format!(
                "only failed runs can be retried, run is {}",
                run.status.as_str()
            )));
        }
        let seq = run.last_seq as i64 + 1;
        insert_events(
            &mut tx,
            run_id,
            at,
            vec![(seq, json(&EventKind::RunRetried)?)],
        )
        .await?;
        sqlx::query(
            "UPDATE runs SET status = 'pending', last_seq = ?1, error = NULL, last_error = NULL,
                             cancel_requested_at = NULL, cancel_reason = NULL, wake_at = ?2,
                             updated_at = ?2
             WHERE id = ?3",
        )
        .bind(seq)
        .bind(us(at))
        .bind(id(run_id))
        .execute(&mut *tx)
        .await
        .map_err(be)?;
        tx.commit().await.map_err(be)?;
        let reopened = Run {
            status: RunStatus::Pending,
            last_seq: seq as u64,
            ..run
        };
        Ok(reopened.queue_item(at))
    }

    async fn due_runs(
        &self,
        namespace: &str,
        before: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<QueueItem>> {
        let rows = sqlx::query(
            "SELECT * FROM runs
             WHERE namespace = ? AND wake_at <= ? AND status NOT IN ('succeeded', 'failed', 'cancelled')
             ORDER BY wake_at, id LIMIT ?",
        )
        .bind(namespace)
        .bind(us(before))
        .bind(limit as i64)
        .fetch_all(&mut *self.conn().await?)
        .await
        .map_err(be)?;
        rows.iter()
            .map(|r| {
                let run = run_from_row(r).map_err(be)?;
                Ok(run.queue_item(run.wake_at.expect("filtered on wake_at")))
            })
            .collect()
    }

    async fn upsert_definitions(
        &self,
        namespace: &str,
        defs: Vec<WorkflowDefinition>,
    ) -> Result<()> {
        if defs.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn().await?;
        let mut tx = conn.begin_with("BEGIN IMMEDIATE").await.map_err(be)?;
        for d in defs {
            sqlx::query(
                "INSERT INTO workflow_definitions (namespace, name, definition) VALUES (?, ?, ?)
                 ON CONFLICT (namespace, name) DO UPDATE SET definition = excluded.definition",
            )
            .bind(namespace)
            .bind(&d.name)
            .bind(json(&d)?)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }
        tx.commit().await.map_err(be)
    }

    async fn get_definition(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<WorkflowDefinition>> {
        let v: Option<String> = sqlx::query_scalar(
            "SELECT definition FROM workflow_definitions WHERE namespace = ? AND name = ?",
        )
        .bind(namespace)
        .bind(name)
        .fetch_optional(&mut *self.conn().await?)
        .await
        .map_err(be)?;
        v.map(|v| from_json(&v).map_err(be)).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn memory_urls() {
        for url in [
            "sqlite::memory:",
            "sqlite://:memory:",
            ":memory:",
            "sqlite://x.db?mode=memory",
        ] {
            assert!(is_memory_url(url), "{url}");
        }
        for url in [
            "sqlite://./durable.db",
            "sqlite:durable.db?mode=rwc",
            // `mode=memory` only counts as a query parameter.
            "sqlite:///data/mode=memory.db",
            "sqlite://x.db?vfs=mode=memory",
        ] {
            assert!(!is_memory_url(url), "{url}");
        }
    }

    #[test]
    fn read_only_urls() {
        for url in [
            "sqlite://x.db?mode=ro",
            "sqlite:x.db?cache=shared&immutable=true",
        ] {
            assert!(is_read_only_url(url), "{url}");
        }
        for url in [
            "sqlite://x.db",
            "sqlite://mode=ro.db",
            "sqlite:x.db?mode=rwc",
        ] {
            assert!(!is_read_only_url(url), "{url}");
        }
    }

    #[test]
    fn containment_matches_postgres() {
        let data = json!({"a": 1, "b": {"c": [1, 2, {"d": 3}]}, "e": "x"});
        assert!(contains(&data, &json!({})));
        assert!(contains(&data, &json!({"a": 1})));
        assert!(contains(&data, &json!({"b": {"c": [2]}})));
        assert!(contains(&data, &json!({"b": {"c": [{"d": 3}]}})));
        assert!(!contains(&data, &json!({"a": 2})));
        assert!(!contains(&data, &json!({"z": 1})));
        assert!(!contains(&data, &json!({"b": {"c": [4]}})));
    }

    #[test]
    fn in_memory_needs_no_runtime() {
        let _ = SqliteStore::in_memory();
    }
}
