use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_core::*;
use serde_json::Value;
use sqlx::{PgPool, Postgres, QueryBuilder, Row, Transaction, postgres::PgRow};
use uuid::Uuid;

use crate::{
    migrate,
    sql::{q, q_scalar},
};

fn be(e: sqlx::Error) -> Error {
    Error::Backend(e.to_string())
}

const TERMINAL: &str = "('succeeded', 'failed', 'cancelled')";

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
    schema: String,
}

impl PostgresStore {
    /// Reuse the host application's pool. Tables live in schema `durable`
    /// unless changed with [`schema`](Self::schema).
    pub fn from_pool(pool: PgPool) -> Self {
        Self {
            pool,
            schema: "durable".into(),
        }
    }

    pub async fn connect(url: &str) -> Result<Self> {
        Ok(Self::from_pool(PgPool::connect(url).await.map_err(be)?))
    }

    /// # Panics
    /// If `name` is not a plain identifier (it is interpolated into SQL).
    pub fn schema(mut self, name: &str) -> Self {
        let ok = !name.is_empty()
            && !name.starts_with(|c: char| c.is_ascii_digit())
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
        assert!(
            ok,
            "invalid schema name `{name}`: use letters, digits and underscores"
        );
        self.schema = name.to_string();
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create or upgrade the tables. Safe to call from every process at boot.
    pub async fn migrate(&self) -> Result<()> {
        migrate::run(&self.pool, &self.schema).await
    }

    fn t(&self, table: &str) -> String {
        format!("{}.{table}", self.schema)
    }
}

fn run_from_row(r: &PgRow) -> std::result::Result<Run, sqlx::Error> {
    let status: String = r.try_get("status")?;
    let trace_context: Option<Value> = r.try_get("trace_context")?;
    Ok(Run {
        id: r.try_get("id")?,
        namespace: r.try_get("namespace")?,
        workflow: r.try_get("workflow")?,
        idempotency_key: r.try_get("idempotency_key")?,
        status: status
            .parse()
            .map_err(|e: String| sqlx::Error::Decode(e.into()))?,
        input: r.try_get("input")?,
        output: r.try_get("output")?,
        error: r.try_get("error")?,
        last_seq: r.try_get::<i64, _>("last_seq")? as u64,
        wake_at: r.try_get("wake_at")?,
        awaiting: r.try_get("awaiting")?,
        concurrency_key: r.try_get("concurrency_key")?,
        throttle_key: r.try_get("throttle_key")?,
        deadline_at: r.try_get("deadline_at")?,
        cancel_requested_at: r.try_get("cancel_requested_at")?,
        cancel_reason: r.try_get("cancel_reason")?,
        last_error: r.try_get("last_error")?,
        trace_context: trace_context
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| sqlx::Error::Decode(Box::new(e)))?,
        created_at: r.try_get("created_at")?,
        updated_at: r.try_get("updated_at")?,
    })
}

fn kind_to_json(k: &EventKind) -> Result<Value> {
    serde_json::to_value(k).map_err(|e| Error::Backend(e.to_string()))
}

async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    events_table: &str,
    run_id: RunId,
    seq: i64,
    at: DateTime<Utc>,
    kind: &EventKind,
) -> Result<()> {
    q(format!(
        "INSERT INTO {events_table} (run_id, seq, at, kind) VALUES ($1, $2, $3, $4)"
    ))
    .bind(run_id)
    .bind(seq)
    .bind(at)
    .bind(kind_to_json(kind)?)
    .execute(&mut **tx)
    .await
    .map_err(be)?;
    Ok(())
}

impl PostgresStore {
    async fn lock_run(&self, tx: &mut Transaction<'_, Postgres>, id: RunId) -> Result<Run> {
        let row = q(format!(
            "SELECT * FROM {} WHERE id = $1 FOR UPDATE",
            self.t("runs")
        ))
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(be)?
        .ok_or_else(|| Error::NotFound(format!("run {id}")))?;
        run_from_row(&row).map_err(be)
    }

    async fn lock_live_run(&self, tx: &mut Transaction<'_, Postgres>, id: RunId) -> Result<Run> {
        let run = self.lock_run(tx, id).await?;
        if run.status.is_terminal() {
            return Err(Error::InvalidState(format!(
                "run {id} is {}",
                run.status.as_str()
            )));
        }
        Ok(run)
    }

    /// Put a signal in the inbox under the run lock; report whether to wake.
    async fn deliver_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run: &Run,
        name: &str,
        target_key: Option<&str>,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        q(format!(
            "INSERT INTO {} (id, run_id, name, target_key, payload, created_at)
             VALUES ($1, $2, $3, $4, $5, $6)",
            self.t("signals")
        ))
        .bind(Uuid::now_v7())
        .bind(run.id)
        .bind(name)
        .bind(target_key)
        .bind(payload)
        .bind(at)
        .execute(&mut **tx)
        .await
        .map_err(be)?;

        if run.status != RunStatus::Waiting {
            return Ok(None);
        }
        let waits = q(format!(
            "SELECT key, kind, name FROM {} WHERE run_id = $1",
            self.t("waits")
        ))
        .bind(run.id)
        .fetch_all(&mut **tx)
        .await
        .map_err(be)?;
        let satisfied = waits.iter().any(|w| {
            let key: String = w.get("key");
            let kind: String = w.get("kind");
            let wname: String = w.get("name");
            match target_key {
                Some(k) => key == k,
                None => inbox_name(&kind, &wname) == name,
            }
        });
        if !satisfied {
            return Ok(None);
        }
        let wake = run.wake_at.map_or(at, |w| w.min(at));
        q(format!(
            "UPDATE {} SET wake_at = $2, updated_at = $3 WHERE id = $1",
            self.t("runs")
        ))
        .bind(run.id)
        .bind(wake)
        .bind(at)
        .execute(&mut **tx)
        .await
        .map_err(be)?;
        Ok(Some(run.queue_item(wake)))
    }
}

impl PostgresStore {
    /// Make the stored open waits equal `waits`. Most commits (step results,
    /// timers) leave them unchanged, so they are only rewritten on a change.
    async fn sync_waits(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: RunId,
        waits: &[WaitReg],
    ) -> Result<()> {
        let table = self.t("waits");
        let stored: Vec<WaitRow> = q(format!(
            "SELECT key, kind, name, filter, timeout_at FROM {table} WHERE run_id = $1"
        ))
        .bind(run_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(be)?
        .iter()
        .map(|r| WaitRow {
            key: r.get("key"),
            kind: r.get("kind"),
            name: r.get("name"),
            filter: r.get("filter"),
            timeout_us: r
                .get::<Option<DateTime<Utc>>, _>("timeout_at")
                .map(|t| t.timestamp_micros()),
        })
        .collect();
        let wanted: Vec<WaitRow> = waits.iter().map(WaitRow::from).collect();
        if same_waits(stored, wanted) {
            return Ok(());
        }
        q(format!("DELETE FROM {table} WHERE run_id = $1"))
            .bind(run_id)
            .execute(&mut **tx)
            .await
            .map_err(be)?;
        if waits.is_empty() {
            return Ok(());
        }
        let keys: Vec<&str> = waits.iter().map(|w| w.key.as_str()).collect();
        let kinds: Vec<&str> = waits.iter().map(|w| wait_kind_str(w.kind)).collect();
        let names: Vec<&str> = waits.iter().map(|w| w.name.as_str()).collect();
        let filters: Vec<Option<Value>> = waits.iter().map(|w| w.filter.clone()).collect();
        let timeouts: Vec<Option<DateTime<Utc>>> = waits.iter().map(|w| w.timeout_at).collect();
        q(format!(
            "INSERT INTO {table} (run_id, key, kind, name, filter, timeout_at)
             SELECT $1, w.key, w.kind, w.name, w.filter, w.timeout_at
             FROM UNNEST($2::text[], $3::text[], $4::text[], $5::jsonb[], $6::timestamptz[])
                  AS w(key, kind, name, filter, timeout_at)"
        ))
        .bind(run_id)
        .bind(&keys)
        .bind(&kinds)
        .bind(&names)
        .bind(&filters)
        .bind(&timeouts)
        .execute(&mut **tx)
        .await
        .map_err(be)?;
        Ok(())
    }
}

/// A wait as stored. Timestamps compare at Postgres' microsecond precision.
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
            timeout_us: w.timeout_at.map(|t| t.timestamp_micros()),
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

#[async_trait]
impl Store for PostgresStore {
    async fn create_run(&self, new: NewRun) -> Result<CreateOutcome> {
        let id = Uuid::now_v7();
        let mut tx = self.pool.begin().await.map_err(be)?;
        let inserted = q(format!(
            "INSERT INTO {runs} (id, namespace, workflow, idempotency_key, status, input, last_seq,
                                 wake_at, concurrency_key, throttle_key, deadline_at, trace_context,
                                 created_at, updated_at)
             VALUES ($1, $2, $3, $4, 'pending', $5, 1, $6, $7, $8, $9, $10, $6, $6)
             ON CONFLICT (namespace, workflow, idempotency_key) WHERE idempotency_key IS NOT NULL
             DO NOTHING
             RETURNING *",
            runs = self.t("runs")
        ))
        .bind(id)
        .bind(&new.namespace)
        .bind(&new.workflow)
        .bind(&new.idempotency_key)
        .bind(&new.input)
        .bind(new.at)
        .bind(&new.concurrency_key)
        .bind(&new.throttle_key)
        .bind(new.deadline_at)
        .bind(
            new.trace_context
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|e| Error::Backend(e.to_string()))?,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(be)?;

        match inserted {
            Some(row) => {
                let run = run_from_row(&row).map_err(be)?;
                insert_event(
                    &mut tx,
                    &self.t("run_events"),
                    id,
                    1,
                    new.at,
                    &EventKind::RunStarted { input: new.input },
                )
                .await?;
                tx.commit().await.map_err(be)?;
                Ok(CreateOutcome::Created(run))
            }
            None => {
                let row = q(format!(
                    "SELECT * FROM {} WHERE namespace = $1 AND workflow = $2 AND idempotency_key = $3",
                    self.t("runs")
                ))
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

    async fn get_run(&self, id: RunId) -> Result<Option<Run>> {
        let row = q(format!("SELECT * FROM {} WHERE id = $1", self.t("runs")))
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(be)?;
        row.map(|r| run_from_row(&r).map_err(be)).transpose()
    }

    async fn list_runs(&self, f: RunFilter) -> Result<Vec<Run>> {
        let mut q =
            QueryBuilder::<Postgres>::new(format!("SELECT * FROM {} WHERE true", self.t("runs")));
        if let Some(ns) = &f.namespace {
            q.push(" AND namespace = ").push_bind(ns);
        }
        if let Some(wf) = &f.workflow {
            q.push(" AND workflow = ").push_bind(wf);
        }
        if let Some(s) = f.status {
            q.push(" AND status = ").push_bind(s.as_str());
        }
        if let Some(after) = f.after {
            q.push(format!(
                " AND (created_at, id) < (SELECT created_at, id FROM {} WHERE id = ",
                self.t("runs")
            ))
            .push_bind(after)
            .push(")");
        }
        q.push(" ORDER BY created_at DESC, id DESC");
        if let Some(limit) = f.limit {
            q.push(" LIMIT ").push_bind(limit as i64);
        }
        let rows = q.build().fetch_all(&self.pool).await.map_err(be)?;
        rows.iter().map(|r| run_from_row(r).map_err(be)).collect()
    }

    async fn load_events(&self, id: RunId, after_seq: u64) -> Result<Vec<Event>> {
        let rows = q(format!(
            "SELECT seq, at, kind FROM {} WHERE run_id = $1 AND seq > $2 ORDER BY seq",
            self.t("run_events")
        ))
        .bind(id)
        .bind(after_seq as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(be)?;
        if rows.is_empty() && self.get_run(id).await?.is_none() {
            return Err(Error::NotFound(format!("run {id}")));
        }
        rows.iter()
            .map(|r| {
                let kind: Value = r.get("kind");
                Ok(Event {
                    run_id: id,
                    seq: r.get::<i64, _>("seq") as u64,
                    at: r.get("at"),
                    kind: serde_json::from_value(kind)
                        .map_err(|e| Error::Backend(e.to_string()))?,
                })
            })
            .collect()
    }

    async fn commit(&self, c: Commit) -> Result<CommitOutcome> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        let run = self.lock_run(&mut tx, c.run_id).await?;
        if run.last_seq != c.expected_seq || run.status.is_terminal() {
            return Err(Error::Conflict);
        }
        let mut seq = run.last_seq as i64;
        let mut terminal: Option<RunStatus> = None;
        let (mut output, mut error) = (run.output.clone(), run.error.clone());
        // Every appended event, written in one statement at the end.
        let mut appended: Vec<(i64, Value)> = Vec::with_capacity(c.events.len());
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
            appended.push((seq, kind_to_json(kind)?));
        }
        if !disposed.is_empty() {
            q(format!(
                "DELETE FROM {} WHERE run_id = $1 AND token = ANY($2)",
                self.t("hooks")
            ))
            .bind(run.id)
            .bind(&disposed)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }

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
            let held = q(format!(
                "INSERT INTO {hooks} (namespace, token, run_id) VALUES ($3, $1, $2)
                 ON CONFLICT (namespace, token) DO UPDATE SET run_id = EXCLUDED.run_id
                 WHERE {hooks}.run_id = EXCLUDED.run_id
                    OR EXISTS (SELECT 1 FROM {runs} r WHERE r.id = {hooks}.run_id AND r.status IN {TERMINAL})
                 RETURNING run_id",
                hooks = self.t("hooks"),
                runs = self.t("runs"),
            ))
            .bind(&h.token)
            .bind(run.id)
            .bind(&run.namespace)
            .fetch_optional(&mut *tx)
            .await
            .map_err(be)?;
            let kind = EventKind::HookCreated {
                key: h.key.clone(),
                token: h.token.clone(),
                conflict: held.is_none(),
            };
            seq += 1;
            appended.push((seq, kind_to_json(&kind)?));
            push_generated(seq, kind);
        }

        let waits_table = self.t("waits");
        if terminal.is_none() {
            self.sync_waits(&mut tx, run.id, &c.waits).await?;

            // Consume matching inbox signals, oldest first.
            let signals_table = self.t("signals");
            let mut pending: Vec<(Uuid, String, Option<String>, Value)> = q(format!(
                "SELECT id, name, target_key, payload FROM {signals_table}
                 WHERE run_id = $1 AND consumed_seq IS NULL ORDER BY created_at, id FOR UPDATE"
            ))
            .bind(run.id)
            .fetch_all(&mut *tx)
            .await
            .map_err(be)?
            .iter()
            .map(|r| {
                (
                    r.get("id"),
                    r.get("name"),
                    r.get("target_key"),
                    r.get("payload"),
                )
            })
            .collect();

            let (mut consumed_ids, mut consumed_seqs, mut closed_keys) = (vec![], vec![], vec![]);
            for w in &c.waits {
                let found = pending
                    .iter()
                    .position(|(_, name, target, _)| match w.kind {
                        WaitKind::Event => target.as_deref() == Some(w.key.as_str()),
                        _ => target.is_none() && *name == w.inbox_name(),
                    });
                let Some(i) = found else { continue };
                let (sid, _, _, payload) = pending.remove(i);
                seq += 1;
                consumed_ids.push(sid);
                consumed_seqs.push(seq);
                closed_keys.push(w.key.as_str());
                let kind = EventKind::SignalReceived {
                    key: w.key.clone(),
                    signal_id: sid.to_string(),
                    payload,
                };
                appended.push((seq, kind_to_json(&kind)?));
                push_generated(seq, kind);
            }
            if !consumed_ids.is_empty() {
                q(format!(
                    "UPDATE {signals_table} s SET consumed_seq = d.seq
                     FROM UNNEST($1::uuid[], $2::bigint[]) AS d(id, seq) WHERE s.id = d.id"
                ))
                .bind(&consumed_ids)
                .bind(&consumed_seqs)
                .execute(&mut *tx)
                .await
                .map_err(be)?;
                q(format!(
                    "DELETE FROM {waits_table} WHERE run_id = $1 AND key = ANY($2)"
                ))
                .bind(run.id)
                .bind(&closed_keys)
                .execute(&mut *tx)
                .await
                .map_err(be)?;
            }
        } else {
            for table in ["waits", "hooks"] {
                q(format!("DELETE FROM {} WHERE run_id = $1", self.t(table)))
                    .bind(run.id)
                    .execute(&mut *tx)
                    .await
                    .map_err(be)?;
            }
            q(format!(
                "DELETE FROM {} WHERE run_id = $1 AND consumed_seq IS NULL",
                self.t("signals")
            ))
            .bind(run.id)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }

        if !appended.is_empty() {
            let (seqs, kinds): (Vec<i64>, Vec<Value>) = appended.into_iter().unzip();
            q(format!(
                "INSERT INTO {} (run_id, seq, at, kind)
                 SELECT $1, e.seq, $2, e.kind FROM UNNEST($3::bigint[], $4::jsonb[]) AS e(seq, kind)",
                self.t("run_events")
            ))
            .bind(run.id)
            .bind(c.at)
            .bind(&seqs)
            .bind(&kinds)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }

        let (status, wake_at, awaiting): (RunStatus, Option<DateTime<Utc>>, Vec<String>) =
            match terminal {
                Some(s) => (s, None, vec![]),
                None if c.status == RunStatus::Waiting && !generated.is_empty() => {
                    // Something was just delivered: the engine keeps going.
                    (RunStatus::Running, None, vec![])
                }
                None => (c.status, c.wake_at, c.awaiting.clone()),
            };
        q(format!(
            "UPDATE {} SET status = $2, last_seq = $3, wake_at = $4, awaiting = $5, output = $6,
                            error = $7, last_error = $8, updated_at = $9
             WHERE id = $1",
            self.t("runs")
        ))
        .bind(run.id)
        .bind(status.as_str())
        .bind(seq)
        .bind(wake_at)
        .bind(&awaiting)
        .bind(&output)
        .bind(&error)
        .bind(&c.last_error)
        .bind(c.at)
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
        let mut tx = self.pool.begin().await.map_err(be)?;
        let run = self.lock_live_run(&mut tx, run_id).await?;
        let item = self
            .deliver_tx(&mut tx, &run, name, None, payload, at)
            .await?;
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
        let run_id: Uuid = q_scalar(format!(
            "SELECT run_id FROM {} WHERE namespace = $1 AND token = $2",
            self.t("hooks")
        ))
        .bind(namespace)
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .map_err(be)?
        .ok_or_else(not_found)?;
        let mut tx = self.pool.begin().await.map_err(be)?;
        let run = self
            .lock_live_run(&mut tx, run_id)
            .await
            .map_err(|_| not_found())?;
        let item = self
            .deliver_tx(&mut tx, &run, &format!("hook:{token}"), None, payload, at)
            .await?;
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
        let mut tx = self.pool.begin().await.map_err(be)?;
        // Lock every candidate run at once, in id order so concurrent
        // publishers cannot deadlock.
        let runs: Vec<Run> = q(format!(
            "SELECT * FROM {runs} WHERE namespace = $2 AND status NOT IN {TERMINAL}
               AND id IN (SELECT run_id FROM {waits}
                          WHERE kind = 'event' AND name = $1
                            AND (filter IS NULL OR $3::jsonb @> filter))
             ORDER BY id FOR UPDATE",
            waits = self.t("waits"),
            runs = self.t("runs"),
        ))
        .bind(name)
        .bind(namespace)
        .bind(&data)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?
        .iter()
        .map(run_from_row)
        .collect::<std::result::Result<_, _>>()
        .map_err(be)?;
        if runs.is_empty() {
            return Ok(vec![]);
        }

        // Re-read the waits under the locks: commits change them only while
        // holding the run lock, so this view is stable until we commit.
        let ids: Vec<Uuid> = runs.iter().map(|r| r.id).collect();
        let open: Vec<(Uuid, String)> = q(format!(
            "SELECT run_id, key FROM {} WHERE run_id = ANY($1) AND kind = 'event' AND name = $2
               AND (filter IS NULL OR $3::jsonb @> filter)
             ORDER BY run_id, key",
            self.t("waits")
        ))
        .bind(&ids)
        .bind(name)
        .bind(&data)
        .fetch_all(&mut *tx)
        .await
        .map_err(be)?
        .iter()
        .map(|r| (r.get("run_id"), r.get("key")))
        .collect();
        if open.is_empty() {
            return Ok(vec![]);
        }

        let (run_ids, keys): (Vec<Uuid>, Vec<String>) = open.iter().cloned().unzip();
        let signal_ids: Vec<Uuid> = open.iter().map(|_| Uuid::now_v7()).collect();
        q(format!(
            "INSERT INTO {} (id, run_id, name, target_key, payload, created_at)
             SELECT s.id, s.run_id, $4, s.target_key, $5, $6
             FROM UNNEST($1::uuid[], $2::uuid[], $3::text[]) AS s(id, run_id, target_key)",
            self.t("signals")
        ))
        .bind(&signal_ids)
        .bind(&run_ids)
        .bind(&keys)
        .bind(format!("event:{name}"))
        .bind(&data)
        .bind(at)
        .execute(&mut *tx)
        .await
        .map_err(be)?;

        // Wake the waiting runs, once each; report one item per satisfied wait.
        let waiting: Vec<&Run> = runs
            .iter()
            .filter(|r| r.status == RunStatus::Waiting && run_ids.contains(&r.id))
            .collect();
        let wakes: Vec<(Uuid, DateTime<Utc>)> = waiting
            .iter()
            .map(|r| (r.id, r.wake_at.map_or(at, |w| w.min(at))))
            .collect();
        if !wakes.is_empty() {
            let (wake_ids, wake_ats): (Vec<Uuid>, Vec<DateTime<Utc>>) =
                wakes.iter().copied().unzip();
            q(format!(
                "UPDATE {} r SET wake_at = w.at, updated_at = $3
                 FROM UNNEST($1::uuid[], $2::timestamptz[]) AS w(id, at) WHERE r.id = w.id",
                self.t("runs")
            ))
            .bind(&wake_ids)
            .bind(&wake_ats)
            .bind(at)
            .execute(&mut *tx)
            .await
            .map_err(be)?;
        }
        tx.commit().await.map_err(be)?;

        let mut items = vec![];
        for (run, (_, wake)) in waiting.iter().zip(&wakes) {
            let satisfied = run_ids.iter().filter(|id| **id == run.id).count();
            items.extend(std::iter::repeat_n(run.queue_item(*wake), satisfied));
        }
        Ok(items)
    }

    async fn request_cancel(
        &self,
        run_id: RunId,
        reason: Option<String>,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        let run = self.lock_live_run(&mut tx, run_id).await?;
        let wake = matches!(run.status, RunStatus::Waiting | RunStatus::Pending)
            .then(|| run.wake_at.map_or(at, |w| w.min(at)));
        q(format!(
            "UPDATE {} SET cancel_requested_at = COALESCE(cancel_requested_at, $2),
                            cancel_reason = $3, wake_at = COALESCE($4, wake_at), updated_at = $2
             WHERE id = $1",
            self.t("runs")
        ))
        .bind(run_id)
        .bind(at)
        .bind(&reason)
        .bind(wake)
        .execute(&mut *tx)
        .await
        .map_err(be)?;
        tx.commit().await.map_err(be)?;
        Ok(wake.map(|w| run.queue_item(w)))
    }

    async fn retry_run(&self, run_id: RunId, at: DateTime<Utc>) -> Result<QueueItem> {
        let mut tx = self.pool.begin().await.map_err(be)?;
        let run = self.lock_run(&mut tx, run_id).await?;
        if run.status != RunStatus::Failed {
            return Err(Error::InvalidState(format!(
                "only failed runs can be retried, run is {}",
                run.status.as_str()
            )));
        }
        let seq = run.last_seq as i64 + 1;
        insert_event(
            &mut tx,
            &self.t("run_events"),
            run_id,
            seq,
            at,
            &EventKind::RunRetried,
        )
        .await?;
        q(format!(
            "UPDATE {} SET status = 'pending', last_seq = $2, error = NULL, last_error = NULL,
                            cancel_requested_at = NULL, cancel_reason = NULL, wake_at = $3,
                            updated_at = $3
             WHERE id = $1",
            self.t("runs")
        ))
        .bind(run_id)
        .bind(seq)
        .bind(at)
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
        let rows = q(format!(
            "SELECT * FROM {} WHERE namespace = $1 AND wake_at <= $2 AND status NOT IN {TERMINAL}
             ORDER BY wake_at, id LIMIT $3",
            self.t("runs")
        ))
        .bind(namespace)
        .bind(before)
        .bind(limit as i64)
        .fetch_all(&self.pool)
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
        for d in defs {
            q(format!(
                "INSERT INTO {} (namespace, name, definition) VALUES ($1, $2, $3)
                 ON CONFLICT (namespace, name) DO UPDATE SET definition = EXCLUDED.definition",
                self.t("workflow_definitions")
            ))
            .bind(namespace)
            .bind(&d.name)
            .bind(serde_json::to_value(&d).map_err(|e| Error::Backend(e.to_string()))?)
            .execute(&self.pool)
            .await
            .map_err(be)?;
        }
        Ok(())
    }

    async fn get_definition(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<WorkflowDefinition>> {
        let v: Option<Value> = q_scalar(format!(
            "SELECT definition FROM {} WHERE namespace = $1 AND name = $2",
            self.t("workflow_definitions")
        ))
        .bind(namespace)
        .bind(name)
        .fetch_optional(&self.pool)
        .await
        .map_err(be)?;
        v.map(|v| serde_json::from_value(v).map_err(|e| Error::Backend(e.to_string())))
            .transpose()
    }
}
