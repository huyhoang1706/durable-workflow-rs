use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_core::*;
use parking_lot::Mutex;
use serde_json::Value;
use uuid::Uuid;

struct InboxEntry {
    signal_id: String,
    name: String,
    /// Event signals are addressed to the specific wait that matched.
    target_key: Option<String>,
    payload: Value,
}

struct RunRec {
    run: Run,
    events: Vec<Event>,
    inbox: Vec<InboxEntry>,
    waits: Vec<WaitReg>,
}

#[derive(Default)]
struct Inner {
    runs: HashMap<RunId, RunRec>,
    /// Creation order, oldest first.
    order: Vec<RunId>,
    idempotency: HashMap<(String, String, String), RunId>,
    /// `(namespace, token)` to the run holding it.
    hooks: HashMap<(String, String), RunId>,
    /// `(namespace, workflow)`.
    definitions: HashMap<(String, String), WorkflowDefinition>,
}

#[derive(Default)]
pub struct MemoryStore {
    inner: Mutex<Inner>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
}

fn push_event(rec: &mut RunRec, kind: EventKind, at: DateTime<Utc>) -> Event {
    rec.run.last_seq += 1;
    let ev = Event {
        run_id: rec.run.id,
        seq: rec.run.last_seq,
        at,
        kind,
    };
    rec.events.push(ev.clone());
    ev
}

/// JSONB `@>` semantics.
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

/// Put a signal in the inbox; report whether the run must be woken now.
fn deliver(
    rec: &mut RunRec,
    name: String,
    target_key: Option<String>,
    payload: Value,
    at: DateTime<Utc>,
) -> Option<QueueItem> {
    rec.inbox.push(InboxEntry {
        signal_id: Uuid::now_v7().to_string(),
        name: name.clone(),
        target_key: target_key.clone(),
        payload,
    });
    let satisfies = |w: &WaitReg| match &target_key {
        Some(k) => w.key == *k,
        None => w.inbox_name() == name,
    };
    if rec.run.status == RunStatus::Waiting && rec.waits.iter().any(satisfies) {
        let wake = rec.run.wake_at.map_or(at, |w| w.min(at));
        rec.run.wake_at = Some(wake);
        return Some(rec.run.queue_item(wake));
    }
    None
}

fn live_run(inner: &mut Inner, id: RunId) -> Result<&mut RunRec> {
    let rec = inner
        .runs
        .get_mut(&id)
        .ok_or_else(|| Error::NotFound(format!("run {id}")))?;
    if rec.run.status.is_terminal() {
        return Err(Error::InvalidState(format!(
            "run {id} is {}",
            rec.run.status.as_str()
        )));
    }
    Ok(rec)
}

#[async_trait]
impl Store for MemoryStore {
    async fn create_run(&self, new: NewRun) -> Result<CreateOutcome> {
        let mut inner = self.inner.lock();
        let idem = new
            .idempotency_key
            .clone()
            .map(|k| (new.namespace.clone(), new.workflow.clone(), k));
        if let Some(existing) = idem.as_ref().and_then(|k| inner.idempotency.get(k)) {
            return Ok(CreateOutcome::Existing(inner.runs[existing].run.clone()));
        }
        let id = Uuid::now_v7();
        let run = Run {
            id,
            namespace: new.namespace,
            workflow: new.workflow,
            idempotency_key: new.idempotency_key,
            status: RunStatus::Pending,
            input: new.input.clone(),
            output: None,
            error: None,
            last_seq: 0,
            wake_at: Some(new.at),
            awaiting: vec![],
            concurrency_key: new.concurrency_key,
            throttle_key: new.throttle_key,
            deadline_at: new.deadline_at,
            cancel_requested_at: None,
            cancel_reason: None,
            last_error: None,
            trace_context: new.trace_context,
            created_at: new.at,
            updated_at: new.at,
        };
        let mut rec = RunRec {
            run,
            events: vec![],
            inbox: vec![],
            waits: vec![],
        };
        push_event(&mut rec, EventKind::RunStarted { input: new.input }, new.at);
        let out = rec.run.clone();
        inner.runs.insert(id, rec);
        inner.order.push(id);
        if let Some(k) = idem {
            inner.idempotency.insert(k, id);
        }
        Ok(CreateOutcome::Created(out))
    }

    async fn get_run(&self, id: RunId) -> Result<Option<Run>> {
        Ok(self.inner.lock().runs.get(&id).map(|r| r.run.clone()))
    }

    async fn list_runs(&self, f: RunFilter) -> Result<Vec<Run>> {
        let inner = self.inner.lock();
        let mut ids: Vec<_> = inner.order.iter().rev().copied().collect();
        if let Some(after) = f.after {
            let pos = ids.iter().position(|i| *i == after);
            ids = ids.split_off(pos.map_or(0, |p| p + 1));
        }
        Ok(ids
            .into_iter()
            .map(|id| &inner.runs[&id].run)
            .filter(|r| f.namespace.as_ref().is_none_or(|n| &r.namespace == n))
            .filter(|r| f.workflow.as_ref().is_none_or(|w| &r.workflow == w))
            .filter(|r| f.status.is_none_or(|s| r.status == s))
            .take(f.limit.unwrap_or(usize::MAX))
            .cloned()
            .collect())
    }

    async fn load_events(&self, id: RunId, after_seq: u64) -> Result<Vec<Event>> {
        let inner = self.inner.lock();
        let rec = inner
            .runs
            .get(&id)
            .ok_or_else(|| Error::NotFound(format!("run {id}")))?;
        Ok(rec
            .events
            .iter()
            .filter(|e| e.seq > after_seq)
            .cloned()
            .collect())
    }

    async fn commit(&self, c: Commit) -> Result<CommitOutcome> {
        let mut inner = self.inner.lock();
        let Inner { runs, hooks, .. } = &mut *inner;
        let rec = runs
            .get_mut(&c.run_id)
            .ok_or_else(|| Error::NotFound(format!("run {}", c.run_id)))?;
        if rec.run.last_seq != c.expected_seq || rec.run.status.is_terminal() {
            return Err(Error::Conflict);
        }
        let ns = rec.run.namespace.clone();
        let hook_key = |token: &str| (ns.clone(), token.to_string());

        let mut terminal = None;
        for kind in c.events {
            match &kind {
                EventKind::RunSucceeded { output } => {
                    rec.run.output = Some(output.clone());
                    terminal = Some(RunStatus::Succeeded);
                }
                EventKind::RunFailed { error } => {
                    rec.run.error = Some(error.clone());
                    terminal = Some(RunStatus::Failed);
                }
                EventKind::RunCancelled { .. } => terminal = Some(RunStatus::Cancelled),
                EventKind::HookDisposed { token, .. }
                    if hooks.get(&hook_key(token)) == Some(&c.run_id) =>
                {
                    hooks.remove(&hook_key(token));
                }
                _ => {}
            }
            push_event(rec, kind, c.at);
        }

        let mut generated = vec![];
        for h in c.hooks {
            let holder = hooks.get(&hook_key(&h.token)).copied();
            let conflict = holder.is_some_and(|other| {
                other != c.run_id
                    && !matches!(runs.get(&other), Some(r) if r.run.status.is_terminal())
            });
            let rec = runs.get_mut(&c.run_id).unwrap();
            if !conflict {
                hooks.insert(hook_key(&h.token), c.run_id);
            }
            let kind = EventKind::HookCreated {
                key: h.key,
                token: h.token,
                conflict,
            };
            generated.push(push_event(rec, kind, c.at));
        }
        let rec = runs.get_mut(&c.run_id).unwrap();

        rec.waits = c.waits;
        if terminal.is_none() {
            let mut remaining = vec![];
            for w in std::mem::take(&mut rec.waits) {
                let found = rec.inbox.iter().position(|e| match w.kind {
                    WaitKind::Event => e.target_key.as_deref() == Some(&w.key),
                    _ => e.target_key.is_none() && e.name == w.inbox_name(),
                });
                match found {
                    Some(i) => {
                        let e = rec.inbox.remove(i);
                        let kind = EventKind::SignalReceived {
                            key: w.key,
                            signal_id: e.signal_id,
                            payload: e.payload,
                        };
                        generated.push(push_event(rec, kind, c.at));
                    }
                    None => remaining.push(w),
                }
            }
            rec.waits = remaining;
        }

        rec.run.updated_at = c.at;
        rec.run.last_error = c.last_error;
        match terminal {
            Some(status) => {
                rec.run.status = status;
                rec.run.wake_at = None;
                rec.run.awaiting.clear();
                rec.waits.clear();
                rec.inbox.clear();
            }
            None if c.status == RunStatus::Waiting && !generated.is_empty() => {
                // Something was just delivered: the engine keeps going.
                rec.run.status = RunStatus::Running;
                rec.run.wake_at = None;
                rec.run.awaiting.clear();
            }
            None => {
                rec.run.status = c.status;
                rec.run.wake_at = c.wake_at;
                rec.run.awaiting = c.awaiting;
            }
        }
        Ok(CommitOutcome {
            last_seq: rec.run.last_seq,
            generated,
            cancel_requested: rec.run.cancel_requested_at.is_some(),
        })
    }

    async fn deliver_signal(
        &self,
        run_id: RunId,
        name: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let mut inner = self.inner.lock();
        let rec = live_run(&mut inner, run_id)?;
        Ok(deliver(rec, name.to_string(), None, payload, at))
    }

    async fn resolve_hook(
        &self,
        namespace: &str,
        token: &str,
        payload: Value,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let mut inner = self.inner.lock();
        let run_id = inner
            .hooks
            .get(&(namespace.to_string(), token.to_string()))
            .copied()
            .ok_or_else(|| Error::NotFound(format!("hook {token}")))?;
        let rec =
            live_run(&mut inner, run_id).map_err(|_| Error::NotFound(format!("hook {token}")))?;
        Ok(deliver(rec, format!("hook:{token}"), None, payload, at))
    }

    async fn publish_event(
        &self,
        namespace: &str,
        name: &str,
        data: Value,
        at: DateTime<Utc>,
    ) -> Result<Vec<QueueItem>> {
        let mut inner = self.inner.lock();
        let mut items = vec![];
        for rec in inner.runs.values_mut() {
            if rec.run.namespace != namespace || rec.run.status.is_terminal() {
                continue;
            }
            let matching: Vec<_> = rec
                .waits
                .iter()
                .filter(|w| w.kind == WaitKind::Event && w.name == name)
                .filter(|w| w.filter.as_ref().is_none_or(|f| contains(&data, f)))
                .map(|w| w.key.clone())
                .collect();
            for key in matching {
                if let Some(item) =
                    deliver(rec, format!("event:{name}"), Some(key), data.clone(), at)
                {
                    items.push(item);
                }
            }
        }
        Ok(items)
    }

    async fn request_cancel(
        &self,
        run_id: RunId,
        reason: Option<String>,
        at: DateTime<Utc>,
    ) -> Result<Option<QueueItem>> {
        let mut inner = self.inner.lock();
        let rec = live_run(&mut inner, run_id)?;
        rec.run.cancel_requested_at.get_or_insert(at);
        rec.run.cancel_reason = reason;
        if matches!(rec.run.status, RunStatus::Waiting | RunStatus::Pending) {
            let wake = rec.run.wake_at.map_or(at, |w| w.min(at));
            rec.run.wake_at = Some(wake);
            return Ok(Some(rec.run.queue_item(wake)));
        }
        Ok(None)
    }

    async fn retry_run(&self, run_id: RunId, at: DateTime<Utc>) -> Result<QueueItem> {
        let mut inner = self.inner.lock();
        let rec = inner
            .runs
            .get_mut(&run_id)
            .ok_or_else(|| Error::NotFound(format!("run {run_id}")))?;
        if rec.run.status != RunStatus::Failed {
            return Err(Error::InvalidState(format!(
                "only failed runs can be retried, run is {}",
                rec.run.status.as_str()
            )));
        }
        push_event(rec, EventKind::RunRetried, at);
        let r = &mut rec.run;
        r.status = RunStatus::Pending;
        r.error = None;
        r.last_error = None;
        r.cancel_requested_at = None;
        r.cancel_reason = None;
        r.wake_at = Some(at);
        r.updated_at = at;
        Ok(r.queue_item(at))
    }

    async fn due_runs(
        &self,
        namespace: &str,
        before: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<QueueItem>> {
        let inner = self.inner.lock();
        let mut due: Vec<_> = inner
            .runs
            .values()
            .map(|r| &r.run)
            .filter(|r| r.namespace == namespace && !r.status.is_terminal())
            .filter_map(|r| r.wake_at.filter(|w| *w <= before).map(|w| (w, r)))
            .collect();
        due.sort_by_key(|(w, r)| (*w, r.id));
        Ok(due
            .into_iter()
            .take(limit)
            .map(|(w, r)| r.queue_item(w))
            .collect())
    }

    async fn upsert_definitions(
        &self,
        namespace: &str,
        defs: Vec<WorkflowDefinition>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        for d in defs {
            inner
                .definitions
                .insert((namespace.to_string(), d.name.clone()), d);
        }
        Ok(())
    }

    async fn get_definition(
        &self,
        namespace: &str,
        name: &str,
    ) -> Result<Option<WorkflowDefinition>> {
        let key = (namespace.to_string(), name.to_string());
        Ok(self.inner.lock().definitions.get(&key).cloned())
    }
}
