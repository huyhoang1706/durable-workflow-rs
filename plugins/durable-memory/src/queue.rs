use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_spi::*;
use parking_lot::Mutex;
use uuid::Uuid;

use crate::gcra::gcra;

#[derive(Debug)]
enum State {
    Queued,
    Leased {
        token: String,
        expires_at: DateTime<Utc>,
        /// Enqueued while executing: re-run after ack.
        rerun: Option<QueueItem>,
    },
}

#[derive(Debug)]
struct Entry {
    item: QueueItem,
    state: State,
}

#[derive(Default)]
struct Inner {
    entries: HashMap<RunId, Entry>,
    /// GCRA state per (scope, throttle key).
    throttle: HashMap<(QueueScope, String), DateTime<Utc>>,
}

#[derive(Default)]
pub struct MemoryQueue {
    inner: Mutex<Inner>,
}

impl MemoryQueue {
    pub fn new() -> Self {
        Self::default()
    }
}

fn in_scope(item: &QueueItem, scope: &QueueScope) -> bool {
    item.namespace == scope.namespace && item.workflow == scope.workflow
}

/// Merge `new` into `old`: earliest due time wins; any resume upgrades a start.
fn merge(old: &mut QueueItem, new: &QueueItem) {
    old.due_at = old.due_at.min(new.due_at);
    if new.kind == WorkKind::Resume {
        old.kind = WorkKind::Resume;
    }
}

#[async_trait]
impl Queue for MemoryQueue {
    async fn enqueue(&self, items: Vec<QueueItem>) -> Result<()> {
        let mut inner = self.inner.lock();
        for item in items {
            match inner.entries.get_mut(&item.run_id) {
                None => {
                    inner.entries.insert(
                        item.run_id,
                        Entry {
                            item,
                            state: State::Queued,
                        },
                    );
                }
                Some(Entry {
                    item: queued,
                    state: State::Queued,
                }) => merge(queued, &item),
                Some(Entry {
                    state: State::Leased { rerun, .. },
                    ..
                }) => match rerun {
                    Some(r) => merge(r, &item),
                    None => {
                        *rerun = Some(QueueItem {
                            kind: WorkKind::Resume,
                            ..item
                        })
                    }
                },
            }
        }
        Ok(())
    }

    async fn claim(
        &self,
        scope: &QueueScope,
        limits: &ClaimLimits,
        max: usize,
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Claimed> {
        let mut inner = self.inner.lock();
        let mut inflight_total = 0u32;
        let mut inflight_by_key: HashMap<String, u32> = HashMap::new();
        let mut candidates = vec![];
        let mut next_due: Option<DateTime<Utc>> = None;
        let note_due = |t: DateTime<Utc>, next: &mut Option<DateTime<Utc>>| {
            *next = Some(next.map_or(t, |n| n.min(t)));
        };

        for (id, e) in &inner.entries {
            if !in_scope(&e.item, scope) {
                continue;
            }
            match &e.state {
                State::Leased { expires_at, .. } if *expires_at > now => {
                    inflight_total += 1;
                    if let Some(k) = &e.item.concurrency_key {
                        *inflight_by_key.entry(k.clone()).or_default() += 1;
                    }
                }
                State::Leased { .. } => {}
                State::Queued if e.item.due_at <= now => candidates.push(*id),
                State::Queued => note_due(e.item.due_at, &mut next_due),
            }
        }
        // Resumes before starts, then oldest due first.
        candidates.sort_by_key(|id| {
            let i = &inner.entries[id].item;
            (i.kind == WorkKind::Start, i.due_at, *id)
        });

        let mut leases = vec![];
        for id in candidates {
            if leases.len() >= max {
                break;
            }
            if limits
                .workflow_concurrency
                .is_some_and(|l| inflight_total >= l)
            {
                break;
            }
            let item = inner.entries[&id].item.clone();
            if let (Some(limit), Some(k)) = (limits.key_concurrency, &item.concurrency_key)
                && inflight_by_key.get(k).copied().unwrap_or(0) >= limit
            {
                continue;
            }
            if let (Some(th), WorkKind::Start) = (limits.throttle, item.kind) {
                let key = (scope.clone(), item.throttle_key.clone().unwrap_or_default());
                match gcra(
                    inner.throttle.get(&key).copied(),
                    now,
                    th.limit,
                    th.period,
                    th.burst,
                ) {
                    Ok(tat) => {
                        inner.throttle.insert(key, tat);
                    }
                    Err(retry_at) => {
                        note_due(retry_at, &mut next_due);
                        continue;
                    }
                }
            }
            inflight_total += 1;
            if let Some(k) = &item.concurrency_key {
                *inflight_by_key.entry(k.clone()).or_default() += 1;
            }
            let token = Uuid::now_v7().to_string();
            let expires_at = saturating_add(now, lease);
            inner.entries.get_mut(&id).unwrap().state = State::Leased {
                token: token.clone(),
                expires_at,
                rerun: None,
            };
            leases.push(Lease {
                item,
                token,
                expires_at,
            });
        }
        Ok(Claimed { leases, next_due })
    }

    async fn extend(
        &self,
        leases: &[Lease],
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Lease>> {
        let mut inner = self.inner.lock();
        let mut lost = vec![];
        for l in leases {
            match inner.entries.get_mut(&l.item.run_id) {
                Some(Entry {
                    state:
                        State::Leased {
                            token, expires_at, ..
                        },
                    ..
                }) if *token == l.token => {
                    *expires_at = saturating_add(now, lease);
                }
                _ => lost.push(l.clone()),
            }
        }
        Ok(lost)
    }

    async fn ack(
        &self,
        lease: &Lease,
        next_wake: Option<DateTime<Utc>>,
        _now: DateTime<Utc>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let id = lease.item.run_id;
        let Some(Entry {
            item,
            state: State::Leased { token, rerun, .. },
        }) = inner.entries.get_mut(&id)
        else {
            return Err(Error::Conflict);
        };
        if *token != lease.token {
            return Err(Error::Conflict);
        }
        let mut next = rerun.take();
        if let Some(t) = next_wake {
            let resume = QueueItem {
                due_at: t,
                kind: WorkKind::Resume,
                ..item.clone()
            };
            match &mut next {
                Some(n) => merge(n, &resume),
                None => next = Some(resume),
            }
        }
        match next {
            Some(n) => inner.entries.insert(
                id,
                Entry {
                    item: n,
                    state: State::Queued,
                },
            ),
            None => inner.entries.remove(&id),
        };
        Ok(())
    }

    async fn nack(
        &self,
        lease: &Lease,
        retry_at: DateTime<Utc>,
        _now: DateTime<Utc>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let Some(
            e @ Entry {
                state: State::Leased { .. },
                ..
            },
        ) = inner.entries.get_mut(&lease.item.run_id)
        else {
            return Err(Error::Conflict);
        };
        let State::Leased { token, rerun, .. } = &mut e.state else {
            unreachable!()
        };
        if *token != lease.token {
            return Err(Error::Conflict);
        }
        // A pending rerun (e.g. a signal that arrived mid-execution) must not
        // wait out the retry delay.
        let due = rerun.take().map_or(retry_at, |r| {
            let due = retry_at.min(r.due_at);
            merge(&mut e.item, &r);
            due
        });
        e.item.due_at = due;
        e.state = State::Queued;
        Ok(())
    }

    async fn requeue_expired(&self, scope: &QueueScope, now: DateTime<Utc>) -> Result<usize> {
        let mut inner = self.inner.lock();
        let mut n = 0;
        for e in inner.entries.values_mut() {
            if !in_scope(&e.item, scope) {
                continue;
            }
            if let State::Leased {
                expires_at, rerun, ..
            } = &mut e.state
                && *expires_at <= now
            {
                if let Some(r) = rerun.take() {
                    merge(&mut e.item, &r);
                }
                e.item.due_at = e.item.due_at.min(now);
                e.state = State::Queued;
                n += 1;
            }
        }
        Ok(n)
    }
}
