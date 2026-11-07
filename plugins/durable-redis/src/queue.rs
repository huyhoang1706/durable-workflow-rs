use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_spi::*;
use redis::{Script, aio::ConnectionManager};
use serde::Deserialize;
use serde_json::Value;

use crate::be;

struct Scripts {
    enqueue: Script,
    claim: Script,
    extend: Script,
    release: Script,
    requeue_expired: Script,
}

/// Dispatch index: a partitioned, leased queue implemented as Lua scripts.
///
/// Keys are hash-tagged per workflow (`{prefix}:{ns:workflow}:...`) so every
/// script touches a single slot and Redis Cluster stays viable.
pub struct RedisQueue {
    conn: ConnectionManager,
    prefix: String,
    scripts: Scripts,
}

impl RedisQueue {
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: "dw".into(),
            scripts: Scripts {
                enqueue: Script::new(include_str!("lua/enqueue.lua")),
                claim: Script::new(include_str!("lua/claim.lua")),
                extend: Script::new(include_str!("lua/extend.lua")),
                release: Script::new(include_str!("lua/release.lua")),
                requeue_expired: Script::new(include_str!("lua/requeue_expired.lua")),
            },
        }
    }

    /// Key prefix (default `dw`); lets several deployments share one Redis.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    fn scope_key(&self, namespace: &str, workflow: &str) -> String {
        format!("{}:{{{namespace}:{workflow}}}", self.prefix)
    }
}

fn ms(t: DateTime<Utc>) -> i64 {
    t.timestamp_millis()
}

fn from_ms(v: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(v).expect("timestamp in range")
}

fn kind_str(k: WorkKind) -> &'static str {
    match k {
        WorkKind::Start => "start",
        WorkKind::Resume => "resume",
    }
}

#[derive(Deserialize)]
struct ClaimReply {
    /// cjson encodes an empty array as `{}`, hence `Value`.
    leases: Value,
    next_due: i64,
}

#[async_trait]
impl Queue for RedisQueue {
    async fn enqueue(&self, items: Vec<QueueItem>) -> Result<()> {
        let mut by_scope: HashMap<String, Vec<QueueItem>> = HashMap::new();
        for item in items {
            by_scope
                .entry(self.scope_key(&item.namespace, &item.workflow))
                .or_default()
                .push(item);
        }
        for (key, items) in by_scope {
            let mut inv = self.scripts.enqueue.key(&key);
            for item in &items {
                inv.arg(item.run_id.to_string())
                    .arg(ms(item.due_at))
                    .arg(kind_str(item.kind))
                    .arg(item.concurrency_key.as_deref().unwrap_or(""))
                    .arg(item.throttle_key.as_deref().unwrap_or(""))
                    .arg(serde_json::to_string(item).map_err(|e| Error::Backend(e.to_string()))?);
            }
            let _: i64 = inv.invoke_async(&mut self.conn.clone()).await.map_err(be)?;
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
        let (th_limit, th_period, th_burst) = limits.throttle.map_or((-1, 0, 1), |t| {
            (t.limit as i64, t.period.as_millis() as i64, t.burst as i64)
        });
        let raw: String = self
            .scripts
            .claim
            .key(self.scope_key(&scope.namespace, &scope.workflow))
            .arg(ms(now))
            .arg(max as i64)
            .arg(lease.as_millis() as i64)
            .arg(limits.workflow_concurrency.map_or(-1, i64::from))
            .arg(limits.key_concurrency.map_or(-1, i64::from))
            .arg(th_limit)
            .arg(th_period)
            .arg(th_burst)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        let reply: ClaimReply = serde_json::from_str(&raw)
            .map_err(|e| Error::Backend(format!("bad claim reply: {e}")))?;
        let mut leases = vec![];
        for entry in reply
            .leases
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or_default()
        {
            let f: Vec<String> = serde_json::from_value(entry.clone())
                .map_err(|e| Error::Backend(format!("bad lease entry: {e}")))?;
            let mut item: QueueItem =
                serde_json::from_str(&f[5]).map_err(|e| Error::Backend(e.to_string()))?;
            // The hash holds the authoritative kind and due time (merged on enqueue).
            item.kind = if f[3] == "resume" {
                WorkKind::Resume
            } else {
                WorkKind::Start
            };
            item.due_at = from_ms(f[4].parse().map_err(|_| Error::Backend("bad due".into()))?);
            leases.push(Lease {
                item,
                token: f[1].clone(),
                expires_at: from_ms(
                    f[2].parse()
                        .map_err(|_| Error::Backend("bad expiry".into()))?,
                ),
            });
        }
        Ok(Claimed {
            leases,
            next_due: (reply.next_due >= 0).then(|| from_ms(reply.next_due)),
        })
    }

    async fn extend(
        &self,
        leases: &[Lease],
        lease: Duration,
        now: DateTime<Utc>,
    ) -> Result<Vec<Lease>> {
        let mut by_scope: HashMap<String, Vec<&Lease>> = HashMap::new();
        for l in leases {
            by_scope
                .entry(self.scope_key(&l.item.namespace, &l.item.workflow))
                .or_default()
                .push(l);
        }
        let mut lost = vec![];
        for (key, group) in by_scope {
            let mut inv = self.scripts.extend.key(&key);
            inv.arg(ms(now)).arg(lease.as_millis() as i64);
            for l in &group {
                inv.arg(l.item.run_id.to_string()).arg(&l.token);
            }
            let raw: String = inv.invoke_async(&mut self.conn.clone()).await.map_err(be)?;
            let lost_runs: Vec<String> = serde_json::from_str(&raw).unwrap_or_default();
            lost.extend(
                group
                    .into_iter()
                    .filter(|l| lost_runs.contains(&l.item.run_id.to_string()))
                    .cloned(),
            );
        }
        Ok(lost)
    }

    async fn ack(
        &self,
        lease: &Lease,
        next_wake: Option<DateTime<Utc>>,
        now: DateTime<Utc>,
    ) -> Result<()> {
        self.release("ack", lease, next_wake.map_or(-1, ms), now)
            .await
    }

    async fn nack(&self, lease: &Lease, retry_at: DateTime<Utc>, now: DateTime<Utc>) -> Result<()> {
        self.release("nack", lease, ms(retry_at), now).await
    }

    async fn requeue_expired(&self, scope: &QueueScope, now: DateTime<Utc>) -> Result<usize> {
        let n: i64 = self
            .scripts
            .requeue_expired
            .key(self.scope_key(&scope.namespace, &scope.workflow))
            .arg(ms(now))
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        Ok(n as usize)
    }
}

impl RedisQueue {
    async fn release(&self, mode: &str, lease: &Lease, at: i64, _now: DateTime<Utc>) -> Result<()> {
        let ok: i64 = self
            .scripts
            .release
            .key(self.scope_key(&lease.item.namespace, &lease.item.workflow))
            .arg(mode)
            .arg(lease.item.run_id.to_string())
            .arg(&lease.token)
            .arg(at)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        if ok == 1 {
            Ok(())
        } else {
            Err(Error::Conflict)
        }
    }
}
