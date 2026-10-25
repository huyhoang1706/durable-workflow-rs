use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_core::{
    ChunkBody, Error, NewChunk, Result, RunId, StreamChunk, StreamCursor, StreamInfo, StreamStore,
};
use redis::{Script, aio::ConnectionManager};
use serde_json::Value;

use crate::be;

const DEFAULT_TTL: Duration = Duration::from_secs(24 * 3600);
const DEFAULT_MAX_LEN: usize = 10_000;

/// Transient user streams on Redis Streams.
///
/// Keys carry the run id as a hash tag, so everything one run needs lives in
/// one cluster slot:
///
/// | Key | |
/// |---|---|
/// | `{prefix}:s:{run}:n:{name}` | the stream (one `XADD` entry per chunk) |
/// | `{prefix}:s:{run}:names` | set of the run's stream names |
/// | `{prefix}:s:{run}:closed` | set of its closed streams |
///
/// Every key expires `ttl` after the run's last append. Streams are capped at
/// roughly `max_len` entries (`MAXLEN ~`). Losing them (expiry, `FLUSHALL`)
/// loses nothing durable.
pub struct RedisStreams {
    conn: ConnectionManager,
    prefix: String,
    ttl: Duration,
    max_len: usize,
    append: Script,
    close_all: Script,
}

impl RedisStreams {
    pub fn new(conn: ConnectionManager) -> Self {
        Self {
            conn,
            prefix: "dw".into(),
            ttl: DEFAULT_TTL,
            max_len: DEFAULT_MAX_LEN,
            append: Script::new(include_str!("lua/stream_append.lua")),
            close_all: Script::new(include_str!("lua/stream_close_all.lua")),
        }
    }

    /// Key prefix (default `dw`); lets several deployments share one Redis.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// How long a run's streams live after the last append (default 24 h).
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Approximate entries kept per stream, oldest dropped first (default 10 000).
    pub fn max_len(mut self, n: usize) -> Self {
        self.max_len = n.max(1);
        self
    }

    fn run_key(&self, run: RunId) -> String {
        format!("{}:s:{{{run}}}", self.prefix)
    }
    fn stream_key(&self, run: RunId, name: &str) -> String {
        format!("{}:n:{name}", self.run_key(run))
    }
    fn names_key(&self, run: RunId) -> String {
        format!("{}:names", self.run_key(run))
    }
    fn closed_key(&self, run: RunId) -> String {
        format!("{}:closed", self.run_key(run))
    }
    fn ttl_ms(&self) -> u64 {
        u64::try_from(self.ttl.as_millis())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}

fn encode(c: NewChunk) -> Result<[String; 4]> {
    let (kind, data) = match c.body {
        ChunkBody::Data(v) => ("data", v.to_string()),
        ChunkBody::AttemptStarted => ("started", "null".to_string()),
        ChunkBody::AttemptFailed => ("failed", "null".to_string()),
        ChunkBody::Closed => ("closed", "null".to_string()),
    };
    Ok([
        c.step_key.unwrap_or_default(),
        c.attempt.to_string(),
        kind.to_string(),
        data,
    ])
}

/// `XRANGE` returns `[id, [field, value, ...]]` per entry.
fn decode(id: String, fields: Vec<String>) -> Result<StreamChunk> {
    let get = |name: &str| {
        fields
            .chunks(2)
            .find(|kv| kv[0] == name)
            .map(|kv| kv[1].as_str())
    };
    let bad = |what: &str| Error::Backend(format!("stream entry {id} has a bad `{what}` field"));
    let body = match get("t") {
        Some("data") => ChunkBody::Data(
            serde_json::from_str::<Value>(get("d").unwrap_or("null")).map_err(|_| bad("d"))?,
        ),
        Some("started") => ChunkBody::AttemptStarted,
        Some("failed") => ChunkBody::AttemptFailed,
        Some("closed") => ChunkBody::Closed,
        _ => return Err(bad("t")),
    };
    let ms: i64 = id
        .split('-')
        .next()
        .and_then(|ms| ms.parse().ok())
        .ok_or_else(|| bad("id"))?;
    Ok(StreamChunk {
        at: DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default(),
        step_key: get("k").filter(|k| !k.is_empty()).map(String::from),
        attempt: get("a").and_then(|a| a.parse().ok()).unwrap_or(0),
        body,
        cursor: StreamCursor(id),
    })
}

fn valid_cursor(c: &str) -> bool {
    let mut parts = c.splitn(2, '-');
    let ok =
        |p: Option<&str>| p.is_some_and(|p| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()));
    ok(parts.next()) && parts.next().is_none_or(|seq| ok(Some(seq)))
}

#[async_trait]
impl StreamStore for RedisStreams {
    async fn append(
        &self,
        run_id: RunId,
        stream: &str,
        chunks: Vec<NewChunk>,
    ) -> Result<StreamCursor> {
        if let Some(pos) = chunks
            .iter()
            .position(|c| matches!(c.body, ChunkBody::Closed))
            && pos + 1 != chunks.len()
        {
            return Err(Error::InvalidState(
                "a Closed chunk must be last in its batch".into(),
            ));
        }
        if chunks.is_empty() {
            return Ok(StreamCursor("0-0".into()));
        }
        let mut inv = self.append.prepare_invoke();
        inv.key(self.stream_key(run_id, stream))
            .key(self.names_key(run_id))
            .key(self.closed_key(run_id))
            .arg(self.ttl_ms())
            .arg(self.max_len)
            .arg(stream);
        for c in chunks {
            for field in encode(c)? {
                inv.arg(field);
            }
        }
        let reply: String = inv.invoke_async(&mut self.conn.clone()).await.map_err(be)?;
        if reply == "CLOSED" {
            return Err(Error::Closed);
        }
        Ok(StreamCursor(reply))
    }

    async fn read(
        &self,
        run_id: RunId,
        stream: &str,
        after: Option<&StreamCursor>,
        limit: usize,
    ) -> Result<Vec<StreamChunk>> {
        let start = match after {
            Some(c) if valid_cursor(&c.0) => format!("({}", c.0),
            Some(c) => return Err(Error::Config(format!("invalid stream cursor `{}`", c.0))),
            None => "-".to_string(),
        };
        let entries: Vec<(String, Vec<String>)> = redis::cmd("XRANGE")
            .arg(self.stream_key(run_id, stream))
            .arg(start)
            .arg("+")
            .arg("COUNT")
            .arg(limit.max(1))
            .query_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        entries.into_iter().map(|(id, f)| decode(id, f)).collect()
    }

    async fn info(&self, run_id: RunId, stream: &str) -> Result<Option<StreamInfo>> {
        let (len, closed): (u64, bool) = redis::pipe()
            .cmd("XLEN")
            .arg(self.stream_key(run_id, stream))
            .cmd("SISMEMBER")
            .arg(self.closed_key(run_id))
            .arg(stream)
            .query_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        Ok((len > 0).then(|| StreamInfo {
            stream: stream.to_string(),
            len,
            closed,
        }))
    }

    async fn list(&self, run_id: RunId) -> Result<Vec<StreamInfo>> {
        let mut names: Vec<String> = redis::cmd("SMEMBERS")
            .arg(self.names_key(run_id))
            .query_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        names.sort();
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            if let Some(info) = self.info(run_id, &name).await? {
                out.push(info);
            }
        }
        Ok(out)
    }

    async fn close_all(&self, run_id: RunId) -> Result<()> {
        let _: i64 = self
            .close_all
            .key(self.names_key(run_id))
            .key(self.closed_key(run_id))
            .arg(self.ttl_ms())
            .arg(self.max_len)
            .arg(format!("{}:n:", self.run_key(run_id)))
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(be)?;
        Ok(())
    }
}
