use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use durable_spi::{
    ChunkBody, Clock, Error, NewChunk, Result, RunId, StreamChunk, StreamCursor, StreamInfo,
    StreamStore, saturating_add,
};
use parking_lot::Mutex;

/// Default time a stream lives after its last append.
const DEFAULT_TTL: Duration = Duration::from_secs(24 * 3600);
const DEFAULT_MAX_LEN: usize = 10_000;

struct Stream {
    chunks: VecDeque<StreamChunk>,
    next_id: u64,
    closed: bool,
    expires_at: DateTime<Utc>,
}

/// In-memory [`StreamStore`]. Expiry is measured against the injected
/// [`Clock`], so tests can drive it with a manual clock.
pub struct MemoryStreams {
    clock: Arc<dyn Clock>,
    ttl: Duration,
    max_len: usize,
    streams: Mutex<HashMap<(RunId, String), Stream>>,
}

impl MemoryStreams {
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            ttl: DEFAULT_TTL,
            max_len: DEFAULT_MAX_LEN,
            streams: Mutex::default(),
        }
    }

    /// How long a stream lives after its last append (default 24 h).
    pub fn ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Entries kept per stream; the oldest are dropped first (default 10 000).
    pub fn max_len(mut self, n: usize) -> Self {
        self.max_len = n.max(1);
        self
    }

    fn purge_expired(&self, map: &mut HashMap<(RunId, String), Stream>) {
        let now = self.clock.now();
        map.retain(|_, s| s.expires_at > now);
    }
}

fn parse_cursor(c: &StreamCursor) -> u64 {
    c.0.parse().unwrap_or(0)
}

#[async_trait]
impl StreamStore for MemoryStreams {
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
        let now = self.clock.now();
        let expires_at = saturating_add(now, self.ttl);
        let mut map = self.streams.lock();
        self.purge_expired(&mut map);
        let s = map.entry((run_id, stream.to_string())).or_insert(Stream {
            chunks: VecDeque::new(),
            next_id: 1,
            closed: false,
            expires_at,
        });
        if s.closed {
            return Err(Error::Closed);
        }
        let mut last = s.next_id.saturating_sub(1);
        for c in chunks {
            last = s.next_id;
            s.next_id += 1;
            s.closed |= matches!(c.body, ChunkBody::Closed);
            s.chunks.push_back(StreamChunk {
                cursor: StreamCursor(last.to_string()),
                at: now,
                step_key: c.step_key,
                attempt: c.attempt,
                body: c.body,
            });
        }
        while s.chunks.len() > self.max_len {
            s.chunks.pop_front();
        }
        s.expires_at = expires_at;
        Ok(StreamCursor(last.to_string()))
    }

    async fn read(
        &self,
        run_id: RunId,
        stream: &str,
        after: Option<&StreamCursor>,
        limit: usize,
    ) -> Result<Vec<StreamChunk>> {
        let mut map = self.streams.lock();
        self.purge_expired(&mut map);
        let after = after.map_or(0, parse_cursor);
        Ok(map
            .get(&(run_id, stream.to_string()))
            .map(|s| {
                s.chunks
                    .iter()
                    .filter(|c| parse_cursor(&c.cursor) > after)
                    .take(limit)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }

    async fn info(&self, run_id: RunId, stream: &str) -> Result<Option<StreamInfo>> {
        let mut map = self.streams.lock();
        self.purge_expired(&mut map);
        Ok(map
            .get(&(run_id, stream.to_string()))
            .map(|s| info_of(stream, s)))
    }

    async fn list(&self, run_id: RunId) -> Result<Vec<StreamInfo>> {
        let mut map = self.streams.lock();
        self.purge_expired(&mut map);
        let mut out: Vec<_> = map
            .iter()
            .filter(|((r, _), _)| *r == run_id)
            .map(|((_, name), s)| info_of(name, s))
            .collect();
        out.sort_by(|a, b| a.stream.cmp(&b.stream));
        Ok(out)
    }

    async fn close_all(&self, run_id: RunId) -> Result<()> {
        let now = self.clock.now();
        let expires_at = saturating_add(now, self.ttl);
        let mut map = self.streams.lock();
        self.purge_expired(&mut map);
        for ((r, _), s) in map.iter_mut() {
            if *r != run_id || s.closed {
                continue;
            }
            let id = s.next_id;
            s.next_id += 1;
            s.closed = true;
            s.chunks.push_back(StreamChunk {
                cursor: StreamCursor(id.to_string()),
                at: now,
                step_key: None,
                attempt: 0,
                body: ChunkBody::Closed,
            });
            while s.chunks.len() > self.max_len {
                s.chunks.pop_front();
            }
            s.expires_at = expires_at;
        }
        Ok(())
    }
}

fn info_of(name: &str, s: &Stream) -> StreamInfo {
    StreamInfo {
        stream: name.to_string(),
        len: s.chunks.len() as u64,
        closed: s.closed,
    }
}
