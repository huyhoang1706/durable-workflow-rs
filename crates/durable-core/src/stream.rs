//! Transient user streams: named, ordered chunk feeds that steps write to
//! (LLM tokens, progress) and clients tail. They are kept for a TTL and are
//! never durable state; the durable part is the step's output.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Result, RunId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum ChunkBody {
    Data(Value),
    /// The attempt that wrote the preceding chunks failed; discard them.
    AttemptFailed,
    /// The stream is finished; further appends are rejected.
    Closed,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NewChunk {
    pub step_key: Option<String>,
    pub attempt: u32,
    pub body: ChunkBody,
}

impl NewChunk {
    pub fn data(step_key: impl Into<String>, attempt: u32, value: Value) -> Self {
        Self {
            step_key: Some(step_key.into()),
            attempt,
            body: ChunkBody::Data(value),
        }
    }
    pub fn attempt_failed(step_key: impl Into<String>, attempt: u32) -> Self {
        Self {
            step_key: Some(step_key.into()),
            attempt,
            body: ChunkBody::AttemptFailed,
        }
    }
    pub fn closed() -> Self {
        Self {
            step_key: None,
            attempt: 0,
            body: ChunkBody::Closed,
        }
    }
}

/// Opaque, ordered cursor (a Redis stream entry id; a counter in memory).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StreamCursor(pub String);

impl std::fmt::Display for StreamCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamChunk {
    pub cursor: StreamCursor,
    pub at: DateTime<Utc>,
    pub step_key: Option<String>,
    pub attempt: u32,
    pub body: ChunkBody,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamInfo {
    pub stream: String,
    pub len: u64,
    pub closed: bool,
}

#[async_trait]
pub trait StreamStore: Send + Sync + 'static {
    /// Appends in order and refreshes the stream's TTL. `Error::Closed` after
    /// `Closed`. Returns the cursor of the last appended chunk.
    async fn append(
        &self,
        run_id: RunId,
        stream: &str,
        chunks: Vec<NewChunk>,
    ) -> Result<StreamCursor>;
    /// Chunks strictly after `after` (from the start when `None`). An expired
    /// or unknown stream reads as empty, and `info` returns `None`.
    async fn read(
        &self,
        run_id: RunId,
        stream: &str,
        after: Option<&StreamCursor>,
        limit: usize,
    ) -> Result<Vec<StreamChunk>>;
    async fn info(&self, run_id: RunId, stream: &str) -> Result<Option<StreamInfo>>;
    async fn list(&self, run_id: RunId) -> Result<Vec<StreamInfo>>;
    /// Appends `Closed` to every open stream of the run; idempotent.
    async fn close_all(&self, run_id: RunId) -> Result<()>;
}
