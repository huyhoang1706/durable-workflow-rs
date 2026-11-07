/// Infrastructure error shared by every plugin trait.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Optimistic `expected_seq` mismatch: another worker owns the run.
    #[error("run version conflict")]
    Conflict,
    #[error("not found: {0}")]
    NotFound(String),
    /// The operation is not valid for the run's current state.
    #[error("invalid state: {0}")]
    InvalidState(String),
    #[error("invalid configuration: {0}")]
    Config(String),
    /// Appending to a stream that has been closed.
    #[error("stream is closed")]
    Closed,
    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
