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
    #[error("backend error: {0}")]
    Backend(String),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
