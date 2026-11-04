use std::{fmt, time::Duration};

pub use durable_sm::ErrorClass;

/// Lets a dependency's error pick how a step failure is retried.
pub trait ClassifyError {
    fn classify(&self) -> ErrorClass;
}

#[derive(Debug, Clone, PartialEq)]
pub struct StepError {
    pub message: String,
    pub class: ErrorClass,
}

impl StepError {
    pub fn retryable(msg: impl fmt::Display) -> Self {
        Self {
            message: msg.to_string(),
            class: ErrorClass::Retryable,
        }
    }
    pub fn fatal(msg: impl fmt::Display) -> Self {
        Self {
            message: msg.to_string(),
            class: ErrorClass::Fatal,
        }
    }
    pub fn retry_after(after: Duration, msg: impl fmt::Display) -> Self {
        Self {
            message: msg.to_string(),
            class: ErrorClass::RetryAfter(after),
        }
    }
    pub fn classified<E: ClassifyError + fmt::Display>(e: E) -> Self {
        Self {
            message: e.to_string(),
            class: e.classify(),
        }
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

// Deliberately not `std::error::Error`, so any other error converts via `?`.
impl<E: std::error::Error + Send + Sync + 'static> From<E> for StepError {
    fn from(e: E) -> Self {
        Self::retryable(e)
    }
}

/// What workflow code sees from every ctx call.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum WorkflowError {
    /// A step exhausted its retries (or failed fatally).
    #[error("step failed: {0}")]
    Step(StepError),
    /// The workflow itself decided to fail the run.
    #[error("{0}")]
    Failed(String),
    #[error("run cancelled")]
    Cancelled,
    /// The engine is rebuilding or suspending; propagate with `?`.
    #[error("workflow interrupted by the engine")]
    Interrupted,
    #[error("non-determinism: {0}")]
    NonDeterminism(String),
    #[error("hook token `{0}` is held by another run")]
    HookConflict(String),
    /// Engine or configuration fault (missing dependency, ...).
    #[error("engine fault: {0}")]
    Fault(String),
}

impl WorkflowError {
    pub fn failed(msg: impl fmt::Display) -> Self {
        Self::Failed(msg.to_string())
    }

    /// Faults leave the run stuck for a code fix instead of failing it.
    pub fn is_engine_fault(&self) -> bool {
        matches!(self, Self::NonDeterminism(_) | Self::Fault(_))
    }
}

impl From<StepError> for WorkflowError {
    fn from(e: StepError) -> Self {
        Self::Step(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Boom;
    impl fmt::Display for Boom {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("boom")
        }
    }
    impl std::error::Error for Boom {}
    impl ClassifyError for Boom {
        fn classify(&self) -> ErrorClass {
            ErrorClass::RetryAfter(Duration::from_secs(9))
        }
    }

    #[test]
    fn any_std_error_is_retryable_by_default() {
        fn f() -> Result<(), StepError> {
            Err(Boom)?
        }
        let e = f().unwrap_err();
        assert_eq!(
            (e.message.as_str(), e.class),
            ("boom", ErrorClass::Retryable)
        );
    }

    #[test]
    fn constructors_set_the_class() {
        assert_eq!(StepError::fatal("x").class, ErrorClass::Fatal);
        let d = Duration::from_secs(3);
        assert_eq!(
            StepError::retry_after(d, "x").class,
            ErrorClass::RetryAfter(d)
        );
    }

    #[test]
    fn classified_uses_the_error_own_class() {
        let e = StepError::classified(Boom);
        assert_eq!(e.class, ErrorClass::RetryAfter(Duration::from_secs(9)));
        assert_eq!(e.message, "boom");
    }

    #[test]
    fn only_non_determinism_and_faults_are_engine_faults() {
        assert!(WorkflowError::Fault("x".into()).is_engine_fault());
        assert!(WorkflowError::NonDeterminism("x".into()).is_engine_fault());
        assert!(!WorkflowError::failed("x").is_engine_fault());
        assert!(!WorkflowError::Step(StepError::fatal("x")).is_engine_fault());
    }
}
