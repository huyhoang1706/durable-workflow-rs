use std::{borrow::Cow, error::Error as StdError, fmt};

use durable_runtime::{ClassifyError, ErrorClass, StepError};
use sqlx::error::{DatabaseError, ErrorKind};

#[derive(Debug)]
struct FakeDb {
    code: &'static str,
    kind: ErrorKind,
}

impl fmt::Display for FakeDb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fake db error {}", self.code)
    }
}
impl StdError for FakeDb {}

impl DatabaseError for FakeDb {
    fn message(&self) -> &str {
        "fake"
    }
    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.code))
    }
    fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
        self
    }
    fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
        self
    }
    fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
        self
    }
    fn kind(&self) -> ErrorKind {
        match self.kind {
            ErrorKind::UniqueViolation => ErrorKind::UniqueViolation,
            _ => ErrorKind::Other,
        }
    }
}

fn db(code: &'static str, kind: ErrorKind) -> sqlx::Error {
    sqlx::Error::Database(Box::new(FakeDb { code, kind }))
}

#[test]
fn serialization_failures_and_deadlocks_are_retryable() {
    for code in ["40001", "40P01"] {
        assert_eq!(
            db(code, ErrorKind::Other).classify(),
            ErrorClass::Retryable,
            "{code}"
        );
    }
}

#[test]
fn transient_connection_problems_are_retryable() {
    assert_eq!(sqlx::Error::PoolTimedOut.classify(), ErrorClass::Retryable);
    let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
    assert_eq!(sqlx::Error::Io(io).classify(), ErrorClass::Retryable);
    // Postgres "admin shutdown" / connection exceptions (class 08, 57P01).
    assert_eq!(
        db("57P01", ErrorKind::Other).classify(),
        ErrorClass::Retryable
    );
    assert_eq!(
        db("08006", ErrorKind::Other).classify(),
        ErrorClass::Retryable
    );
}

#[test]
fn constraint_violations_and_logic_errors_are_fatal() {
    assert_eq!(
        db("23505", ErrorKind::UniqueViolation).classify(),
        ErrorClass::Fatal
    );
    assert_eq!(db("42601", ErrorKind::Other).classify(), ErrorClass::Fatal);
    assert_eq!(sqlx::Error::RowNotFound.classify(), ErrorClass::Fatal);
}

#[test]
fn classified_step_error_keeps_the_class() {
    let e = StepError::classified(db("40001", ErrorKind::Other));
    assert_eq!(e.class, ErrorClass::Retryable);
    assert!(e.message.contains("40001"));
}
