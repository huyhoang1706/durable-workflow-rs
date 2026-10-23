//! Built-in [`ClassifyError`] impls for common client libraries, behind the
//! `reqwest` and `sqlx` cargo features. Use them with
//! [`StepError::classified`](crate::StepError::classified).

#[cfg(feature = "reqwest")]
pub use http::check_response;

#[cfg(feature = "reqwest")]
mod http {
    use std::time::Duration;

    use reqwest::{Response, StatusCode, header::RETRY_AFTER};

    use crate::{ClassifyError, ErrorClass, StepError};

    /// 429 (honouring `Retry-After`), 408 and 5xx are worth retrying; any other
    /// non-success status is the caller's mistake and retrying cannot fix it.
    fn classify_status(status: StatusCode, retry_after: Option<Duration>) -> ErrorClass {
        if status == StatusCode::TOO_MANY_REQUESTS {
            return retry_after.map_or(ErrorClass::Retryable, ErrorClass::RetryAfter);
        }
        if status == StatusCode::REQUEST_TIMEOUT || status.is_server_error() {
            ErrorClass::Retryable
        } else {
            ErrorClass::Fatal
        }
    }

    /// Passes success through; turns any other status into a classified
    /// [`StepError`]. A 429 with a `Retry-After: <seconds>` header becomes
    /// [`ErrorClass::RetryAfter`].
    pub fn check_response(resp: Response) -> Result<Response, StepError> {
        let status = resp.status();
        if status.is_success() {
            return Ok(resp);
        }
        let retry_after = resp
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        Err(StepError {
            message: format!("HTTP {status} for {}", resp.url()),
            class: classify_status(status, retry_after),
        })
    }

    impl ClassifyError for reqwest::Error {
        fn classify(&self) -> ErrorClass {
            if let Some(status) = self.status() {
                return classify_status(status, None);
            }
            if self.is_timeout() || self.is_connect() || self.is_request() || self.is_body() {
                ErrorClass::Retryable
            } else {
                // Builder, redirect-loop and decode errors repeat identically.
                ErrorClass::Fatal
            }
        }
    }
}

#[cfg(feature = "sqlx")]
mod db {
    use crate::{ClassifyError, ErrorClass};

    impl ClassifyError for sqlx::Error {
        fn classify(&self) -> ErrorClass {
            match self {
                sqlx::Error::Io(_)
                | sqlx::Error::Tls(_)
                | sqlx::Error::PoolTimedOut
                | sqlx::Error::PoolClosed
                | sqlx::Error::WorkerCrashed => ErrorClass::Retryable,
                sqlx::Error::Database(db) => match db.code().as_deref() {
                    // serialization_failure, deadlock_detected, lock_not_available,
                    // admin/crash shutdown, too_many_connections, and the
                    // connection-exception class.
                    Some("40001" | "40P01" | "55P03" | "57P01" | "57P02" | "57P03" | "53300") => {
                        ErrorClass::Retryable
                    }
                    Some(code) if code.starts_with("08") => ErrorClass::Retryable,
                    _ => ErrorClass::Fatal,
                },
                _ => ErrorClass::Fatal,
            }
        }
    }
}
