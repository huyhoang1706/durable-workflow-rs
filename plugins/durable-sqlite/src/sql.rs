//! How values are stored: ids as hyphenated text, timestamps as Unix
//! microseconds, JSON as text.

use chrono::{DateTime, Utc};
use durable_spi::Error;
use serde::{Serialize, de::DeserializeOwned};
use uuid::Uuid;

pub(crate) fn be(e: sqlx::Error) -> Error {
    Error::Backend(e.to_string())
}

fn decode(e: impl std::error::Error + Send + Sync + 'static) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(e))
}

pub(crate) fn id(id: Uuid) -> String {
    id.hyphenated().to_string()
}

pub(crate) fn parse_id(s: &str) -> Result<Uuid, sqlx::Error> {
    Uuid::parse_str(s).map_err(decode)
}

pub(crate) fn us(t: DateTime<Utc>) -> i64 {
    t.timestamp_micros()
}

pub(crate) fn from_us(us: i64) -> Result<DateTime<Utc>, sqlx::Error> {
    DateTime::from_timestamp_micros(us)
        .ok_or_else(|| sqlx::Error::Decode(format!("timestamp out of range: {us}").into()))
}

pub(crate) fn opt_from_us(us: Option<i64>) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    us.map(from_us).transpose()
}

pub(crate) fn json<T: Serialize + ?Sized>(v: &T) -> Result<String, Error> {
    serde_json::to_string(v).map_err(|e| Error::Backend(e.to_string()))
}

pub(crate) fn from_json<T: DeserializeOwned>(s: &str) -> Result<T, sqlx::Error> {
    serde_json::from_str(s).map_err(decode)
}

pub(crate) fn opt_from_json<T: DeserializeOwned>(
    s: Option<String>,
) -> Result<Option<T>, sqlx::Error> {
    s.as_deref().map(from_json).transpose()
}
