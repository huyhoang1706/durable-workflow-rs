//! Durations on the wire are whole milliseconds (`*_ms` fields), as plain
//! JSON integers every language reads without a custom type. Sub-millisecond
//! parts are dropped; durations past `u64::MAX` ms saturate.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serializer};

fn millis(d: &Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_u64(millis(d))
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
    u64::deserialize(d).map(Duration::from_millis)
}

/// For `Option<Duration>`; `null` is `None`.
pub mod option {
    use super::*;

    pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match d {
            Some(d) => s.serialize_some(&millis(d)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        Option::<u64>::deserialize(d).map(|ms| ms.map(Duration::from_millis))
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct T {
        #[serde(with = "crate::ms")]
        a: Duration,
        #[serde(with = "crate::ms::option")]
        b: Option<Duration>,
    }

    #[test]
    fn durations_are_whole_milliseconds() {
        let t = T {
            a: Duration::from_millis(1500),
            b: None,
        };
        let v = serde_json::to_value(&t).unwrap();
        assert_eq!(v, json!({"a": 1500, "b": null}));
        assert_eq!(serde_json::from_value::<T>(v).unwrap(), t);
        let v = serde_json::to_value(T {
            a: Duration::MAX,
            b: Some(Duration::from_micros(2500)),
        })
        .unwrap();
        assert_eq!(v, json!({"a": u64::MAX, "b": 2}));
    }
}
