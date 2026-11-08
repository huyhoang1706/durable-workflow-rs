//! A record of every call between a host and a [`Replayer`](crate::Replayer).
//!
//! Turn it on with [`Replayer::record`](crate::Replayer::record). The trace is
//! the cross-language replay contract: the conformance corpus stores one per
//! history, and every SDK must produce the same trace when its version of the
//! workflow replays that history.
//!
//! Most ops are calls the host made (with what they returned, where that is
//! deterministic). [`TraceOp::Advance`] is the replayer's answer to the host,
//! with the resolutions it produced.

use serde::{Deserialize, Serialize};

use crate::{Advance, Command, Resolution};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum TraceOp {
    /// `user_key(name)` returned `key`.
    UserKey {
        name: String,
        key: String,
    },
    /// `check_name(name)`.
    CheckName {
        name: String,
    },
    /// `key_for(name)` returned `key`.
    KeyFor {
        name: String,
        key: String,
    },
    /// `issue(key, command)`.
    Issue {
        key: String,
        command: Command,
    },
    /// `release(handle)` for the command issued under `key`.
    Release {
        key: String,
    },
    /// `random_u64()` returned `value`. A string, because JSON numbers lose
    /// precision above 2^53 in JavaScript.
    Random {
        #[serde(with = "u64_string")]
        value: u64,
    },
    /// `uuid()` returned `value`.
    Uuid {
        value: uuid::Uuid,
    },
    /// `set_fault(message)`, called by the host (decode failures, panics, ...).
    SetFault {
        message: String,
    },
    Interrupt,
    Cancel,
    /// `advance()` returned `result`, after producing these resolutions in
    /// history order.
    Advance {
        result: Advance,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        resolved: Vec<Resolved>,
    },
}

/// A resolution, by the key of the command it settles.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resolved {
    pub key: String,
    pub resolution: Resolution,
}

mod u64_string {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    pub fn serialize<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn ops_are_tagged_and_random_values_are_strings() {
        let op = TraceOp::Random { value: u64::MAX };
        let v = serde_json::to_value(&op).unwrap();
        assert_eq!(v, json!({"op": "random", "value": "18446744073709551615"}));
        assert_eq!(serde_json::from_value::<TraceOp>(v).unwrap(), op);
    }

    #[test]
    fn an_advance_without_resolutions_omits_the_list() {
        let op = TraceOp::Advance {
            result: Advance::Live,
            resolved: vec![],
        };
        let v = serde_json::to_value(&op).unwrap();
        assert_eq!(v, json!({"op": "advance", "result": "live"}));
        assert_eq!(serde_json::from_value::<TraceOp>(v).unwrap(), op);
    }
}
