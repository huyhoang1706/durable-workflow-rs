use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConcurrencySpec {
    pub limit: u32,
    /// JSON pointer into the workflow input; `None` means one workflow-wide bucket.
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThrottleSpec {
    pub limit: u32,
    pub period_secs: u64,
    /// Starts allowed back-to-back; defaults to 1 (evenly spaced).
    #[serde(default = "one")]
    pub burst: u32,
    pub key: Option<String>,
}

fn one() -> u32 {
    1
}

/// Lossy start-time limit: excess starts are rejected, not delayed.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RateLimitSpec {
    pub limit: u32,
    pub period_secs: u64,
    pub key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDefinition {
    pub name: String,
    pub concurrency: Option<ConcurrencySpec>,
    pub throttle: Option<ThrottleSpec>,
    pub rate_limit: Option<RateLimitSpec>,
    /// Whole-run deadline in seconds.
    pub timeout_secs: Option<u64>,
}

impl WorkflowDefinition {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ..Default::default()
        }
    }

    /// Derive `(concurrency_key, throttle_key)` for an input.
    pub fn flow_keys(&self, input: &Value) -> (Option<String>, Option<String>) {
        let concurrency = self
            .concurrency
            .as_ref()
            .map(|c| bucket(c.key.as_deref(), input));
        let throttle = self
            .throttle
            .as_ref()
            .map(|t| bucket(t.key.as_deref(), input));
        (concurrency, throttle)
    }
}

fn bucket(pointer: Option<&str>, input: &Value) -> String {
    match pointer.and_then(|p| input.pointer(p)) {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Null) | None => "-".into(),
        Some(other) => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn def() -> WorkflowDefinition {
        WorkflowDefinition {
            name: "w".into(),
            concurrency: Some(ConcurrencySpec {
                limit: 2,
                key: Some("/channel_id".into()),
            }),
            throttle: Some(ThrottleSpec {
                limit: 5,
                period_secs: 60,
                burst: 1,
                key: None,
            }),
            rate_limit: None,
            timeout_secs: None,
        }
    }

    #[test]
    fn keys_come_from_json_pointers() {
        let (c, t) = def().flow_keys(&json!({"channel_id": "c1"}));
        assert_eq!(c.as_deref(), Some("c1"));
        assert_eq!(t.as_deref(), Some("-")); // no key => single bucket
    }

    #[test]
    fn numbers_are_stringified_and_missing_pointer_falls_back_to_bucket() {
        let (c, _) = def().flow_keys(&json!({"channel_id": 7}));
        assert_eq!(c.as_deref(), Some("7"));
        let (c, _) = def().flow_keys(&json!({}));
        assert_eq!(c.as_deref(), Some("-"));
    }

    #[test]
    fn no_specs_means_no_keys() {
        assert_eq!(
            WorkflowDefinition::new("x").flow_keys(&json!({})),
            (None, None)
        );
    }
}
