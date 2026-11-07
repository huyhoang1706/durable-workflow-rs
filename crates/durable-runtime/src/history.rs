//! Pure helpers over an event log.

use std::collections::HashSet;

use durable_spi::{Event, EventKind};

/// Prepare a stored log for replay.
///
/// After a `RunRetried`, steps that had failed for good get a fresh retry
/// budget, so their failure events (and the old terminal event) are dropped;
/// succeeded steps stay memoized.
pub fn normalize(events: Vec<Event>) -> Vec<Event> {
    let mut out: Vec<Event> = Vec::with_capacity(events.len());
    for ev in events {
        if !matches!(ev.kind, EventKind::RunRetried) {
            out.push(ev);
            continue;
        }
        let exhausted: HashSet<String> = out
            .iter()
            .filter_map(|e| match &e.kind {
                EventKind::StepFailed {
                    key,
                    retry_at: None,
                    ..
                } => Some(key.clone()),
                _ => None,
            })
            .collect();
        out.retain(|e| match &e.kind {
            EventKind::StepFailed { key, .. } => !exhausted.contains(key),
            k => !k.is_terminal(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use durable_spi::EventKind::{self, *};
    use serde_json::json;

    use super::*;

    fn log(kinds: Vec<EventKind>) -> Vec<Event> {
        kinds
            .into_iter()
            .enumerate()
            .map(|(i, kind)| Event {
                run_id: uuid::Uuid::nil(),
                seq: i as u64 + 1,
                at: Utc::now(),
                kind,
            })
            .collect()
    }

    fn fail(key: &str, attempt: u32, final_: bool) -> EventKind {
        StepFailed {
            key: key.into(),
            attempt,
            error: "e".into(),
            retryable: true,
            retry_at: (!final_).then(Utc::now),
        }
    }

    fn kinds(events: &[Event]) -> Vec<&EventKind> {
        events.iter().map(|e| &e.kind).collect()
    }

    #[test]
    fn untouched_without_a_retry() {
        let events = log(vec![
            RunStarted { input: json!(1) },
            fail("a", 1, false),
            fail("a", 2, true),
        ]);
        assert_eq!(normalize(events.clone()), events);
    }

    #[test]
    fn retry_drops_final_failures_terminal_event_and_the_marker() {
        let events = log(vec![
            RunStarted { input: json!(1) },
            StepSucceeded {
                key: "ok".into(),
                output: json!(1),
            },
            fail("bad", 1, false),
            fail("bad", 2, true),
            RunFailed { error: "x".into() },
            RunRetried,
        ]);
        let out = normalize(events);
        assert_eq!(
            kinds(&out),
            [
                &RunStarted { input: json!(1) },
                &StepSucceeded {
                    key: "ok".into(),
                    output: json!(1)
                },
            ]
        );
    }

    #[test]
    fn failures_after_the_retry_are_kept() {
        let events = log(vec![
            RunStarted { input: json!(1) },
            fail("bad", 1, true),
            RunFailed { error: "x".into() },
            RunRetried,
            fail("bad", 1, false),
        ]);
        let out = normalize(events);
        assert_eq!(out.len(), 2);
        assert!(matches!(
            &out[1].kind,
            StepFailed {
                retry_at: Some(_),
                ..
            }
        ));
    }
}
