use durable_core::{PubSub, RunId};

/// Publish a notice that readers also poll for, so a lost one only delays
/// them. Failures are logged at debug level rather than dropped silently.
pub(crate) async fn notify(pubsub: &dyn PubSub, channel: &str, payload: Vec<u8>) {
    if let Err(e) = pubsub.publish(channel, payload).await {
        tracing::debug!(channel, "publishing a notice failed: {e}");
    }
}

pub(crate) fn nudge(namespace: &str, workflow: &str) -> String {
    format!("dw:{namespace}:nudge:{workflow}")
}

/// Notices about a run: `{"seq": n}` after every commit, and the best-effort
/// `{"step_started": {"key", "attempt"}}`.
pub(crate) fn run(id: RunId) -> String {
    format!("dw:run:{id}")
}

/// "New chunks were flushed to this stream" (the payload is empty).
pub(crate) fn stream(id: RunId, name: &str) -> String {
    format!("dw:stream:{id}:{name}")
}

pub(crate) fn cancel(id: RunId) -> String {
    format!("dw:cancel:{id}")
}
