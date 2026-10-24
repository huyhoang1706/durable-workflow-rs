use durable_core::RunId;

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
