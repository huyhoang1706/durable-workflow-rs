use durable_core::RunId;

pub(crate) fn nudge(namespace: &str, workflow: &str) -> String {
    format!("dw:{namespace}:nudge:{workflow}")
}

pub(crate) fn run_finished(id: RunId) -> String {
    format!("dw:run:{id}")
}

pub(crate) fn cancel(id: RunId) -> String {
    format!("dw:cancel:{id}")
}
