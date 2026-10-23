//! Ambient step metadata: a tokio task-local for async steps and a
//! thread-local for blocking ones, so step closures stay `|| ...`.

use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::Deps;

#[derive(Clone)]
pub struct StepInfo {
    pub(crate) attempt: u32,
    pub(crate) idempotency_key: String,
    pub(crate) cancelled: Arc<AtomicBool>,
    pub(crate) heartbeat: Arc<dyn Fn() + Send + Sync>,
    pub(crate) deps: Deps,
}

impl StepInfo {
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
    /// `run_id:step_key`, stable across retries and replays.
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }
    /// Long CPU steps should poll this: blocking code cannot be preempted.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }
    pub fn heartbeat(&self) {
        (self.heartbeat)()
    }
    pub fn dep<T: ?Sized + Send + Sync + 'static>(&self) -> Option<crate::Dep<T>> {
        self.deps.get::<T>()
    }
}

tokio::task_local! {
    static ASYNC_STEP: StepInfo;
}

thread_local! {
    static BLOCKING_STEP: RefCell<Option<StepInfo>> = const { RefCell::new(None) };
}

/// True inside a step body (async or blocking).
pub fn in_step() -> bool {
    try_current().is_some()
}

pub fn try_current() -> Option<StepInfo> {
    ASYNC_STEP
        .try_with(Clone::clone)
        .ok()
        .or_else(|| BLOCKING_STEP.with(|s| s.borrow().clone()))
}

/// Metadata of the running step. Panics outside a step.
pub fn current() -> StepInfo {
    try_current().expect("step::current() called outside a step")
}

pub(crate) async fn scope_async<F: std::future::Future>(info: StepInfo, f: F) -> F::Output {
    ASYNC_STEP.scope(info, f).await
}

pub(crate) fn scope_blocking<R>(info: StepInfo, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<StepInfo>);
    impl Drop for Restore {
        fn drop(&mut self) {
            BLOCKING_STEP.with(|s| *s.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(BLOCKING_STEP.with(|s| s.borrow_mut().replace(info)));
    f()
}

#[cfg(test)]
pub(crate) fn test_info(key: &str) -> StepInfo {
    StepInfo {
        attempt: 1,
        idempotency_key: key.into(),
        cancelled: Arc::new(AtomicBool::new(false)),
        heartbeat: Arc::new(|| {}),
        deps: Deps::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_in_a_step_by_default() {
        assert!(!in_step());
        assert!(try_current().is_none());
    }

    #[test]
    fn blocking_scope_exposes_info_and_restores() {
        let seen = scope_blocking(test_info("r:s"), || {
            assert!(in_step());
            current().idempotency_key().to_string()
        });
        assert_eq!(seen, "r:s");
        assert!(!in_step());
    }

    #[test]
    fn blocking_scope_is_restored_after_panic() {
        let r = std::panic::catch_unwind(|| scope_blocking(test_info("x"), || panic!("boom")));
        assert!(r.is_err());
        assert!(!in_step());
    }

    #[tokio::test]
    async fn async_scope_follows_the_task_not_the_thread() {
        assert!(!in_step());
        let inside = scope_async(test_info("a:b"), async {
            tokio::task::yield_now().await;
            (in_step(), current().attempt())
        })
        .await;
        assert_eq!(inside, (true, 1));
        assert!(!in_step());
    }

    #[test]
    fn cancellation_and_heartbeat_are_observable() {
        let info = test_info("k");
        assert!(!info.is_cancelled());
        info.cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        assert!(info.is_cancelled());
        info.heartbeat();
    }
}
