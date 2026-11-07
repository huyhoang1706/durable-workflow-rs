use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
};

use durable_spi::{ComputePool, Job};
use parking_lot::Mutex;

/// Fixed-size pool for blocking (CPU-bound) steps. Never runs on tokio threads.
pub struct ThreadPool {
    tx: Mutex<Option<mpsc::Sender<Job>>>,
    pending: Arc<AtomicUsize>,
}

impl ThreadPool {
    pub fn new(threads: usize) -> Self {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        let pending = Arc::new(AtomicUsize::new(0));
        for i in 0..threads.max(1) {
            let (rx, pending) = (rx.clone(), pending.clone());
            std::thread::Builder::new()
                .name(format!("durable-compute-{i}"))
                .spawn(move || {
                    loop {
                        // Hold the lock only while waiting for a job.
                        let job = rx.lock().recv();
                        let Ok(job) = job else { return };
                        // A panicking job must not take the worker down.
                        let _ = catch_unwind(AssertUnwindSafe(job));
                        pending.fetch_sub(1, Ordering::SeqCst);
                    }
                })
                .expect("spawn compute thread");
        }
        Self {
            tx: Mutex::new(Some(tx)),
            pending,
        }
    }

    pub fn default_size() -> usize {
        std::thread::available_parallelism().map_or(4, |n| n.get())
    }
}

impl ComputePool for ThreadPool {
    fn execute(&self, job: Job) {
        self.pending.fetch_add(1, Ordering::SeqCst);
        if let Some(tx) = self.tx.lock().as_ref() {
            let _ = tx.send(job);
        }
    }
    fn backlog(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering::SeqCst},
            mpsc,
        },
        time::Duration,
    };

    use super::*;

    #[test]
    fn parallelism_is_bounded_by_pool_size() {
        let pool = ThreadPool::new(2);
        let (running, peak) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (tx, rx) = mpsc::channel();
        for _ in 0..6 {
            let (running, peak, tx) = (running.clone(), peak.clone(), tx.clone());
            pool.execute(Box::new(move || {
                let now = running.fetch_add(1, SeqCst) + 1;
                peak.fetch_max(now, SeqCst);
                std::thread::sleep(Duration::from_millis(30));
                running.fetch_sub(1, SeqCst);
                tx.send(()).unwrap();
            }));
        }
        for _ in 0..6 {
            rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        assert_eq!(peak.load(SeqCst), 2);
    }

    #[test]
    fn backlog_counts_queued_and_running_then_drains() {
        let pool = ThreadPool::new(1);
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel();
        let first_done = done_tx.clone();
        pool.execute(Box::new(move || {
            release_rx.recv().unwrap();
            first_done.send(()).unwrap();
        }));
        pool.execute(Box::new(move || done_tx.send(()).unwrap()));
        assert_eq!(pool.backlog(), 2);
        release_tx.send(()).unwrap();
        for _ in 0..2 {
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(pool.backlog(), 0);
    }

    #[test]
    fn a_panicking_job_does_not_kill_the_worker() {
        let pool = ThreadPool::new(1);
        pool.execute(Box::new(|| panic!("job panic")));
        let (tx, rx) = mpsc::channel();
        pool.execute(Box::new(move || tx.send(()).unwrap()));
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }
}
