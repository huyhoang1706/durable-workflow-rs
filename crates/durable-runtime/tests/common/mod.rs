#![allow(dead_code)]

use std::{
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use durable_runtime::{Client, StartOptions, Worker, WorkerBuilder, WorkerHandle};
use durable_spi::{Backend, Run, RunId, RunStatus};
use durable_testkit::{ManualClock, t0};
use serde_json::{Value, json};

pub struct Harness {
    pub clock: Arc<ManualClock>,
    pub backend: Backend,
    pub client: Client,
    workers: Vec<WorkerHandle>,
}

impl Harness {
    pub fn new() -> Self {
        let clock = ManualClock::new(t0());
        let backend = durable_memory::backend_with_clock(clock.clone());
        let client = Client::new(backend.clone());
        Self {
            clock,
            backend,
            client,
            workers: vec![],
        }
    }

    /// As [`new`](Self::new), over a caller-built backend that uses `clock`.
    pub fn with_backend(clock: Arc<ManualClock>, backend: Backend) -> Self {
        let client = Client::new(backend.clone());
        Self {
            clock,
            backend,
            client,
            workers: vec![],
        }
    }

    pub fn worker(&self) -> WorkerBuilder {
        Worker::builder(self.backend.clone())
            // Leases outlive any manual clock jump a test makes.
            .lease(Duration::from_secs(100_000_000))
            .drain_timeout(Duration::from_millis(300))
            .poll_interval(Duration::from_millis(20))
            .reconcile_interval(Duration::from_millis(50))
    }

    pub fn spawn(&mut self, b: WorkerBuilder) -> usize {
        self.workers.push(b.build().expect("worker builds").spawn());
        self.workers.len() - 1
    }

    pub async fn stop(&mut self, idx: usize) {
        self.workers.remove(idx).shutdown().await;
    }

    pub async fn start(&self, workflow: &str, input: Value) -> RunId {
        self.start_with(workflow, input, StartOptions::default())
            .await
    }

    pub async fn start_with(&self, workflow: &str, input: Value, o: StartOptions) -> RunId {
        for w in &self.workers {
            w.ready().await;
        }
        self.client
            .start_raw(workflow, input, o)
            .await
            .unwrap()
            .handle()
            .unwrap()
            .id()
    }

    pub fn advance(&self, secs: i64) {
        self.clock.advance(chrono::Duration::seconds(secs));
    }

    pub async fn run(&self, id: RunId) -> Run {
        self.client.get_run(id).await.unwrap().unwrap()
    }

    /// Poll until `pred` holds for the run, with a real-time timeout.
    pub async fn until(&self, id: RunId, what: &str, pred: impl Fn(&Run) -> bool) -> Run {
        let started = Instant::now();
        loop {
            let run = self.run(id).await;
            if pred(&run) {
                return run;
            }
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "timed out waiting for {what}; run: {run:#?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    pub async fn status(&self, id: RunId, s: RunStatus) -> Run {
        self.until(id, &format!("status {s:?}"), |r| r.status == s)
            .await
    }

    pub async fn succeeded(&self, id: RunId) -> Value {
        self.status(id, RunStatus::Succeeded).await.output.unwrap()
    }

    /// Waiting with a wake time: suspended on a timer.
    pub async fn suspended(&self, id: RunId) -> Run {
        self.until(id, "suspension", |r| r.status == RunStatus::Waiting)
            .await
    }

    pub async fn eventually<F: Future<Output = bool>>(&self, what: &str, f: impl Fn() -> F) {
        let started = Instant::now();
        while !f().await {
            assert!(
                started.elapsed() < Duration::from_secs(10),
                "timed out: {what}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }
}

pub fn n(v: &Value) -> i64 {
    v.as_i64().unwrap_or_else(|| panic!("not a number: {v}"))
}

pub fn j<T: serde::Serialize>(v: T) -> Value {
    json!(v)
}
