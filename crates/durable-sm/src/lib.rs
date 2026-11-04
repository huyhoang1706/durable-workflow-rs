//! The sans-IO core of durable-workflow: the event model and the replay state
//! machine. No tokio, no futures, no IO. Every SDK (Rust today; Node and
//! Python bindings next) drives the same [`Replayer`].

mod event;
mod replayer;
mod retry;

pub use event::*;
pub use replayer::*;
pub use retry::RetryPolicy;

pub type RunId = uuid::Uuid;
