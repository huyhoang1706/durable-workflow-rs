//! The sans-IO core of durable-workflow: the event model, the replay state
//! machine and the engine's decisions. No tokio, no futures, no IO. Every SDK (Rust today; Node and
//! Python bindings next) drives the same [`Replayer`].

mod decide;
mod event;
mod ms;
mod replayer;
mod retry;
mod time;
mod trace;

pub use decide::*;
pub use event::*;
pub use replayer::*;
pub use retry::RetryPolicy;
pub use time::{far_future, saturating_add};
pub use trace::{Resolved, TraceOp};

pub type RunId = uuid::Uuid;
