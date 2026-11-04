//! The event model lives in `durable-sm`, next to the replay state machine
//! that consumes it.

pub use durable_sm::{Event, EventKind, WaitKind};
