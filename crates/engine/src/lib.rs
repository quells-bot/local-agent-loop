//! Backend-agnostic engine surface (traits + driver). Spec §5, §15.
mod types;
pub use types::*;

mod traits;
pub use traits::{History, TaskQueue};

mod engine;
pub use engine::{Engine, Handle, RunCompleted, SignalError, StartOptions};
