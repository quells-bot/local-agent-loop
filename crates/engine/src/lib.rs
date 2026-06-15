//! Backend-agnostic engine surface (traits + driver). Spec §5, §15.
//! Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec: docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
mod types;
pub use types::*;

mod traits;
pub use traits::{History, TaskQueue};

mod engine;
pub use engine::{Engine, Handle, RunCompleted, SignalError, StartOptions};
