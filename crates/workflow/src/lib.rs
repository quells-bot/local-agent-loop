//! Workflow-authoring surface + replay protocol (mirrors Go SDK `workflow`).

pub use activity::Execution; // re-export so workflow::Execution exists (spec §9)

mod info;
pub use info::Info;

mod retry;
pub use retry::RetryPolicy;

mod command;
pub use command::Command;

mod event;
pub use event::Event;

mod result;
pub use result::CommandResult;

mod error;
pub use error::Error;

mod context;
pub use context::Context;

mod future;
pub use future::{ActivityFuture, TimerFuture};

mod def;
pub use def::Definition;

mod state;
pub use state::WorkflowState;

mod replay;
pub use replay::{cold_replay, Nondeterminism, ReplayOutcome};
