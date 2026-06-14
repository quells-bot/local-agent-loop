//! Workflow-authoring surface + replay protocol (mirrors Go SDK `workflow`).

pub use activity::Execution; // re-export so workflow::Execution exists (spec §9)

mod info;
pub use info::Info;

mod retry;
pub use retry::RetryPolicy;

mod command;
pub use command::Command;
