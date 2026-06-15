//! Workflow-authoring surface + replay protocol (mirrors Go SDK `workflow`).
//!
//! ## Deterministic concurrency contract (spec §4.2)
//!
//! Workflow code may use only combinators whose poll/branch order is deterministic,
//! so that with the one-event-per-turn rule (spec §4.1) they replay identically:
//!
//! - **Allowed:** `futures::join!`, `futures::try_join!`, ordered `join_all`, and
//!   `futures::select_biased!` (the `workflow.Selector` analog). Its winner is a pure
//!   function of recorded history: under one-event-per-turn (spec §4.1) only one
//!   branch can become ready per poll, so the first completion in `history.event_id`
//!   order wins; registration order is only the tie-break that one-event-per-turn
//!   never needs. Spawn detached branches with [`Context::spawn`].
//! - **Banned (non-deterministic):** `futures::select!` (randomizes branch order)
//!   and bare `FuturesUnordered` (reorders by wakeup/wall-clock order). Using either
//!   breaks replay.
//!
//! These bans are a documented contract today; a `#[workflow]` macro / clippy lint
//! (the `workflow-macros` crate) is deferred to Pass 5.

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
pub use result::{ChildResult, CommandResult};

mod error;
pub use error::Error;

mod context;
pub use context::Context;

mod future;
pub use future::{ActivityFuture, TimerFuture};

mod spawn;
pub use spawn::SpawnHandle;

mod signal;
pub use signal::{SignalChannel, SignalRecv};

mod def;
pub use def::Definition;

mod state;
pub use state::WorkflowState;

mod replay;
pub use replay::{cold_replay, Nondeterminism, ReplayOutcome};
