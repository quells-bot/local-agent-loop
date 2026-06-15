//! SQLite implementation of engine::History + engine::TaskQueue (spec §11, §15).
//! Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
//! docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
mod history_impl;
mod schema;
mod sqlite;
mod taskqueue_impl;

pub use sqlite::Sqlite;
