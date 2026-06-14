//! SQLite implementation of engine::History + engine::TaskQueue (spec §11, §15).
mod schema;
mod sqlite;
mod history_impl;
mod taskqueue_impl;

pub use sqlite::Sqlite;
