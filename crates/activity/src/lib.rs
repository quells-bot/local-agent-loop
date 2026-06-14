//! Activity-authoring surface (mirrors Temporal Go SDK `activity` package).

mod execution;
pub use execution::Execution;

mod error;
pub use error::Error;

mod info;
pub use info::Info;
