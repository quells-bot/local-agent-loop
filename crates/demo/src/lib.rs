//! Demo workflows + activities for the Tauri chat app: parse space-separated
//! integers, then sum them via a child workflow (spec §4).
mod activities;
mod types;
mod workflows;

pub use activities::{Parse, SumActivity};
pub use types::{
    ParentParams, ParentResult, ParseParams, ParseResult, SumChildParams, SumChildResult,
    SumParams, SumResult,
};
pub use workflows::{Parent, SumChild};
