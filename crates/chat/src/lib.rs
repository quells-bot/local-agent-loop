//! LLM chat: a long-lived `ChatSession` workflow that answers user-message signals
//! via a local llama.cpp server, recording the transcript into the mock chat
//! service (chat spec). Replaces the integer `demo` crate.
mod activities;
mod types;

pub use activities::RecordMessage;
pub use types::{
    ChatSessionParams, ChatSessionResult, LlmParams, LlmResult, RecordResult, StopSignal,
    UserMessage,
};
