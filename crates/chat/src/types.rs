//! Bespoke params/results + signal payloads for the chat workflow and its
//! activities (CLAUDE.md: every activity/workflow uses named structs). The
//! `RecordMessage` activity reuses `chat_service::RecordArgs` as its `Input`, so it
//! is not re-declared here.
use serde::{Deserialize, Serialize};

/// `ChatSession` workflow input: the conversation this session owns.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatSessionParams {
    pub conversation_id: String,
}

/// `ChatSession` workflow output (the session carries no transcript — the service
/// owns it — so the result is empty).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatSessionResult {}

/// `"message"` signal payload: a user message to answer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserMessage {
    pub message_id: String,
    pub text: String,
}

/// `"stop"` signal payload (the window closed).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StopSignal {}

/// `RecordMessage` activity output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordResult {}

/// `LlmComplete` activity input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmParams {
    pub conversation_id: String,
}

/// `LlmComplete` activity output.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmResult {
    pub reply: String,
}
