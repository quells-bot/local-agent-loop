use activity::{Context, Definition, Error};
use chat_service::{Client, RecordArgs};
use serde::Serialize;

use crate::types::{LlmParams, LlmResult, RecordResult};

/// Records one message into the chat service. The instance holds the injected
/// service client. At-least-once safe: the underlying insert is idempotent by
/// `(conversation_id, message_id)`.
pub struct RecordMessage {
    chat: Client,
}

impl RecordMessage {
    pub fn new(chat: Client) -> Self {
        Self { chat }
    }
}

#[async_trait::async_trait]
impl Definition for RecordMessage {
    type Input = RecordArgs;
    type Output = RecordResult;
    const TYPE: &'static str = "RecordMessage";
    async fn run(&self, _ctx: Context, args: RecordArgs) -> Result<RecordResult, Error> {
        self.chat
            .record_message(args)
            .map_err(|e| Error::retryable(format!("record_message: {e}")))?;
        Ok(RecordResult {})
    }
}

/// One OpenAI-style chat message (request side).
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct OaiMessage {
    pub role: String,
    pub content: String,
}

/// The POST body for `/v1/chat/completions`.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ChatRequest {
    pub model: String,
    pub messages: Vec<OaiMessage>,
    pub stream: bool,
}

/// Build the request body from the conversation's `complete` messages (error rows
/// are not sent to the model). Pure; unit-tested without a live server.
pub fn build_request(messages: &[chat_service::ChatMessage], model: &str) -> ChatRequest {
    let oai = messages
        .iter()
        .filter(|m| m.status == "complete")
        .map(|m| OaiMessage { role: m.role.clone(), content: m.content.clone() })
        .collect();
    ChatRequest { model: model.to_string(), messages: oai, stream: false }
}

/// Extract the assistant reply from a `/v1/chat/completions` response body. A 2xx
/// body without `choices[0].message.content` is a FATAL error (no retry helps).
pub fn parse_response(json: &serde_json::Value) -> Result<String, Error> {
    json.get("choices")
        .and_then(|c| c.get(0))
        .and_then(|c| c.get("message"))
        .and_then(|m| m.get("content"))
        .and_then(|c| c.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| Error::fatal("malformed LLM response: no choices[0].message.content"))
}

/// Calls the local llama.cpp server for the next assistant reply. The instance
/// holds the chat-service client, an HTTP client, and LLM config.
pub struct LlmComplete {
    chat: Client,
    http: reqwest::Client,
    base_url: String,
    model: String,
}

impl LlmComplete {
    pub fn new(chat: Client, http: reqwest::Client, base_url: String, model: String) -> Self {
        Self { chat, http, base_url, model }
    }
}

#[async_trait::async_trait]
impl Definition for LlmComplete {
    type Input = LlmParams;
    type Output = LlmResult;
    const TYPE: &'static str = "LlmComplete";
    async fn run(&self, _ctx: Context, params: LlmParams) -> Result<LlmResult, Error> {
        let history = self
            .chat
            .list_messages(&params.conversation_id)
            .map_err(|e| Error::retryable(format!("list_messages: {e}")))?;
        let body = build_request(&history, &self.model);
        let url = format!("{}/v1/chat/completions", self.base_url);
        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| Error::retryable(format!("llm request failed: {e}")))?;
        if !resp.status().is_success() {
            // Network/5xx/non-2xx is transient -> retryable so the engine backs off.
            let code = resp.status();
            return Err(Error::retryable(format!("llm returned HTTP {code}")));
        }
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| Error::retryable(format!("llm response read failed: {e}")))?;
        let reply = parse_response(&json)?;
        Ok(LlmResult { reply })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use activity::{Context, Execution, Info};
    use chat_service::{Client, RecordArgs};
    use serde_json::json;

    fn ctx() -> Context {
        Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            activity_id: "0".into(),
            activity_type: "RecordMessage".into(),
            attempt: 1,
        })
    }

    fn msg(role: &str, content: &str, status: &str, seq: i64) -> chat_service::ChatMessage {
        chat_service::ChatMessage {
            conversation_id: "c1".into(),
            message_id: format!("m{seq}"),
            role: role.into(),
            content: content.into(),
            status: status.into(),
            seq,
            created_at: 0,
        }
    }

    #[tokio::test]
    async fn record_message_writes_a_row() {
        let chat = Client::open(":memory:").unwrap();
        let act = RecordMessage::new(chat.clone());
        act.run(ctx(), RecordArgs {
            conversation_id: "c1".into(),
            message_id: "m1".into(),
            role: "user".into(),
            content: "hello".into(),
            status: "complete".into(),
        })
        .await
        .unwrap();
        let msgs = chat.list_messages("c1").unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].content, "hello");
    }

    #[test]
    fn build_request_keeps_complete_rows_in_order() {
        let history = vec![
            msg("user", "hi", "complete", 1),
            msg("assistant", "broken", "error", 2), // dropped
            msg("user", "again", "complete", 3),
        ];
        let req = build_request(&history, "test-model");
        assert_eq!(req.model, "test-model");
        assert!(!req.stream);
        assert_eq!(
            req.messages,
            vec![
                OaiMessage { role: "user".into(), content: "hi".into() },
                OaiMessage { role: "user".into(), content: "again".into() },
            ]
        );
    }

    #[test]
    fn parse_response_extracts_reply() {
        let body = json!({ "choices": [ { "message": { "content": "hello there" } } ] });
        assert_eq!(parse_response(&body).unwrap(), "hello there");
    }

    #[test]
    fn parse_response_malformed_is_fatal() {
        let body = json!({ "choices": [] });
        let err = parse_response(&body).unwrap_err();
        assert!(err.non_retryable, "a malformed 2xx body is non-retryable");
    }
}
