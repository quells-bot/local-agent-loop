//! The long-lived chat session workflow (chat spec). One run per open window:
//! answer `"message"` signals (record user msg → LLM → record reply) until a
//! `"stop"` signal terminates it. Uses `select_biased!` — the engine-approved
//! deterministic combinator (CLAUDE.md / engine spec §4.2).
use futures::FutureExt;
use workflow::{Context, Definition, Error, RetryPolicy};

use crate::activities::{LlmComplete, RecordMessage};
use crate::types::{ChatSessionParams, ChatSessionResult, LlmParams, StopSignal, UserMessage};
use chat_service::RecordArgs;

pub struct ChatSession;

#[async_trait::async_trait(?Send)]
impl Definition for ChatSession {
    type Input = ChatSessionParams;
    type Output = ChatSessionResult;
    const TYPE: &'static str = "ChatSession";
    async fn run(ctx: Context, params: ChatSessionParams) -> Result<ChatSessionResult, Error> {
        let conversation_id = params.conversation_id;
        let messages = ctx.signal_channel::<UserMessage>("message");
        let stop = ctx.signal_channel::<StopSignal>("stop");
        loop {
            futures::select_biased! {
                _ = stop.recv().fuse() => break, // window closed -> terminate
                msg = messages.recv().fuse() => {
                    let msg = msg?;
                    // 1. Record the user's message.
                    ctx.activity::<RecordMessage>(RecordArgs {
                        conversation_id: conversation_id.clone(),
                        message_id: msg.message_id.clone(),
                        role: "user".into(),
                        content: msg.text.clone(),
                        status: "complete".into(),
                    })
                    .await?;
                    // 2. Ask the LLM (bounded retry). A failure becomes a visible
                    //    error reply, never fails the session (handled inline, no `?`).
                    let reply = ctx
                        .activity::<LlmComplete>(LlmParams {
                            conversation_id: conversation_id.clone(),
                        })
                        .retry(RetryPolicy::exponential(4))
                        .await;
                    let (content, status) = match reply {
                        Ok(r) => (r.reply, "complete"),
                        Err(e) => (e.message, "error"),
                    };
                    // 3. Record the assistant reply (or error) under "{id}-reply".
                    ctx.activity::<RecordMessage>(RecordArgs {
                        conversation_id: conversation_id.clone(),
                        message_id: format!("{}-reply", msg.message_id),
                        role: "assistant".into(),
                        content,
                        status: status.into(),
                    })
                    .await?;
                }
            }
        }
        Ok(ChatSessionResult {})
    }
}
