use activity::{Context, Definition, Error};
use chat_service::{Client, RecordArgs};

use crate::types::RecordResult;

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

#[cfg(test)]
mod tests {
    use super::*;
    use activity::{Context, Execution, Info};
    use chat_service::{Client, RecordArgs};

    fn ctx() -> Context {
        Context::new(Info {
            execution: Execution { workflow_id: "w".into(), run_id: "r".into() },
            activity_id: "0".into(),
            activity_type: "RecordMessage".into(),
            attempt: 1,
        })
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
}
