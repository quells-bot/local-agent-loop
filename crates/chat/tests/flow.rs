use std::sync::Arc;

use chat::{ChatSession, ChatSessionParams, LlmComplete, LlmParams, LlmResult, RecordMessage};
use chat_service::Client;
use engine::{Engine, ExecStatus, History, StartOptions, TaskQueue};
use persist::Sqlite;

// Fake LlmComplete: same TYPE, real public Input/Output types reused, canned reply
// (DI design §4 "fake struct, same TYPE"). The workflow only references the TYPE
// string, so this overrides the real activity in the registry.
struct FakeLlm {
    canned: LlmResult,
}

#[async_trait::async_trait]
impl activity::Definition for FakeLlm {
    type Input = LlmParams;
    type Output = LlmResult;
    const TYPE: &'static str = <LlmComplete as activity::Definition>::TYPE;
    async fn run(
        &self,
        _c: activity::Context,
        _i: LlmParams,
    ) -> Result<LlmResult, activity::Error> {
        Ok(self.canned.clone())
    }
}

async fn pump(engine: &Engine) {
    loop {
        let drove = engine.process_one_runnable().await.unwrap();
        let worked = engine.process_one_activity().await.unwrap();
        if !drove && !worked {
            return;
        }
    }
}

#[tokio::test]
async fn message_signal_records_user_then_assistant_reply_and_stop_terminates() {
    let db = Sqlite::open_in_memory().unwrap();
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn TaskQueue> = Arc::new(db.clone());
    let chat = Client::open(":memory:").unwrap();

    let mut e = Engine::new(h, q);
    e.register_workflow::<ChatSession>();
    e.register_activity(RecordMessage::new(chat.clone()));
    e.register_activity(FakeLlm { canned: LlmResult { reply: "hi there".into() } });

    e.start_workflow::<ChatSession>(
        ChatSessionParams { conversation_id: "c1".into() },
        StartOptions { id: "c1".into() },
    )
    .await
    .unwrap();
    pump(&e).await; // drive to the parked select

    // Deliver a user message as a signal.
    let payload = serde_json::to_vec(
        &serde_json::json!({ "message_id": "m1", "text": "hello" }),
    )
    .unwrap();
    e.signal_workflow("c1", "message", &payload).await.unwrap();
    pump(&e).await;

    // The session recorded user then assistant, in order, with the canned reply.
    let got: Vec<(String, String, String)> = chat
        .list_messages("c1")
        .unwrap()
        .into_iter()
        .map(|m| (m.message_id, m.role, m.content))
        .collect();
    assert_eq!(
        got,
        vec![
            ("m1".into(), "user".into(), "hello".into()),
            ("m1-reply".into(), "assistant".into(), "hi there".into()),
        ]
    );

    // A "stop" signal terminates the run.
    e.signal_workflow("c1", "stop", b"{}").await.unwrap();
    pump(&e).await;
    let (_run, status, _res) = db.find_execution("c1").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
}
