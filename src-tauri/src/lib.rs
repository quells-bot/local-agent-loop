use std::sync::Arc;

use engine::{
    Engine, ExecStatus, ExecutionSummary, History, HistoryRecord, RunCompleted, StartOptions,
    TaskQueue,
};
use workflow::{ChildResult, Event};
use persist::Sqlite;
use serde::Serialize;
use tauri::{Emitter, Manager, State};
use tauri::menu::{MenuBuilder, MenuItemBuilder, SubmenuBuilder};

use chat::{ChatSession, ChatSessionParams, LlmComplete, RecordMessage};
use chat_service::{ChatMessage, Client as ChatClient};

/// Local llama.cpp endpoint and model (chat spec). Host constants for now.
const LLM_BASE_URL: &str = "http://mss1.quells.house:8080";
const LLM_MODEL: &str = "Qwen3.6-35B-A3B-MTP-GGUF";

/// Pushed to the frontend after a run reaches a terminal status (spec §7.3).
/// `result` is decoded as a generic JSON value so this host stays demo-agnostic:
/// on `completed` it is the workflow's result object (`{ "total": N }`); on
/// `failed` it is the `workflow::Error` object (`{ "message": "..." }`).
#[derive(Clone, Serialize)]
struct CompletionPayload {
    workflow_id: String,
    run_id: String,
    status: &'static str,
    result: Option<serde_json::Value>,
}

/// Open a fresh chat session: start the long-lived `ChatSession` workflow, deduped
/// by the frontend-supplied `conversation_id` (also the workflow id).
#[tauri::command]
async fn open_chat(
    conversation_id: String,
    engine: State<'_, Arc<Engine>>,
) -> Result<(), String> {
    engine
        .start_workflow::<ChatSession>(
            ChatSessionParams { conversation_id: conversation_id.clone() },
            StartOptions { id: conversation_id },
        )
        .await
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// Deliver a user message to the session as a durable `"message"` signal.
#[tauri::command]
async fn send_message(
    conversation_id: String,
    message_id: String,
    text: String,
    engine: State<'_, Arc<Engine>>,
) -> Result<(), String> {
    let payload = serde_json::to_vec(
        &serde_json::json!({ "message_id": message_id, "text": text }),
    )
    .map_err(|e| e.to_string())?;
    engine
        .signal_workflow(&conversation_id, "message", &payload)
        .await
        .map_err(|e| e.to_string())
}

/// Read the transcript for a conversation from the chat service (frontend poll).
#[tauri::command]
async fn chat_history(
    conversation_id: String,
    chat: State<'_, ChatClient>,
) -> Result<Vec<ChatMessageDto>, String> {
    let msgs = chat.list_messages(&conversation_id).map_err(|e| e.to_string())?;
    Ok(msgs.into_iter().map(message_dto).collect())
}

/// Terminate the session via a durable `"stop"` signal (window closing).
#[tauri::command]
async fn close_chat(
    conversation_id: String,
    engine: State<'_, Arc<Engine>>,
) -> Result<(), String> {
    engine
        .signal_workflow(&conversation_id, "stop", b"{}")
        .await
        .map_err(|e| e.to_string())
}

/// List root runs for the history viewer, newest first (history-viewer design §5.1).
#[tauri::command]
async fn list_runs(
    history: State<'_, Arc<dyn History>>,
) -> Result<Vec<RunSummaryDto>, String> {
    let runs = history.list_executions().await.map_err(|e| e.to_string())?;
    Ok(runs.into_iter().map(summary_dto).collect())
}

/// Full detail (header + humanized timeline) for one run (history-viewer design §5.1).
#[tauri::command]
async fn run_detail(
    run_id: String,
    history: State<'_, Arc<dyn History>>,
) -> Result<RunDetailDto, String> {
    let meta = history
        .load_run(&run_id)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("run not found: {run_id}"))?;
    let records = history.read_events(&run_id).await.map_err(|e| e.to_string())?;
    // The run's terminal result lives on the execution row (not the event stream);
    // decode it to generic JSON, demo-agnostic, like `completion_payload`.
    let result = history
        .find_execution(&meta.workflow_id)
        .await
        .map_err(|e| e.to_string())?
        .and_then(|(_, _, bytes)| bytes)
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
    let started_at = records.first().map(|r| r.ts).unwrap_or(0);
    let last_event_at = records.last().map(|r| r.ts).unwrap_or(0);
    let summary = RunSummaryDto {
        run_id: meta.run_id,
        workflow_id: meta.workflow_id,
        workflow_type: meta.workflow_type,
        status: meta.status.as_str().to_string(),
        started_at,
        last_event_at,
        event_count: records.len() as i64,
    };
    let events = records.into_iter().map(event_to_json).collect();
    Ok(RunDetailDto {
        summary,
        result,
        events,
    })
}

/// Map a terminal `RunCompleted` into the event payload pushed to the frontend
/// (spec §7.3). Extracted as a free function so the result-bytes → payload
/// mapping is unit-testable without a running Tauri app.
fn completion_payload(ev: RunCompleted) -> CompletionPayload {
    CompletionPayload {
        workflow_id: ev.workflow_id,
        run_id: ev.run_id,
        status: if matches!(ev.status, ExecStatus::Completed) {
            "completed"
        } else {
            "failed"
        },
        result: ev
            .result
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok()),
    }
}

/// One row of the run list, pushed to the `/history` viewer.
#[derive(Clone, Serialize)]
struct RunSummaryDto {
    run_id: String,
    workflow_id: String,
    workflow_type: String,
    status: String,
    started_at: i64,
    last_event_at: i64,
    event_count: i64,
}

/// One humanized timeline row. `detail` carries the decoded, demo-agnostic
/// payload fields (input/output/error/…), flattened alongside the envelope fields.
#[derive(Clone, Serialize)]
struct EventDto {
    event_id: i64,
    ts: i64,
    kind: String,
    #[serde(flatten)]
    detail: serde_json::Value,
    child_run_id: Option<String>,
}

#[derive(Clone, Serialize)]
struct RunDetailDto {
    summary: RunSummaryDto,
    /// The run's terminal output (workflow result object) or failure (`workflow::Error`),
    /// decoded as generic JSON. `None` while the run is still `Running`. This lives on
    /// the execution row, not in the event stream, so the viewer surfaces it here.
    result: Option<serde_json::Value>,
    events: Vec<EventDto>,
}

/// One chat-history row sent to the frontend poll. `created_at` is dropped — the
/// frontend orders by `seq` and renders by role/content/status.
#[derive(Clone, Serialize)]
struct ChatMessageDto {
    message_id: String,
    role: String,
    content: String,
    status: String,
    seq: i64,
}

/// Map a service `ChatMessage` to the frontend DTO. Free function so it is
/// unit-testable without a running Tauri app (mirrors `completion_payload`).
fn message_dto(m: ChatMessage) -> ChatMessageDto {
    ChatMessageDto {
        message_id: m.message_id,
        role: m.role,
        content: m.content,
        status: m.status,
        seq: m.seq,
    }
}

fn summary_dto(s: ExecutionSummary) -> RunSummaryDto {
    RunSummaryDto {
        run_id: s.run_id,
        workflow_id: s.workflow_id,
        workflow_type: s.workflow_type,
        status: s.status.as_str().to_string(),
        started_at: s.started_at,
        last_event_at: s.last_event_at,
        event_count: s.event_count,
    }
}

/// Decode an event's inner byte payload to JSON so the host stays demo-agnostic.
/// Non-JSON bytes (shouldn't happen for our payloads) decode to `null`.
fn decode(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap_or(serde_json::Value::Null)
}

/// Humanize a `ChildResult`, decoding its inner output / failure-error.
fn child_result_json(result: &ChildResult) -> serde_json::Value {
    use serde_json::json;
    match result {
        ChildResult::Completed(output) => json!({ "status": "completed", "output": decode(output) }),
        ChildResult::Failed(error) => json!({ "status": "failed", "error": error }),
    }
}

/// Map a read-model `HistoryRecord` to the humanized DTO the viewer renders.
/// Inner byte payloads are decoded to generic JSON (mirrors `completion_payload`,
/// keeping `src-tauri` demo-agnostic). Unit-testable without a running Tauri app.
fn event_to_json(record: HistoryRecord) -> EventDto {
    use serde_json::json;
    let kind = record.event.kind().to_string();
    let detail = match &record.event {
        Event::WorkflowStarted { input } => json!({ "input": decode(input) }),
        Event::ActivityScheduled {
            seq,
            activity_type,
            input,
            retry,
        } => json!({
            "seq": seq,
            "activity_type": activity_type,
            "input": decode(input),
            "retry": retry,
        }),
        Event::ActivityCompleted { seq, output } => {
            json!({ "seq": seq, "output": decode(output) })
        }
        Event::ActivityFailed { seq, error } => json!({ "seq": seq, "error": error }),
        Event::TimerStarted { seq, duration_ms } => {
            json!({ "seq": seq, "duration_ms": duration_ms })
        }
        Event::TimerFired { seq } => json!({ "seq": seq }),
        Event::SignalReceived { name, payload } => {
            json!({ "name": name, "payload": decode(payload) })
        }
        Event::ChildScheduled {
            seq,
            workflow_type,
            input,
        } => json!({ "seq": seq, "workflow_type": workflow_type, "input": decode(input) }),
        Event::ChildCompleted { seq, result } => {
            json!({ "seq": seq, "result": child_result_json(result) })
        }
        Event::Patched { change_id } => json!({ "change_id": change_id }),
    };
    EventDto {
        event_id: record.event_id,
        ts: record.ts,
        kind,
        detail,
        child_run_id: record.child_run_id,
    }
}

pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            // Durable store under the OS app-data dir.
            let dir = app.path().app_data_dir().expect("app data dir");
            std::fs::create_dir_all(&dir).ok();
            let db_path = dir.join("workflows.db");
            let db = Sqlite::open(db_path.to_str().expect("utf-8 db path")).expect("open db");

            let history: Arc<dyn History> = Arc::new(db.clone());
            let queue: Arc<dyn TaskQueue> = Arc::new(db.clone());
            let mut engine = Engine::new(history.clone(), queue);
            // Mock chat service in its own DB (separate from workflows.db).
            let chat_db_path = dir.join("chat-service.db");
            let chat = ChatClient::open(
                chat_db_path.to_str().expect("utf-8 chat db path"),
            )
            .expect("open chat-service db");

            engine.register_workflow::<ChatSession>();
            engine.register_activity(RecordMessage::new(chat.clone()));
            engine.register_activity(LlmComplete::new(
                chat.clone(),
                reqwest::Client::new(),
                LLM_BASE_URL.into(),
                LLM_MODEL.into(),
            ));

            // Push terminal completions to the frontend (spec §7.3).
            let app_handle = app.handle().clone();
            engine.on_run_completed(move |ev: RunCompleted| {
                let _ = app_handle.emit("run_completed", completion_payload(ev));
            });

            // `Engine::start` uses `tokio::spawn`, so a tokio runtime must be the
            // current runtime when we call it. Build one for the engine's
            // background loops and keep it alive for the app's lifetime.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("tokio runtime");
            let engine = {
                let _guard = rt.enter();
                engine.start() // spawns driver/worker/timer/sweeper loops on `rt`
            };
            app.manage(rt); // keep the runtime (and its threads) alive
            app.manage(engine);
            // Expose the read model to the history viewer commands.
            app.manage(history);
            // Expose the chat-service client to the `chat_history` read command.
            app.manage(chat);

            // "View ▸ Workflow History…" opens a second window at /history.
            let history_item =
                MenuItemBuilder::with_id("history", "Workflow History…").build(app)?;
            let view_menu = SubmenuBuilder::new(app, "View")
                .item(&history_item)
                .build()?;
            let menu = MenuBuilder::new(app).item(&view_menu).build()?;
            app.set_menu(menu)?;

            app.on_menu_event(move |app, event| {
                if event.id().as_ref() == "history" {
                    if let Some(w) = app.get_webview_window("history") {
                        let _ = w.set_focus();
                    } else if let Err(e) = tauri::WebviewWindowBuilder::new(
                        app,
                        "history",
                        // Loads the /history SvelteKit route (via the adapter-static SPA fallback).
                        tauri::WebviewUrl::App("history".into()),
                    )
                    .title("Workflow History")
                    .inner_size(900.0, 640.0)
                    .build()
                    {
                        eprintln!("[history window] failed to open: {e}");
                    }
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            open_chat,
            send_message,
            chat_history,
            close_chat,
            list_runs,
            run_detail
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ev(status: ExecStatus, result: Option<serde_json::Value>) -> RunCompleted {
        RunCompleted {
            run_id: "run-1".into(),
            workflow_id: "wf-1".into(),
            status,
            result: result.map(|v| serde_json::to_vec(&v).unwrap()),
        }
    }

    #[test]
    fn completed_run_maps_to_completed_status_and_total() {
        let p = completion_payload(ev(ExecStatus::Completed, Some(json!({ "total": 6 }))));
        assert_eq!(p.workflow_id, "wf-1");
        assert_eq!(p.run_id, "run-1");
        assert_eq!(p.status, "completed");
        assert_eq!(p.result, Some(json!({ "total": 6 })));
    }

    #[test]
    fn failed_run_maps_to_failed_status_and_message() {
        let p = completion_payload(ev(
            ExecStatus::Failed,
            Some(json!({ "message": "could not parse 'two' as an integer" })),
        ));
        assert_eq!(p.status, "failed");
        let msg = p.result.unwrap();
        assert!(msg["message"].as_str().unwrap().contains("two"));
    }

    #[test]
    fn missing_result_bytes_map_to_none() {
        let p = completion_payload(ev(ExecStatus::Failed, None));
        assert_eq!(p.status, "failed");
        assert!(p.result.is_none());
    }

    fn record(event_id: i64, event: Event, child_run_id: Option<String>) -> HistoryRecord {
        HistoryRecord {
            event_id,
            ts: 1_700_000_000_000,
            event,
            child_run_id,
        }
    }

    #[test]
    fn event_to_json_humanizes_activity_scheduled() {
        let dto = event_to_json(record(
            2,
            Event::ActivityScheduled {
                seq: 0,
                activity_type: "Parse".into(),
                input: serde_json::to_vec(&json!({ "text": "1 2 3" })).unwrap(),
                retry: workflow::RetryPolicy::none(),
            },
            None,
        ));
        assert_eq!(dto.kind, "ActivityScheduled");
        assert_eq!(dto.event_id, 2);
        assert_eq!(dto.ts, 1_700_000_000_000);
        assert_eq!(dto.detail["activity_type"], "Parse");
        assert_eq!(dto.detail["input"], json!({ "text": "1 2 3" }));
        assert!(dto.child_run_id.is_none());
    }

    #[test]
    fn event_to_json_passes_child_run_id_for_child_events() {
        let dto = event_to_json(record(
            3,
            Event::ChildScheduled {
                seq: 0,
                workflow_type: "SumChild".into(),
                input: b"[1,2]".to_vec(),
            },
            Some("child-run".into()),
        ));
        assert_eq!(dto.kind, "ChildScheduled");
        assert_eq!(dto.detail["workflow_type"], "SumChild");
        assert_eq!(dto.child_run_id.as_deref(), Some("child-run"));
    }

    #[test]
    fn event_to_json_decodes_activity_failure_error() {
        let dto = event_to_json(record(
            4,
            Event::ActivityFailed {
                seq: 0,
                error: activity::Error::fatal("could not parse 'two' as an integer"),
            },
            None,
        ));
        assert_eq!(dto.kind, "ActivityFailed");
        assert!(dto.detail["error"]["message"]
            .as_str()
            .unwrap()
            .contains("two"));
    }

    #[test]
    fn event_to_json_humanizes_child_completed_result() {
        let dto = event_to_json(record(
            5,
            Event::ChildCompleted {
                seq: 0,
                result: ChildResult::Completed(
                    serde_json::to_vec(&json!({ "total": 6 })).unwrap(),
                ),
            },
            None,
        ));
        assert_eq!(dto.kind, "ChildCompleted");
        assert_eq!(dto.detail["result"]["status"], "completed");
        assert_eq!(dto.detail["result"]["output"], json!({ "total": 6 }));
    }

    #[test]
    fn decode_falls_back_to_null_for_non_json() {
        assert_eq!(decode(b"not-json"), serde_json::Value::Null);
    }

    #[test]
    fn message_dto_maps_service_fields() {
        let dto = message_dto(chat_service::ChatMessage {
            conversation_id: "c1".into(),
            message_id: "m1".into(),
            role: "user".into(),
            content: "hello".into(),
            status: "complete".into(),
            seq: 1,
            created_at: 0,
        });
        assert_eq!(dto.message_id, "m1");
        assert_eq!(dto.role, "user");
        assert_eq!(dto.content, "hello");
        assert_eq!(dto.status, "complete");
        assert_eq!(dto.seq, 1);
    }
}
