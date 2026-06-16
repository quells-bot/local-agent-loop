use std::sync::Arc;

use engine::{
    Engine, ExecStatus, ExecutionSummary, History, HistoryRecord, RunCompleted, StartOptions,
    TaskQueue,
};
use workflow::{ChildResult, Event};
use persist::Sqlite;
use serde::Serialize;
use tauri::{Emitter, Manager, State};

use demo::{Parent, ParentParams, Parse, SumActivity, SumChild};

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

/// Start the parse→sum workflow for `text`, deduped by the frontend-supplied
/// `workflow_id` (spec §7.1). Returns the run_id.
#[tauri::command]
async fn submit(
    text: String,
    workflow_id: String,
    engine: State<'_, Arc<Engine>>,
) -> Result<String, String> {
    engine
        .start_workflow::<Parent>(ParentParams { text }, StartOptions { id: workflow_id })
        .await
        .map(|h| h.run_id().to_string())
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
    Ok(RunDetailDto { summary, events })
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
    events: Vec<EventDto>,
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
            engine.register_workflow::<Parent>();
            engine.register_workflow::<SumChild>();
            engine.register_activity::<Parse>();
            engine.register_activity::<SumActivity>();

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
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![submit, list_runs, run_detail])
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
}
