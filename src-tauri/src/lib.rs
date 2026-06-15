use std::sync::Arc;

use engine::{Engine, ExecStatus, History, RunCompleted, StartOptions, TaskQueue};
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
            let mut engine = Engine::new(history, queue);
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
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![submit])
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
}
