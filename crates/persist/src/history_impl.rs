use rusqlite::{params, OptionalExtension};

use engine::{CreateOutcome, ExecStatus, History, RunMeta, SignalOutcome, StoredEvent, TurnCommit};
use workflow::Event;

use crate::sqlite::{now_ms, Sqlite};

/// Encode an event to (seq, kind, payload-bytes) for a history row.
fn encode(event: &Event) -> (Option<i64>, &'static str, Vec<u8>) {
    let seq = match event {
        Event::ActivityScheduled { seq, .. }
        | Event::ActivityCompleted { seq, .. }
        | Event::ActivityFailed { seq, .. }
        | Event::TimerStarted { seq, .. }
        | Event::TimerFired { seq } => Some(*seq as i64),
        Event::WorkflowStarted { .. } | Event::SignalReceived { .. } => None,
    };
    let payload = serde_json::to_vec(event).expect("event serializes");
    (seq, event.kind(), payload)
}

fn next_event_id(tx: &rusqlite::Transaction, run_id: &str) -> rusqlite::Result<i64> {
    tx.query_row(
        "SELECT COALESCE(MAX(event_id), 0) + 1 FROM history WHERE run_id = ?1",
        params![run_id],
        |r| r.get(0),
    )
}

#[async_trait::async_trait]
impl History for Sqlite {
    async fn create_execution(
        &self,
        candidate_run_id: &str,
        workflow_id: &str,
        workflow_type: &str,
        input: &[u8],
    ) -> anyhow::Result<(CreateOutcome, String)> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let inserted = tx.execute(
            "INSERT OR IGNORE INTO executions \
             (run_id, workflow_id, workflow_type, input, status) \
             VALUES (?1, ?2, ?3, ?4, 'running')",
            params![candidate_run_id, workflow_id, workflow_type, input],
        )?;

        if inserted == 1 {
            let (seq, kind, payload) = encode(&Event::WorkflowStarted {
                input: input.to_vec(),
            });
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, 1, ?2, ?3, ?4, ?5)",
                params![candidate_run_id, seq, kind, payload, now_ms()],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
                params![candidate_run_id, now_ms()],
            )?;
            tx.commit()?;
            Ok((CreateOutcome::Created, candidate_run_id.to_string()))
        } else {
            let existing: String = tx.query_row(
                "SELECT run_id FROM executions WHERE workflow_id = ?1",
                params![workflow_id],
                |r| r.get(0),
            )?;
            tx.commit()?;
            Ok((CreateOutcome::AlreadyExists, existing))
        }
    }

    async fn read_history(&self, run_id: &str) -> anyhow::Result<Vec<StoredEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT event_id, payload FROM history WHERE run_id = ?1 ORDER BY event_id")?;
        let rows = stmt.query_map(params![run_id], |r| {
            let event_id: i64 = r.get(0)?;
            let payload: Vec<u8> = r.get(1)?;
            Ok((event_id, payload))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (event_id, payload) = row?;
            let event: Event = serde_json::from_slice(&payload)?;
            out.push(StoredEvent { event_id, event });
        }
        Ok(out)
    }

    async fn load_run(&self, run_id: &str) -> anyhow::Result<Option<RunMeta>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT workflow_id, workflow_type, status FROM executions WHERE run_id = ?1",
                params![run_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(|(workflow_id, workflow_type, status)| RunMeta {
            run_id: run_id.to_string(),
            workflow_id,
            workflow_type,
            status: ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
        }))
    }

    async fn commit_turn(&self, run_id: &str, commit: &TurnCommit) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let base_event_id = next_event_id(&tx, run_id)?;
        for (offset, event) in commit.events.iter().enumerate() {
            let event_id = base_event_id + offset as i64;
            let (seq, kind, payload) = encode(event);
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![run_id, event_id, seq, kind, payload, now_ms()],
            )?;
        }

        for task in &commit.new_tasks {
            tx.execute(
                "INSERT OR REPLACE INTO activity_tasks \
                 (run_id, seq, activity_type, input, attempt, next_run_at, status) \
                 VALUES (?1, ?2, ?3, ?4, 0, ?5, 'pending')",
                params![
                    run_id,
                    task.seq,
                    task.activity_type,
                    task.input,
                    task.next_run_at
                ],
            )?;
        }

        for timer in &commit.new_timers {
            tx.execute(
                "INSERT OR REPLACE INTO timers (run_id, seq, fire_at) VALUES (?1, ?2, ?3)",
                params![run_id, timer.seq, timer.fire_at],
            )?;
        }

        tx.execute(
            "UPDATE executions SET status = ?2, result = ?3 WHERE run_id = ?1",
            params![run_id, commit.status.as_str(), commit.result],
        )?;
        tx.execute("DELETE FROM runnable WHERE run_id = ?1", params![run_id])?;

        tx.commit()?;
        Ok(())
    }

    async fn find_execution(
        &self,
        workflow_id: &str,
    ) -> anyhow::Result<Option<(String, ExecStatus, Option<Vec<u8>>)>> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT run_id, status, result FROM executions WHERE workflow_id = ?1",
                params![workflow_id],
                |r| {
                    let run_id: String = r.get(0)?;
                    let status: String = r.get(1)?;
                    let result: Option<Vec<u8>> = r.get(2)?;
                    Ok((run_id, status, result))
                },
            )
            .optional()?;
        Ok(row.map(|(run_id, status, result)| {
            (
                run_id,
                ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
                result,
            )
        }))
    }

    async fn append_signal(
        &self,
        workflow_id: &str,
        name: &str,
        payload: &[u8],
    ) -> anyhow::Result<SignalOutcome> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        // Resolve the run + status under the same transaction as the append, so the
        // status check and the write are atomic (spec §6.1).
        let row: Option<(String, String)> = tx
            .query_row(
                "SELECT run_id, status FROM executions WHERE workflow_id = ?1",
                params![workflow_id],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;

        let Some((run_id, status)) = row else {
            tx.commit()?;
            return Ok(SignalOutcome::WorkflowNotFound);
        };
        if ExecStatus::from_str(&status) != Some(ExecStatus::Running) {
            tx.commit()?;
            return Ok(SignalOutcome::NotRunning);
        }

        // Append SignalReceived (inbound → seq NULL) and re-arm the runnable queue.
        let event = Event::SignalReceived {
            name: name.to_string(),
            payload: payload.to_vec(),
        };
        let payload_bytes = serde_json::to_vec(&event)?;
        let next_id = next_event_id(&tx, &run_id)?;
        tx.execute(
            "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, NULL, ?3, ?4, ?5)",
            params![run_id, next_id, event.kind(), payload_bytes, now_ms()],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
            params![run_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(SignalOutcome::Delivered)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{ExecStatus, NewActivityTask, SignalOutcome};
    use workflow::RetryPolicy;

    #[tokio::test]
    async fn create_is_idempotent_by_workflow_id() {
        let db = Sqlite::open_in_memory().unwrap();
        let (o1, r1) = db
            .create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        let (o2, r2) = db
            .create_execution("run-2", "wf-A", "T", b"in")
            .await
            .unwrap();
        assert_eq!(o1, CreateOutcome::Created);
        assert_eq!(o2, CreateOutcome::AlreadyExists);
        assert_eq!(r1, "run-1");
        assert_eq!(r2, "run-1");
    }

    #[tokio::test]
    async fn create_writes_workflow_started_and_runnable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        let h = db.read_history("run-1").await.unwrap();
        assert_eq!(h.len(), 1);
        assert!(matches!(h[0].event, Event::WorkflowStarted { .. }));
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            Some("run-1".into())
        );
    }

    #[tokio::test]
    async fn commit_turn_appends_clears_runnable_and_sets_status() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();

        let commit = TurnCommit {
            events: vec![Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: b"[1,2]".to_vec(),
                retry: RetryPolicy::none(),
            }],
            new_tasks: vec![NewActivityTask {
                seq: 0,
                activity_type: "Add".into(),
                input: b"[1,2]".to_vec(),
                next_run_at: 0,
            }],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();

        let h = db.read_history("run-1").await.unwrap();
        assert_eq!(h.len(), 2);
        assert!(matches!(
            h[1].event,
            Event::ActivityScheduled { seq: 0, .. }
        ));
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn commit_turn_round_trips_signal_received_with_null_seq() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();

        let commit = TurnCommit {
            events: vec![Event::SignalReceived {
                name: "approve".into(),
                payload: b"true".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();

        let h = db.read_history("run-1").await.unwrap();
        match &h.last().unwrap().event {
            Event::SignalReceived { name, payload } => {
                assert_eq!(name, "approve");
                assert_eq!(payload, b"true");
            }
            other => panic!("expected SignalReceived, got {other:?}"),
        }

        // Inbound events carry no seq: the history.seq column must be NULL.
        let seq: Option<i64> = db
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT seq FROM history WHERE run_id = 'run-1' ORDER BY event_id DESC LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(seq, None, "inbound events carry no seq (spec §6, §11)");
    }

    #[tokio::test]
    async fn append_signal_delivers_to_running_and_marks_runnable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        // Drive the create's runnable flag away first (simulate the run going idle).
        let idle = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &idle).await.unwrap();
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db).await.unwrap(),
            None
        );

        let out = db.append_signal("wf-A", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::Delivered);

        // The SignalReceived event is appended with NULL seq, and the run is runnable.
        let h = db.read_history("run-1").await.unwrap();
        match &h.last().unwrap().event {
            Event::SignalReceived { name, payload } => {
                assert_eq!(name, "approve");
                assert_eq!(payload, b"true");
            }
            other => panic!("expected SignalReceived, got {other:?}"),
        }
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db).await.unwrap(),
            Some("run-1".into())
        );
    }

    #[tokio::test]
    async fn append_signal_unknown_id_is_not_found() {
        let db = Sqlite::open_in_memory().unwrap();
        let out = db.append_signal("nope", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::WorkflowNotFound);
    }

    #[tokio::test]
    async fn append_signal_to_terminal_run_is_not_running() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        // Mark the run completed.
        let done = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            status: ExecStatus::Completed,
            result: Some(b"\"done\"".to_vec()),
        };
        db.commit_turn("run-1", &done).await.unwrap();

        let out = db.append_signal("wf-A", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::NotRunning);
        // No SignalReceived was appended (last event is still WorkflowStarted).
        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(h.last().unwrap().event, Event::WorkflowStarted { .. }));
    }
}
