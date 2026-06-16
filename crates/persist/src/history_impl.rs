// Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec:
// docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
use rusqlite::{params, OptionalExtension};

use engine::{
    CreateOutcome, ExecStatus, ExecutionSummary, History, HistoryRecord, RunMeta, SignalOutcome,
    StoredEvent, TurnCommit,
};
use workflow::Event;

use crate::sqlite::{now_ms, Sqlite};

/// Encode an event to (seq, kind, payload-bytes) for a history row.
fn encode(event: &Event) -> (Option<i64>, &'static str, Vec<u8>) {
    let seq = match event {
        Event::ActivityScheduled { seq, .. }
        | Event::ActivityCompleted { seq, .. }
        | Event::ActivityFailed { seq, .. }
        | Event::TimerStarted { seq, .. }
        | Event::TimerFired { seq }
        | Event::ChildScheduled { seq, .. }
        | Event::ChildCompleted { seq, .. } => Some(*seq as i64),
        Event::WorkflowStarted { .. } | Event::SignalReceived { .. } | Event::Patched { .. } => {
            None
        }
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
                "SELECT workflow_id, workflow_type, status, parent_run_id, parent_seq \
                 FROM executions WHERE run_id = ?1",
                params![run_id],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<i64>>(4)?,
                    ))
                },
            )
            .optional()?;
        Ok(row.map(
            |(workflow_id, workflow_type, status, parent_run_id, parent_seq)| RunMeta {
                run_id: run_id.to_string(),
                workflow_id,
                workflow_type,
                status: ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
                parent_run_id,
                parent_seq,
            },
        ))
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

        for child in &commit.new_children {
            tx.execute(
                "INSERT INTO executions \
                 (run_id, workflow_id, workflow_type, parent_run_id, parent_seq, input, status) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running')",
                params![
                    child.child_run_id,
                    child.child_workflow_id,
                    child.workflow_type,
                    run_id, // parent's run_id
                    child.seq,
                    child.input
                ],
            )?;
            let (cseq, ckind, cpayload) = encode(&Event::WorkflowStarted {
                input: child.input.clone(),
            });
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, 1, ?2, ?3, ?4, ?5)",
                params![child.child_run_id, cseq, ckind, cpayload, now_ms()],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
                params![child.child_run_id, now_ms()],
            )?;
        }

        if let Some(notify) = &commit.parent_notify {
            let (pseq, pkind, ppayload) = encode(&notify.event);
            let pid = next_event_id(&tx, &notify.parent_run_id)?;
            tx.execute(
                "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![notify.parent_run_id, pid, pseq, pkind, ppayload, now_ms()],
            )?;
            tx.execute(
                "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
                params![notify.parent_run_id, now_ms()],
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

    async fn list_executions(&self) -> anyhow::Result<Vec<ExecutionSummary>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.run_id, e.workflow_id, e.workflow_type, e.status, \
                    MIN(h.ts), MAX(h.ts), COUNT(h.event_id) \
             FROM executions e JOIN history h ON h.run_id = e.run_id \
             WHERE e.parent_run_id IS NULL \
             GROUP BY e.run_id \
             ORDER BY MIN(h.ts) DESC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
            ))
        })?;
        let mut out = Vec::new();
        for row in rows {
            let (run_id, workflow_id, workflow_type, status, started_at, last_event_at, event_count) =
                row?;
            out.push(ExecutionSummary {
                run_id,
                workflow_id,
                workflow_type,
                status: ExecStatus::from_str(&status).unwrap_or(ExecStatus::Running),
                started_at,
                last_event_at,
                event_count,
            });
        }
        Ok(out)
    }

    async fn read_events(&self, run_id: &str) -> anyhow::Result<Vec<HistoryRecord>> {
        let conn = self.conn.lock().unwrap();
        // Collect raw rows first, dropping the prepared statement before the
        // per-child lookups below.
        let raw: Vec<(i64, Vec<u8>, i64)> = {
            let mut stmt = conn.prepare(
                "SELECT event_id, payload, ts FROM history WHERE run_id = ?1 ORDER BY event_id",
            )?;
            let rows = stmt.query_map(params![run_id], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?, r.get::<_, i64>(2)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut out = Vec::with_capacity(raw.len());
        for (event_id, payload, ts) in raw {
            let event: Event = serde_json::from_slice(&payload)?;
            let child_run_id = match &event {
                Event::ChildScheduled { seq, .. } | Event::ChildCompleted { seq, .. } => conn
                    .query_row(
                        "SELECT run_id FROM executions \
                         WHERE parent_run_id = ?1 AND parent_seq = ?2",
                        params![run_id, *seq as i64],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?,
                _ => None,
            };
            out.push(HistoryRecord {
                event_id,
                ts,
                event,
                child_run_id,
            });
        }
        Ok(out)
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
    use engine::{ExecStatus, NewActivityTask, NewChild, SignalOutcome};
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
            new_children: vec![],
            parent_notify: None,
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
            new_children: vec![],
            parent_notify: None,
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
            new_children: vec![],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &idle).await.unwrap();
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
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
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            Some("run-1".into())
        );
    }

    #[tokio::test]
    async fn commit_turn_creates_child_execution_and_marks_it_runnable() {
        use engine::NewChild;
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("parent-run", "parent-wf", "Parent", b"in")
            .await
            .unwrap();

        let commit = TurnCommit {
            events: vec![Event::ChildScheduled {
                seq: 0,
                workflow_type: "Child".into(),
                input: b"5".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![NewChild {
                seq: 0,
                child_run_id: "child-run".into(),
                child_workflow_id: "parent-wf::child::0".into(),
                workflow_type: "Child".into(),
                input: b"5".to_vec(),
            }],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("parent-run", &commit).await.unwrap();

        // The child execution exists, is running, and links back to the parent.
        let meta = db.load_run("child-run").await.unwrap().unwrap();
        assert_eq!(meta.workflow_type, "Child");
        assert_eq!(meta.status, ExecStatus::Running);
        assert_eq!(meta.parent_run_id.as_deref(), Some("parent-run"));
        assert_eq!(meta.parent_seq, Some(0));

        // The child has a WorkflowStarted event and is runnable.
        let h = db.read_history("child-run").await.unwrap();
        assert_eq!(h.len(), 1);
        assert!(matches!(h[0].event, Event::WorkflowStarted { .. }));
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            Some("child-run".into())
        );
    }

    #[tokio::test]
    async fn commit_turn_notifies_parent_with_child_completed() {
        use engine::ParentNotify;
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("parent-run", "parent-wf", "Parent", b"in")
            .await
            .unwrap();
        db.create_execution("child-run", "child-wf", "Child", b"5")
            .await
            .unwrap();
        // Drive both runnable flags away so we can observe the notify re-arm one.
        let idle = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("parent-run", &idle).await.unwrap();

        // The child's terminal turn: complete the child AND notify the parent.
        let commit = TurnCommit {
            events: vec![],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![],
            parent_notify: Some(ParentNotify {
                parent_run_id: "parent-run".into(),
                event: Event::ChildCompleted {
                    seq: 0,
                    result: workflow::ChildResult::Completed(b"10".to_vec()),
                },
            }),
            status: ExecStatus::Completed,
            result: Some(b"10".to_vec()),
        };
        db.commit_turn("child-run", &commit).await.unwrap();

        // ChildCompleted landed in the PARENT's history (with the parent's seq).
        let h = db.read_history("parent-run").await.unwrap();
        match &h.last().unwrap().event {
            Event::ChildCompleted { seq: 0, result } => {
                assert_eq!(*result, workflow::ChildResult::Completed(b"10".to_vec()));
            }
            other => panic!("expected ChildCompleted, got {other:?}"),
        }
        // The parent is runnable again; the child is not.
        assert_eq!(
            <Sqlite as engine::TaskQueue>::next_runnable(&db)
                .await
                .unwrap(),
            Some("parent-run".into())
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
            new_children: vec![],
            parent_notify: None,
            status: ExecStatus::Completed,
            result: Some(b"\"done\"".to_vec()),
        };
        db.commit_turn("run-1", &done).await.unwrap();

        let out = db.append_signal("wf-A", "approve", b"true").await.unwrap();
        assert_eq!(out, SignalOutcome::NotRunning);
        // No SignalReceived was appended (last event is still WorkflowStarted).
        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(
            h.last().unwrap().event,
            Event::WorkflowStarted { .. }
        ));
    }

    #[tokio::test]
    async fn read_events_carries_ts_in_order_without_child_ids() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-1", "Parent", b"in")
            .await
            .unwrap();
        let commit = TurnCommit {
            events: vec![Event::ActivityScheduled {
                seq: 0,
                activity_type: "Parse".into(),
                input: b"\"1 2\"".to_vec(),
                retry: RetryPolicy::none(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();

        let evs = db.read_events("run-1").await.unwrap();
        assert_eq!(evs.len(), 2);
        assert_eq!(evs[0].event_id, 1);
        assert!(matches!(evs[0].event, Event::WorkflowStarted { .. }));
        assert_eq!(evs[1].event_id, 2);
        assert!(matches!(evs[1].event, Event::ActivityScheduled { seq: 0, .. }));
        assert!(evs[0].ts > 0 && evs[1].ts >= evs[0].ts);
        assert!(evs.iter().all(|e| e.child_run_id.is_none()));
    }

    #[tokio::test]
    async fn read_events_resolves_child_run_id_for_child_events() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("parent-run", "parent-wf", "Parent", b"in")
            .await
            .unwrap();
        let commit = TurnCommit {
            events: vec![Event::ChildScheduled {
                seq: 0,
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![NewChild {
                seq: 0,
                child_run_id: "child-run".into(),
                child_workflow_id: "parent-wf::child::0".into(),
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("parent-run", &commit).await.unwrap();

        let evs = db.read_events("parent-run").await.unwrap();
        let child = evs
            .iter()
            .find(|e| matches!(e.event, Event::ChildScheduled { .. }))
            .unwrap();
        assert_eq!(child.child_run_id.as_deref(), Some("child-run"));
    }

    #[tokio::test]
    async fn read_events_empty_for_unknown_run() {
        let db = Sqlite::open_in_memory().unwrap();
        let evs = db.read_events("nope").await.unwrap();
        assert!(evs.is_empty());
    }

    #[tokio::test]
    async fn list_executions_returns_roots_only() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-A", "wf-A", "Parent", b"in-A")
            .await
            .unwrap();
        db.create_execution("run-B", "wf-B", "Parent", b"in-B")
            .await
            .unwrap();
        // run-B spawns a child; the child must NOT appear in the list.
        let commit = TurnCommit {
            events: vec![Event::ChildScheduled {
                seq: 0,
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            new_tasks: vec![],
            new_timers: vec![],
            new_children: vec![NewChild {
                seq: 0,
                child_run_id: "child-run".into(),
                child_workflow_id: "wf-B::child::0".into(),
                workflow_type: "SumChild".into(),
                input: b"[1]".to_vec(),
            }],
            parent_notify: None,
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-B", &commit).await.unwrap();

        let runs = db.list_executions().await.unwrap();
        let ids: Vec<&str> = runs.iter().map(|r| r.run_id.as_str()).collect();
        assert!(ids.contains(&"run-A"));
        assert!(ids.contains(&"run-B"));
        assert!(!ids.contains(&"child-run"), "children are not roots");

        // newest-first: sorted by started_at descending.
        assert!(runs.windows(2).all(|w| w[0].started_at >= w[1].started_at));

        let b = runs.iter().find(|r| r.run_id == "run-B").unwrap();
        assert_eq!(b.workflow_type, "Parent");
        assert_eq!(b.status, ExecStatus::Running);
        assert_eq!(b.event_count, 2); // WorkflowStarted + ChildScheduled
        assert!(b.started_at <= b.last_event_at);
    }
}
