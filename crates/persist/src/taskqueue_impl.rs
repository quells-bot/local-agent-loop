use rusqlite::{params, OptionalExtension};

use engine::{ActivityLease, TaskQueue};
use workflow::{CommandResult, Event, RetryPolicy};

use crate::sqlite::{now_ms, Sqlite};

/// How long a leased activity may run before the reclaim sweep considers it dead
/// and returns it to pending. Generous for desktop scale (spec §5.2).
const LEASE_TTL_MS: i64 = 30_000;

#[async_trait::async_trait]
impl TaskQueue for Sqlite {
    async fn next_runnable(&self) -> anyhow::Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let run_id: Option<String> = conn
            .query_row(
                "SELECT run_id FROM runnable ORDER BY since LIMIT 1",
                [],
                |r| r.get(0),
            )
            .optional()?;
        Ok(run_id)
    }

    async fn lease_activity(&self) -> anyhow::Result<Option<ActivityLease>> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = now_ms();

        let row = tx
            .query_row(
                "SELECT t.run_id, t.seq, t.activity_type, t.input, t.attempt, e.workflow_id \
                 FROM activity_tasks t JOIN executions e ON e.run_id = t.run_id \
                 WHERE t.status = 'pending' AND t.next_run_at <= ?1 \
                 ORDER BY t.next_run_at LIMIT 1",
                params![now],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, Vec<u8>>(3)?,
                        r.get::<_, i64>(4)?,
                        r.get::<_, String>(5)?,
                    ))
                },
            )
            .optional()?;

        let Some((run_id, seq, activity_type, input, attempt, workflow_id)) = row else {
            tx.commit()?;
            return Ok(None);
        };

        // Assumes exactly one ActivityScheduled per (run_id, seq) — guaranteed by
        // the driver's deterministic seq allocation. The matching history row is
        // written in the same commit_turn that enqueued this task, so it exists.
        let retry_payload: Vec<u8> = tx.query_row(
            "SELECT payload FROM history WHERE run_id = ?1 AND seq = ?2 AND kind = 'ActivityScheduled'",
            params![run_id, seq],
            |r| r.get(0),
        )?;
        let retry = match serde_json::from_slice::<Event>(&retry_payload)? {
            Event::ActivityScheduled { retry, .. } => retry,
            _ => RetryPolicy::none(),
        };

        let new_attempt = attempt + 1;
        tx.execute(
            "UPDATE activity_tasks SET status = 'running', attempt = ?3, lease_expires_at = ?4 \
             WHERE run_id = ?1 AND seq = ?2",
            params![run_id, seq, new_attempt, now + LEASE_TTL_MS],
        )?;
        tx.commit()?;

        Ok(Some(ActivityLease {
            run_id,
            workflow_id,
            seq,
            activity_type,
            input,
            attempt: new_attempt as u32,
            retry,
        }))
    }

    async fn complete_activity(
        &self,
        lease: &ActivityLease,
        result: CommandResult,
    ) -> anyhow::Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;

        let event = match result {
            CommandResult::ActivityCompleted(output) => Event::ActivityCompleted {
                seq: lease.seq as u64,
                output,
            },
            CommandResult::ActivityFailed(error) => Event::ActivityFailed {
                seq: lease.seq as u64,
                error,
            },
        };
        let payload = serde_json::to_vec(&event)?;
        let next_id: i64 = tx.query_row(
            "SELECT COALESCE(MAX(event_id), 0) + 1 FROM history WHERE run_id = ?1",
            params![lease.run_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                lease.run_id,
                next_id,
                lease.seq,
                event.kind(),
                payload,
                now_ms()
            ],
        )?;
        tx.execute(
            "UPDATE activity_tasks SET status = 'done' WHERE run_id = ?1 AND seq = ?2",
            params![lease.run_id, lease.seq],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
            params![lease.run_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(())
    }

    async fn reschedule_activity(
        &self,
        lease: &ActivityLease,
        next_run_at: i64,
    ) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE activity_tasks SET status = 'pending', next_run_at = ?3, lease_expires_at = NULL \
             WHERE run_id = ?1 AND seq = ?2",
            params![lease.run_id, lease.seq, next_run_at],
        )?;
        Ok(())
    }

    async fn fire_due_timer(&self) -> anyhow::Result<bool> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let now = now_ms();

        let row: Option<(String, i64)> = tx
            .query_row(
                "SELECT run_id, seq FROM timers WHERE fire_at <= ?1 ORDER BY fire_at LIMIT 1",
                params![now],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
            )
            .optional()?;

        let Some((run_id, seq)) = row else {
            tx.commit()?;
            return Ok(false);
        };

        let event = Event::TimerFired { seq: seq as u64 };
        let payload = serde_json::to_vec(&event)?;
        let next_id: i64 = tx.query_row(
            "SELECT COALESCE(MAX(event_id), 0) + 1 FROM history WHERE run_id = ?1",
            params![run_id],
            |r| r.get(0),
        )?;
        tx.execute(
            "INSERT INTO history (run_id, event_id, seq, kind, payload, ts) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![run_id, next_id, seq, event.kind(), payload, now_ms()],
        )?;
        tx.execute(
            "DELETE FROM timers WHERE run_id = ?1 AND seq = ?2",
            params![run_id, seq],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO runnable (run_id, since) VALUES (?1, ?2)",
            params![run_id, now_ms()],
        )?;
        tx.commit()?;
        Ok(true)
    }

    async fn reclaim_expired_activities(&self) -> anyhow::Result<u64> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE activity_tasks SET status = 'pending', lease_expires_at = NULL \
             WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?1",
            params![now_ms()],
        )?;
        Ok(n as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine::{ExecStatus, History, NewActivityTask, TurnCommit};
    use workflow::RetryPolicy;

    async fn db_with_task() -> Sqlite {
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
        db
    }

    #[tokio::test]
    async fn expired_lease_is_reclaimed_and_releasable() {
        let db = db_with_task().await;
        let lease = db.lease_activity().await.unwrap().unwrap();
        assert_eq!(lease.attempt, 1);
        assert!(
            db.lease_activity().await.unwrap().is_none(),
            "a running task is not leasable"
        );

        // Simulate the worker crashing and the lease TTL elapsing.
        db.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE activity_tasks SET lease_expires_at = 0 WHERE run_id = ?1 AND seq = ?2",
                params![lease.run_id, lease.seq],
            )
            .unwrap();

        assert_eq!(db.reclaim_expired_activities().await.unwrap(), 1);
        let released = db
            .lease_activity()
            .await
            .unwrap()
            .expect("reclaimed task is leasable again");
        assert_eq!(
            released.attempt, 2,
            "re-lease after a crash counts as another attempt"
        );
    }

    #[tokio::test]
    async fn live_lease_is_not_reclaimed() {
        let db = db_with_task().await;
        let _lease = db.lease_activity().await.unwrap().unwrap(); // TTL is in the future
        assert_eq!(
            db.reclaim_expired_activities().await.unwrap(),
            0,
            "a live lease must not be reclaimed"
        );
    }

    #[tokio::test]
    async fn lease_then_complete_appends_event_and_makes_runnable() {
        let db = db_with_task().await;

        let lease = db.lease_activity().await.unwrap().expect("a task is due");
        assert_eq!(lease.seq, 0);
        assert_eq!(lease.attempt, 1);
        assert_eq!(lease.workflow_id, "wf-A");
        assert!(db.lease_activity().await.unwrap().is_none());

        db.complete_activity(&lease, CommandResult::ActivityCompleted(b"3".to_vec()))
            .await
            .unwrap();

        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(
            h.last().unwrap().event,
            Event::ActivityCompleted { seq: 0, .. }
        ));
        assert_eq!(db.next_runnable().await.unwrap(), Some("run-1".into()));
    }

    #[tokio::test]
    async fn reschedule_makes_task_leasable_again() {
        let db = db_with_task().await;
        let lease = db.lease_activity().await.unwrap().unwrap();
        db.reschedule_activity(&lease, 0).await.unwrap();
        assert!(db.lease_activity().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn complete_with_failure_appends_activity_failed() {
        let db = db_with_task().await;
        let lease = db.lease_activity().await.unwrap().unwrap();
        db.complete_activity(
            &lease,
            CommandResult::ActivityFailed(activity::Error::fatal("boom")),
        )
        .await
        .unwrap();
        let h = db.read_history("run-1").await.unwrap();
        match &h.last().unwrap().event {
            Event::ActivityFailed { seq: 0, error } => assert_eq!(error.message, "boom"),
            other => panic!("expected ActivityFailed, got {other:?}"),
        }
        // a terminal completion still re-marks the run runnable (driver decides next)
        assert_eq!(db.next_runnable().await.unwrap(), Some("run-1".into()));
    }

    #[tokio::test]
    async fn lease_round_trips_the_scheduled_retry_policy() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        let policy = RetryPolicy::exponential(5);
        let commit = TurnCommit {
            events: vec![Event::ActivityScheduled {
                seq: 0,
                activity_type: "Add".into(),
                input: b"[1,2]".to_vec(),
                retry: policy.clone(),
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

        let lease = db.lease_activity().await.unwrap().unwrap();
        assert_eq!(
            lease.retry, policy,
            "retry policy must round-trip from the ActivityScheduled payload"
        );
    }

    #[tokio::test]
    async fn task_not_due_yet_is_not_leasable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        let future = now_ms() + 60_000; // due in a minute
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
                next_run_at: future,
            }],
            new_timers: vec![],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();
        assert!(
            db.lease_activity().await.unwrap().is_none(),
            "task with future next_run_at must not lease"
        );
    }

    #[tokio::test]
    async fn fire_due_timer_appends_timer_fired_and_makes_runnable() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();

        // Commit a TimerStarted event + a timer row already due (fire_at = 0).
        let commit = TurnCommit {
            events: vec![Event::TimerStarted {
                seq: 0,
                duration_ms: 500,
            }],
            new_tasks: vec![],
            new_timers: vec![engine::NewTimer { seq: 0, fire_at: 0 }],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();
        // commit_turn cleared runnable; a due timer must re-arm it.
        assert_eq!(db.next_runnable().await.unwrap(), None);

        assert!(
            db.fire_due_timer().await.unwrap(),
            "a due timer should fire"
        );
        assert!(
            !db.fire_due_timer().await.unwrap(),
            "timer row consumed -> nothing due"
        );

        let h = db.read_history("run-1").await.unwrap();
        assert!(matches!(
            h.last().unwrap().event,
            Event::TimerFired { seq: 0 }
        ));
        assert_eq!(db.next_runnable().await.unwrap(), Some("run-1".into()));
    }

    #[tokio::test]
    async fn timer_not_due_yet_does_not_fire() {
        let db = Sqlite::open_in_memory().unwrap();
        db.create_execution("run-1", "wf-A", "T", b"in")
            .await
            .unwrap();
        let commit = TurnCommit {
            events: vec![Event::TimerStarted {
                seq: 0,
                duration_ms: 60_000,
            }],
            new_tasks: vec![],
            new_timers: vec![engine::NewTimer {
                seq: 0,
                fire_at: now_ms() + 60_000,
            }],
            status: ExecStatus::Running,
            result: None,
        };
        db.commit_turn("run-1", &commit).await.unwrap();
        assert!(
            !db.fire_due_timer().await.unwrap(),
            "future timer must not fire"
        );
    }
}
