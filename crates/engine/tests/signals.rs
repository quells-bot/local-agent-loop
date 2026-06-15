use std::sync::Arc;
use std::time::Duration;

use engine::{Engine, ExecStatus, History, SignalError, StartOptions};
use futures::{select_biased, FutureExt};
use persist::Sqlite;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Approval {
    ok: bool,
}

// Workflow that blocks on a single "approve" signal and returns its `ok` flag.
struct WaitApprove;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for WaitApprove {
    type Input = ();
    type Output = bool;
    const TYPE: &'static str = "WaitApprove";
    async fn run(ctx: workflow::Context, _i: ()) -> Result<bool, workflow::Error> {
        let approvals = ctx.signal_channel::<Approval>("approve");
        let a = approvals.recv().await?;
        Ok(a.ok)
    }
}

// Signal-or-timeout: race a "approve" signal against a sleep whose duration is the
// workflow input (ms). `select_biased!` is the deterministic Selector analog (§6.3).
struct SignalOrTimeout;
#[async_trait::async_trait(?Send)]
impl workflow::Definition for SignalOrTimeout {
    type Input = u64; // timeout in ms
    type Output = String;
    const TYPE: &'static str = "SignalOrTimeout";
    async fn run(ctx: workflow::Context, timeout_ms: u64) -> Result<String, workflow::Error> {
        let approvals = ctx.signal_channel::<Approval>("approve");
        let recv = approvals.recv().fuse();
        let nap = ctx.sleep(Duration::from_millis(timeout_ms)).fuse();
        futures::pin_mut!(recv, nap);
        let out = select_biased! {
            a = recv => {
                let a = a?;
                if a.ok { "approved" } else { "rejected" }
            }
            _ = nap => "timed_out",
        };
        Ok(out.to_string())
    }
}

fn build<W: workflow::Definition>(db: &Sqlite) -> Engine {
    let h: Arc<dyn History> = Arc::new(db.clone());
    let q: Arc<dyn engine::TaskQueue> = Arc::new(db.clone());
    let mut e = Engine::new(h, q);
    e.register_workflow::<W>();
    e
}

/// Pump driver + worker + timer turns until quiescent (deterministic).
async fn pump(engine: &Engine) -> anyhow::Result<()> {
    loop {
        let drove = engine.process_one_runnable().await?;
        let worked = engine.process_one_activity().await?;
        let timed = engine.process_one_timer().await?;
        if !drove && !worked && !timed {
            return Ok(());
        }
    }
}

fn approval(ok: bool) -> Vec<u8> {
    serde_json::to_vec(&Approval { ok }).unwrap()
}

#[tokio::test]
async fn workflow_blocked_on_recv_resumes_when_signaled() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<WaitApprove>(&db);
    let handle = engine
        .start_workflow::<WaitApprove>((), StartOptions { id: "sig-1".into() })
        .await
        .unwrap();

    // Drive until it blocks on recv() — no further progress, still running.
    pump(&engine).await.unwrap();
    let (_, status, _) = db.find_execution("sig-1").await.unwrap().unwrap();
    assert_eq!(
        status,
        ExecStatus::Running,
        "blocked on recv(), not yet complete"
    );

    // Deliver the signal; it should resume and complete.
    engine
        .signal_workflow("sig-1", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine).await.unwrap();

    let out: bool = handle.result().await.unwrap();
    assert!(out, "the delivered Approval{{ ok: true }} resumes recv()");
}

#[tokio::test]
async fn signal_or_timeout_takes_the_signal() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<SignalOrTimeout>(&db);
    // A day-long timeout: the timer never fires, so the signal branch must win.
    let handle = engine
        .start_workflow::<SignalOrTimeout>(86_400_000, StartOptions { id: "sot-1".into() })
        .await
        .unwrap();

    assert!(engine.process_one_runnable().await.unwrap()); // turn 1: schedules the timer, blocks on recv
    engine
        .signal_workflow("sot-1", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine).await.unwrap();

    let out: String = handle.result().await.unwrap();
    assert_eq!(
        out, "approved",
        "the signal wins; the day-long timer never fires"
    );
}

#[tokio::test]
async fn signal_or_timeout_times_out() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<SignalOrTimeout>(&db);
    // A 0ms timeout is due immediately; with no signal delivered, the timer wins.
    let handle = engine
        .start_workflow::<SignalOrTimeout>(0, StartOptions { id: "sot-2".into() })
        .await
        .unwrap();

    pump(&engine).await.unwrap();

    let out: String = handle.result().await.unwrap();
    assert_eq!(
        out, "timed_out",
        "no signal arrives; the immediate timer wins"
    );
}

#[tokio::test]
async fn signal_before_crash_replays_to_completion() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn (recv pends), deliver the signal durably, crash.
    {
        let engine = build::<WaitApprove>(&db);
        engine
            .start_workflow::<WaitApprove>((), StartOptions { id: "sig-3".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // turn 1: recv pending, run goes idle
        engine
            .signal_workflow("sig-3", "approve", &approval(true))
            .await
            .unwrap();
        // engine dropped here; only the shared `db` survives. The SignalReceived row
        // is durable, but the driver has not yet consumed it.
    }

    // Phase 2: a fresh engine cold-replays [WorkflowStarted, SignalReceived] and
    // completes — the signal resolves recv() identically on replay (Invariant 10).
    let engine2 = build::<WaitApprove>(&db);
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("sig-3").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: bool = serde_json::from_slice(&result.unwrap()).unwrap();
    assert!(out);
}

#[tokio::test]
async fn signal_after_crash_delivers_to_recovered_run() {
    let db = Sqlite::open_in_memory().unwrap();

    // Phase 1: start, drive one turn (recv pends, run goes idle), then crash — WITHOUT
    // signaling. Only the durable `running` execution row survives.
    {
        let engine = build::<WaitApprove>(&db);
        engine
            .start_workflow::<WaitApprove>((), StartOptions { id: "sig-5".into() })
            .await
            .unwrap();
        assert!(engine.process_one_runnable().await.unwrap()); // turn 1: recv pending, idle
                                                               // engine dropped here; the run is blocked on recv() and durably `running`.
    }
    let (_, status, _) = db.find_execution("sig-5").await.unwrap().unwrap();
    assert_eq!(
        status,
        ExecStatus::Running,
        "recovered run is still blocked on recv()"
    );

    // Phase 2: a FRESH engine delivers the signal against the recovered run. This is
    // the "after a crash" half of the §13 gate: `append_signal` finds the `running`
    // row a previous engine instance wrote, re-arms `runnable`, and the new engine's
    // driver picks it up and replays [WorkflowStarted, SignalReceived] to completion.
    let engine2 = build::<WaitApprove>(&db);
    engine2
        .signal_workflow("sig-5", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine2).await.unwrap();

    let (_, status, result) = db.find_execution("sig-5").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);
    let out: bool = serde_json::from_slice(&result.unwrap()).unwrap();
    assert!(
        out,
        "the signal delivered to the recovered run resumes recv()"
    );
}

#[tokio::test]
async fn signaling_completed_or_unknown_run_errors() {
    let db = Sqlite::open_in_memory().unwrap();
    let engine = build::<WaitApprove>(&db);
    engine
        .start_workflow::<WaitApprove>((), StartOptions { id: "sig-4".into() })
        .await
        .unwrap();
    pump(&engine).await.unwrap(); // blocks on recv()

    // First signal completes it.
    engine
        .signal_workflow("sig-4", "approve", &approval(true))
        .await
        .unwrap();
    pump(&engine).await.unwrap();
    let (_, status, _) = db.find_execution("sig-4").await.unwrap().unwrap();
    assert_eq!(status, ExecStatus::Completed);

    // Signaling the now-completed run errors NotRunning (Temporal-faithful, §6.1).
    let err = engine
        .signal_workflow("sig-4", "approve", &approval(true))
        .await
        .unwrap_err();
    assert!(matches!(err, SignalError::NotRunning), "got {err:?}");

    // Signaling an unknown id errors WorkflowNotFound.
    let err = engine
        .signal_workflow("does-not-exist", "approve", &approval(true))
        .await
        .unwrap_err();
    assert!(matches!(err, SignalError::WorkflowNotFound), "got {err:?}");
}
