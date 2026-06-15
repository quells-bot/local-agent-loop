use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;

use crate::schema::SCHEMA;

#[derive(Clone)]
pub struct Sqlite {
    pub(crate) conn: Arc<Mutex<Connection>>,
}

impl Sqlite {
    pub fn open(path: &str) -> anyhow::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    pub fn open_in_memory() -> anyhow::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(SCHEMA)?;
        migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// Test/diagnostic helper: force a leased task's TTL into the past so the next
    /// `reclaim_expired_activities` reclaims it. Not used by the engine in
    /// production.
    pub fn expire_lease_for_test(&self, run_id: &str, seq: i64) -> anyhow::Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE activity_tasks SET lease_expires_at = 0 WHERE run_id = ?1 AND seq = ?2",
            rusqlite::params![run_id, seq],
        )?;
        Ok(())
    }
}

/// Idempotent schema evolution for DBs created before a column existed. SQLite has
/// no `ADD COLUMN IF NOT EXISTS`, so we add and swallow the duplicate-column error.
fn migrate(conn: &Connection) -> rusqlite::Result<()> {
    match conn.execute(
        "ALTER TABLE activity_tasks ADD COLUMN lease_expires_at INTEGER",
        [],
    ) {
        Ok(_) => Ok(()),
        // Fresh DBs already have the column (from SCHEMA); old DBs get it added above.
        Err(rusqlite::Error::SqliteFailure(_, Some(ref msg)))
            if msg.contains("duplicate column name") =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Milliseconds since the Unix epoch. Engine-side wall clock (not subject to
/// workflow determinism — that is `ctx.now()`, deferred).
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_creates_tables() {
        let db = Sqlite::open_in_memory().unwrap();
        let conn = db.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN \
                 ('executions','history','activity_tasks','timers','runnable')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 5);
    }
}
