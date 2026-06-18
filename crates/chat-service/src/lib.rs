//! Mock "chat service" that owns chat history (chat spec). Stands in for a cloud
//! service that would own history in a real deployment; deliberately has NO engine
//! dependencies and owns its own SQLite database, separate from `workflows.db`, to
//! emphasize the boundary.
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::Connection;
use serde::{Deserialize, Serialize};

/// One stored chat message, ordered within a conversation by `seq`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatMessage {
    pub conversation_id: String,
    pub message_id: String,
    pub role: String,    // "user" | "assistant"
    pub content: String,
    pub status: String,  // "complete" | "error"
    pub seq: i64,
    pub created_at: i64,
}

/// Arguments to record one message. `seq` and `created_at` are assigned by the
/// service at write time, so they are not part of the caller's input.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordArgs {
    pub conversation_id: String,
    pub message_id: String,
    pub role: String,
    pub content: String,
    pub status: String,
}

/// `Send + Sync`, cheaply `Clone`able handle to the chat-service DB. A single
/// instance is injected into shared activity instances, so the `!Sync`
/// `rusqlite::Connection` is wrapped in `Arc<Mutex<…>>` (DI design: deps must be
/// `Send + Sync`). All clones share one connection, so all writes serialize.
#[derive(Clone)]
pub struct Client {
    conn: Arc<Mutex<Connection>>,
}

impl Client {
    /// Open (creating if needed) the chat-service DB at `path` and ensure the
    /// schema. Use `":memory:"` in tests.
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS messages (
                conversation_id TEXT NOT NULL,
                message_id      TEXT NOT NULL,
                role            TEXT NOT NULL,
                content         TEXT NOT NULL,
                status          TEXT NOT NULL,
                seq             INTEGER NOT NULL,
                created_at      INTEGER NOT NULL,
                PRIMARY KEY (conversation_id, message_id)
            );",
        )?;
        Ok(Self { conn: Arc::new(Mutex::new(conn)) })
    }

    /// Idempotent insert of one message. The `RecordMessage` activity is
    /// at-least-once, so a repeat (same conversation_id + message_id) is a no-op
    /// via `ON CONFLICT … DO NOTHING`. `seq` is assigned monotonically per
    /// conversation at write time.
    pub fn record_message(&self, args: RecordArgs) -> rusqlite::Result<()> {
        let conn = self.conn.lock().expect("chat-service mutex poisoned");
        let next_seq: i64 = conn.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM messages WHERE conversation_id = ?1",
            [&args.conversation_id],
            |row| row.get(0),
        )?;
        conn.execute(
            "INSERT INTO messages
                (conversation_id, message_id, role, content, status, seq, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(conversation_id, message_id) DO NOTHING",
            rusqlite::params![
                args.conversation_id,
                args.message_id,
                args.role,
                args.content,
                args.status,
                next_seq,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    /// All messages for a conversation, ordered by `seq` (insertion order).
    pub fn list_messages(&self, conversation_id: &str) -> rusqlite::Result<Vec<ChatMessage>> {
        let conn = self.conn.lock().expect("chat-service mutex poisoned");
        let mut stmt = conn.prepare(
            "SELECT conversation_id, message_id, role, content, status, seq, created_at
             FROM messages WHERE conversation_id = ?1 ORDER BY seq",
        )?;
        let rows = stmt.query_map([conversation_id], |row| {
            Ok(ChatMessage {
                conversation_id: row.get(0)?,
                message_id: row.get(1)?,
                role: row.get(2)?,
                content: row.get(3)?,
                status: row.get(4)?,
                seq: row.get(5)?,
                created_at: row.get(6)?,
            })
        })?;
        rows.collect()
    }
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(conv: &str, msg: &str, role: &str, content: &str) -> RecordArgs {
        RecordArgs {
            conversation_id: conv.into(),
            message_id: msg.into(),
            role: role.into(),
            content: content.into(),
            status: "complete".into(),
        }
    }

    #[test]
    fn record_message_is_idempotent_by_id() {
        let c = Client::open(":memory:").unwrap();
        c.record_message(args("c1", "m1", "user", "hello")).unwrap();
        c.record_message(args("c1", "m1", "user", "hello")).unwrap(); // retry
        let msgs = c.list_messages("c1").unwrap();
        assert_eq!(msgs.len(), 1, "the at-least-once retry must not double-write");
    }

    #[test]
    fn list_messages_orders_by_seq() {
        let c = Client::open(":memory:").unwrap();
        c.record_message(args("c1", "m1", "user", "first")).unwrap();
        c.record_message(args("c1", "m1-reply", "assistant", "second")).unwrap();
        c.record_message(args("c1", "m2", "user", "third")).unwrap();
        let seqs: Vec<i64> = c.list_messages("c1").unwrap().iter().map(|m| m.seq).collect();
        assert_eq!(seqs, vec![1, 2, 3], "seq is monotonic per conversation");
        let contents: Vec<String> =
            c.list_messages("c1").unwrap().into_iter().map(|m| m.content).collect();
        assert_eq!(contents, vec!["first", "second", "third"]);
    }

    #[test]
    fn list_messages_scopes_by_conversation() {
        let c = Client::open(":memory:").unwrap();
        c.record_message(args("c1", "m1", "user", "a")).unwrap();
        c.record_message(args("c2", "m1", "user", "b")).unwrap();
        assert_eq!(c.list_messages("c1").unwrap().len(), 1);
        assert_eq!(c.list_messages("c2").unwrap().len(), 1);
    }
}
