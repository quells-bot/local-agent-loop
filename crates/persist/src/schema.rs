pub const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS executions (
  run_id        TEXT PRIMARY KEY,
  workflow_id   TEXT NOT NULL,
  workflow_type TEXT NOT NULL,
  parent_run_id TEXT,
  parent_seq    INTEGER,
  input         BLOB,
  status        TEXT NOT NULL,
  result        BLOB,
  UNIQUE(workflow_id)
);
CREATE TABLE IF NOT EXISTS history (
  run_id   TEXT NOT NULL,
  event_id INTEGER NOT NULL,
  seq      INTEGER,
  kind     TEXT NOT NULL,
  payload  BLOB,
  ts       INTEGER NOT NULL,
  PRIMARY KEY (run_id, event_id)
);
CREATE TABLE IF NOT EXISTS activity_tasks (
  run_id        TEXT NOT NULL,
  seq           INTEGER NOT NULL,
  activity_type TEXT NOT NULL,
  input         BLOB,
  attempt       INTEGER NOT NULL DEFAULT 0,
  next_run_at   INTEGER NOT NULL,
  status        TEXT NOT NULL,
  PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS timers (
  run_id  TEXT NOT NULL,
  seq     INTEGER NOT NULL,
  fire_at INTEGER NOT NULL,
  PRIMARY KEY (run_id, seq)
);
CREATE TABLE IF NOT EXISTS runnable (
  run_id TEXT PRIMARY KEY,
  since  INTEGER NOT NULL
);
"#;
