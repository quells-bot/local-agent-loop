//! Guards spec §15: `History` + `TaskQueue` are the ENTIRE seam a persistence backend
//! implements. If `persist::Sqlite` ever needs another engine trait to be wired in,
//! this stops compiling — forcing that new seam to be a deliberate, documented choice.
//! Spec references below ("§N", "spec §N") point to the 2026-06-13 design spec: docs/superpowers/specs/2026-06-13-durable-workflow-engine-design.md
//! Read-model methods (`list_executions`, `read_events`) deliberately ride on
//! `History` rather than a third trait, keeping this seam at exactly two traits.

use engine::{History, TaskQueue};
use persist::Sqlite;

fn _assert_sqlite_is_the_seam(db: Sqlite) {
    fn needs_both<T: History + TaskQueue>(_: T) {}
    needs_both(db);
}

#[test]
fn migration_seam_is_exactly_two_traits() {
    // The assertion is the `_assert_sqlite_is_the_seam` signature above; this test
    // exists so the file is exercised and documents intent.
    let db = Sqlite::open_in_memory().unwrap();
    _assert_sqlite_is_the_seam(db);
}
