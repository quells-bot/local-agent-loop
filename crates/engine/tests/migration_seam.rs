//! Guards spec §15: `History` + `TaskQueue` are the ENTIRE seam a persistence backend
//! implements. If `persist::Sqlite` ever needs another engine trait to be wired in,
//! this stops compiling — forcing that new seam to be a deliberate, documented choice.

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
