//! The locks this engine takes keep their meaning on TiDB.
//!
//! TiDB accepts `LOCK IN SHARE MODE` and `FOR SHARE` without taking a lock
//! (they are no-ops unless `tidb_enable_noop_functions` is on, and refused
//! otherwise), and its `FOR UPDATE` locks only in pessimistic transactions.
//! So exclusion on TiDB rests on two things this file checks:
//!
//! - the engine takes no shared lock at all, only `FOR UPDATE`;
//! - every session it opens runs READ COMMITTED and, on TiDB, pessimistic.
//!
//! The second needs a live server (MySQL or TiDB), like the differential:
//!
//!   TEST_MYSQL_URL=mysql://root@127.0.0.1:4000/test \
//!     cargo test -p resonate-server-mysql --test locks
//!
//! Without `TEST_MYSQL_URL` it is skipped.

use resonate_server_mysql::MysqlEngine;

/// No statement of the engine asks for a shared lock, whose loss on TiDB
/// would silently drop exclusion. Every lock is `FOR UPDATE`.
#[test]
fn the_engine_takes_no_shared_lock() {
    let source = include_str!("../src/lib.rs").to_ascii_uppercase();
    for shared in ["LOCK IN SHARE MODE", "FOR SHARE"] {
        assert!(
            !source.contains(shared),
            "the MySQL engine uses `{shared}`, which TiDB does not honour"
        );
    }
    assert!(
        source.contains("FOR UPDATE"),
        "the engine locks rows with FOR UPDATE"
    );
}

#[tokio::test]
async fn sessions_are_read_committed_and_pessimistic_on_tidb() {
    let Ok(url) = std::env::var("TEST_MYSQL_URL") else {
        eprintln!("TEST_MYSQL_URL not set — skipped");
        return;
    };
    let engine = MysqlEngine::connect(&url, 2, 30_000, 10, false)
        .await
        .expect("connect");
    for _ in 0..2 {
        let (isolation, mode) = engine.session_probe().await.expect("probe");
        assert_eq!(isolation, "READ-COMMITTED");
        if let Some(mode) = mode {
            assert_eq!(mode, "pessimistic", "TiDB sessions must be pessimistic");
        }
    }
}
