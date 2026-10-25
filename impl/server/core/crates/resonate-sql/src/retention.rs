//! Retention for the SQL family: the parts of a pass the three engines share.
//!
//! The policy and the rules are `resonate_core::retention`'s. What is here is
//! how a policy becomes SQL — the same predicate in three dialects — and what
//! a pass reports. Each engine owns its own pass: the candidate query, the
//! tombstone insert and the delete are dialect SQL in its own crate, as every
//! other transition is.
//!
//! # The shape of a pass
//!
//! One transaction per pass, bounded by `retention.batch`:
//!
//! 1. select up to a batch of prunable promises, oldest settlement first;
//! 2. write a tombstone for each and delete it (with its listener and
//!    callback rows);
//! 3. delete up to a batch of tombstones past their horizon.
//!
//! A promise is prunable when it is settled — or is an internal promise past
//! its deadline, which is settled in all but the row — longer ago than the
//! shortest age among the rules that select it, nothing waits on it (no
//! listener, no callback, no unconsumed resume) and its task, if it has one,
//! is fulfilled. That is the specification's `prunable`
//! (`spec/spec/02-abstract/retention.lean`), restricted to what a row says.
//!
//! # Why tombstones live in their own table
//!
//! The promise table is read on every request, and every one of the 21
//! operations would have to learn to skip a tombstone row. A table of its own
//! keeps every existing statement as it is: a request that names an id is
//! checked against `promise_tombstones` in the same transaction, after the
//! operation ran, and a hit rolls the operation back and answers `expired`.
//! The table and its indexes are created when an engine is opened with
//! retention configured, so a deployment that never turns retention on has no
//! new schema to migrate to.

use std::sync::atomic::{AtomicI64, Ordering};

use resonate_core::retention::Retention;

/// How often a pass also counts the live tombstones for the gauge, in ms.
///
/// A count is a scan of the tombstone index, which is cheap once a minute and
/// not once per pass.
pub const COUNT_INTERVAL_MS: i64 = 60_000;

/// What one pass did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PruneReport {
    /// Promises pruned, each leaving a tombstone.
    pub pruned: u64,
    /// Tombstones dropped because their horizon passed.
    pub expired: u64,
    /// Live tombstones, when this pass counted them.
    pub live: Option<u64>,
}

impl PruneReport {
    /// Whether the pass ran out of batch, so another pass may find more.
    pub fn full(&self, batch: usize) -> bool {
        self.pruned as usize >= batch || self.expired as usize >= batch
    }
}

/// Record a pass on the family's retention metrics.
pub fn record(report: &PruneReport) {
    crate::metrics::RETENTION_PRUNED_TOTAL.inc_by(report.pruned as f64);
    crate::metrics::RETENTION_TOMBSTONES_EXPIRED_TOTAL.inc_by(report.expired as f64);
    if let Some(live) = report.live {
        crate::metrics::RETENTION_TOMBSTONES.set(live as i64);
    }
}

/// Decides when a pass counts the live tombstones.
#[derive(Debug, Default)]
pub struct CountGate {
    last: AtomicI64,
}

impl CountGate {
    /// Whether a pass at `now` should count. The first pass always does.
    pub fn due(&self, now: i64) -> bool {
        let last = self.last.load(Ordering::Relaxed);
        if last != 0 && now - last < COUNT_INTERVAL_MS && now >= last {
            return false;
        }
        self.last.store(now.max(1), Ordering::Relaxed);
        true
    }
}

/// The three dialects a predicate is written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Sqlite,
    Postgres,
    Mysql,
}

impl Dialect {
    /// A tag's value, as text, or NULL when the promise does not carry it.
    ///
    /// The key is interpolated: `Retention::validate` has held it to printable
    /// ASCII with no quote or backslash, which every dialect reads literally.
    pub fn tag(self, key: &str) -> String {
        match self {
            Dialect::Sqlite => format!("json_extract(tags, '$.\"{key}\"')"),
            Dialect::Postgres => format!("(tags->>'{key}')"),
            Dialect::Mysql => format!("(tags->>'$.\"{key}\"')"),
        }
    }

    /// Whether `expr` starts with `prefix`. Compared by characters, as every
    /// dialect's `left`/`substr` counts them.
    pub fn starts_with(self, expr: &str, prefix: &str) -> String {
        let n = prefix.chars().count();
        match self {
            Dialect::Sqlite => format!("substr({expr}, 1, {n}) = '{prefix}'"),
            Dialect::Postgres | Dialect::Mysql => format!("left({expr}, {n}) = '{prefix}'"),
        }
    }
}

/// The policy as a predicate over a promise row at `now`: some rule selects
/// the row and `time` (the column holding its effective settlement) is at
/// least that rule's age in the past.
///
/// A promise two rules select passes when either's age has passed, which is
/// the shortest of the two — the same rule as `Retention::age_for`.
pub fn selection(dialect: Dialect, policy: &Retention, now: i64, time: &str) -> String {
    let arms: Vec<String> = policy
        .rules
        .iter()
        .map(|rule| {
            let cutoff = now.saturating_sub(rule.age as i64);
            let selects = match &rule.tag {
                None => "1 = 1".to_string(),
                Some(key) => {
                    let value = dialect.tag(key);
                    if rule.prefixes.is_empty() {
                        format!("{value} IS NOT NULL")
                    } else {
                        let any: Vec<String> = rule
                            .prefixes
                            .iter()
                            .map(|p| dialect.starts_with(&value, p))
                            .collect();
                        format!("{value} IS NOT NULL AND ({})", any.join(" OR "))
                    }
                }
            };
            format!("({selects} AND {time} <= {cutoff})")
        })
        .collect();
    if arms.is_empty() {
        "1 = 0".to_string()
    } else {
        format!("({})", arms.join(" OR "))
    }
}

/// The latest effective settlement any rule could accept at `now`: nothing
/// settled after this is prunable. The bound the candidate query's index
/// range uses.
pub fn max_cutoff(policy: &Retention, now: i64) -> i64 {
    now.saturating_sub(policy.min_age())
}

#[cfg(test)]
mod tests {
    use super::*;
    use resonate_core::retention::RetentionRule;

    fn policy() -> Retention {
        Retention {
            rules: vec![
                RetentionRule {
                    tag: Some("loams:run".into()),
                    prefixes: vec![],
                    age: 100,
                },
                RetentionRule {
                    tag: Some("loams:ns".into()),
                    prefixes: vec!["a.".into(), "b.".into()],
                    age: 10,
                },
                RetentionRule {
                    tag: None,
                    prefixes: vec![],
                    age: 1000,
                },
            ],
            tombstone_horizon: 5,
            batch: 2,
            interval: 1000,
        }
    }

    #[test]
    fn a_policy_becomes_one_arm_per_rule() {
        let sql = selection(Dialect::Postgres, &policy(), 2000, "settled_at");
        assert_eq!(
            sql,
            "(((tags->>'loams:run') IS NOT NULL AND settled_at <= 1900) OR \
             ((tags->>'loams:ns') IS NOT NULL AND (left((tags->>'loams:ns'), 2) = 'a.' OR left((tags->>'loams:ns'), 2) = 'b.') AND settled_at <= 1990) OR \
             (1 = 1 AND settled_at <= 1000))"
        );
        assert_eq!(max_cutoff(&policy(), 2000), 1990);
    }

    #[test]
    fn each_dialect_reads_a_tag_its_own_way() {
        assert_eq!(
            Dialect::Sqlite.tag("loams:run"),
            "json_extract(tags, '$.\"loams:run\"')"
        );
        assert_eq!(Dialect::Mysql.tag("loams:run"), "(tags->>'$.\"loams:run\"')");
        assert_eq!(Dialect::Sqlite.starts_with("x", "ab"), "substr(x, 1, 2) = 'ab'");
    }

    #[test]
    fn no_rules_select_nothing() {
        let mut p = policy();
        p.rules.clear();
        assert_eq!(selection(Dialect::Sqlite, &p, 0, "t"), "1 = 0");
    }

    #[test]
    fn the_count_runs_once_a_minute() {
        let g = CountGate::default();
        assert!(g.due(1_000));
        assert!(!g.due(30_000));
        assert!(g.due(61_000));
        // A clock that went backwards (a test resetting debug time) counts.
        assert!(g.due(5_000));
    }

    #[test]
    fn a_full_pass_says_so() {
        let r = PruneReport {
            pruned: 2,
            expired: 0,
            live: None,
        };
        assert!(r.full(2));
        assert!(!r.full(3));
    }
}
