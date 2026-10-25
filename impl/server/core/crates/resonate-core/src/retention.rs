//! Settled-promise retention: which promises a server may prune, and what a
//! pruned id answers afterwards.
//!
//! The protocol has no delete. A promise, once created, is kept forever, and a
//! server that settles millions of them a day grows without bound. Retention
//! is the server setting that lets it forget: a promise settled long enough
//! ago, and selected by a tag, is removed, and its id is kept as a
//! **tombstone** for a further horizon.
//!
//! The tombstone is the point. A late retry of `promise.create` for a pruned
//! id would otherwise find nothing, create the promise again, and run the work
//! a second time. While the tombstone lives, every request that names the id
//! answers [`EXPIRED_STATUS`] instead: the caller learns, definitely, that the
//! promise existed, settled and was pruned, and that its value is gone. After
//! the tombstone horizon the id is free again, so the horizon must exceed the
//! longest time any client may still retry a create.
//!
//! This module is the part every store shares: the policy as configured, the
//! selector, and the ids a request names. How a store finds what to prune and
//! where it keeps tombstones is the store's own business. The specification of
//! the same rules is `spec/spec/02-abstract/retention.lean` and
//! `spec/tlap/Retention.tla`; the operator-facing contract is
//! `docs/protocol-profile.md` ("Retention").

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// The status a request answers when it names a tombstoned id.
///
/// 410 Gone: the resource existed and was removed on purpose, and the
/// condition is expected to be permanent for as long as the server remembers
/// it. That is exactly a tombstone. It is distinct from 404, which a caller
/// reads as "not created yet" and may answer by creating it — the very
/// re-run a tombstone is there to prevent.
pub const EXPIRED_STATUS: i32 = 410;

/// The message that goes with [`EXPIRED_STATUS`].
pub const EXPIRED_MESSAGE: &str =
    "expired: the promise was settled and pruned by retention; its id stays reserved until the tombstone horizon passes";

/// Default tombstone horizon: seven days.
pub const DEFAULT_TOMBSTONE_HORIZON_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// Default interval between an SQL server's retention passes: one second.
pub const DEFAULT_INTERVAL_MS: u64 = 1000;

/// Default number of promises one retention pass prunes.
pub const DEFAULT_BATCH: usize = 500;

/// One selector: which promises, and how long after settlement they go.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetentionRule {
    /// The tag key a promise must carry, for example `loams:run`. Absent
    /// means every promise.
    #[serde(default)]
    pub tag: Option<String>,
    /// Prefixes the tag's value must start with, any one of them. Empty means
    /// any value. Ignored when `tag` is absent.
    #[serde(default)]
    pub prefixes: Vec<String>,
    /// How long after settlement a selected promise may be pruned, in
    /// milliseconds.
    pub age: u64,
}

impl RetentionRule {
    /// Whether this rule selects a promise with these tags.
    pub fn selects<'a>(&self, get: impl Fn(&str) -> Option<&'a str>) -> bool {
        match &self.tag {
            None => true,
            Some(key) => match get(key) {
                None => false,
                Some(value) => {
                    self.prefixes.is_empty() || self.prefixes.iter().any(|p| value.starts_with(p))
                }
            },
        }
    }
}

/// `[servers.<id>.retention]`: the whole policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    /// The selectors. A promise is prunable when ANY rule selects it, after
    /// the shortest age among the rules that do.
    pub rules: Vec<RetentionRule>,
    /// How long a pruned id stays reserved, in milliseconds.
    #[serde(default = "default_tombstone_horizon")]
    pub tombstone_horizon: u64,
    /// The most promises one pass prunes, and the most tombstones it expires.
    #[serde(default = "default_batch")]
    pub batch: usize,
    /// How often an SQL server runs a pass, in milliseconds. A pass that
    /// fills its batch is followed at once by another. The blob server prunes
    /// on its own per-origin timers and ignores this.
    #[serde(default = "default_interval")]
    pub interval: u64,
}

fn default_interval() -> u64 {
    DEFAULT_INTERVAL_MS
}

fn default_tombstone_horizon() -> u64 {
    DEFAULT_TOMBSTONE_HORIZON_MS
}

fn default_batch() -> usize {
    DEFAULT_BATCH
}

impl Retention {
    /// Refuse a policy that cannot mean what it says.
    ///
    /// Tag keys and prefixes reach SQL as literals (they are the operator's
    /// configuration, never a request's), so they are held to a character set
    /// that needs no escaping in any dialect: printable ASCII without quotes or
    /// backslashes. That covers every Loams tag and namespace id.
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.is_empty() {
            return Err("retention.rules must name at least one rule".into());
        }
        if self.batch == 0 {
            return Err("retention.batch must be at least 1".into());
        }
        if self.interval == 0 {
            return Err("retention.interval must be at least 1 ms".into());
        }
        if self.tombstone_horizon == 0 {
            return Err(
                "retention.tombstone_horizon must be positive: a pruned id with no tombstone \
                 can be re-created by a late retry, which re-runs the work"
                    .into(),
            );
        }
        for (i, rule) in self.rules.iter().enumerate() {
            if let Some(tag) = &rule.tag {
                check_literal(&format!("retention.rules[{i}].tag"), tag, false)?;
            } else if !rule.prefixes.is_empty() {
                return Err(format!(
                    "retention.rules[{i}].prefixes needs a tag to apply to"
                ));
            }
            for p in &rule.prefixes {
                check_literal(&format!("retention.rules[{i}].prefixes"), p, true)?;
            }
            if i64::try_from(rule.age).is_err() {
                return Err(format!("retention.rules[{i}].age is out of range"));
            }
        }
        if i64::try_from(self.tombstone_horizon).is_err() {
            return Err("retention.tombstone_horizon is out of range".into());
        }
        Ok(())
    }

    /// The age after which a promise with these tags may be pruned: the
    /// shortest among the rules that select it, or `None` if none does.
    pub fn age_for<'a>(&self, get: impl Fn(&str) -> Option<&'a str> + Copy) -> Option<i64> {
        self.rules
            .iter()
            .filter(|r| r.selects(get))
            .map(|r| r.age as i64)
            .min()
    }

    /// [`Retention::age_for`] over a tag map.
    pub fn age_for_tags(&self, tags: &HashMap<String, String>) -> Option<i64> {
        self.age_for(|k| tags.get(k).map(String::as_str))
    }

    /// The shortest age of any rule: nothing settled more recently than this
    /// can be prunable, whatever its tags.
    pub fn min_age(&self) -> i64 {
        self.rules.iter().map(|r| r.age as i64).min().unwrap_or(0)
    }

    /// The tombstone horizon as a signed instant offset.
    pub fn horizon(&self) -> i64 {
        self.tombstone_horizon as i64
    }
}

fn check_literal(field: &str, s: &str, allow_empty: bool) -> Result<(), String> {
    if s.is_empty() && !allow_empty {
        return Err(format!("{field} must not be empty"));
    }
    if let Some(c) = s
        .chars()
        .find(|c| !c.is_ascii_graphic() || matches!(c, '\'' | '"' | '\\'))
    {
        return Err(format!(
            "{field} {s:?} holds {c:?}: tag keys and prefixes are printable ASCII without quotes or backslashes"
        ));
    }
    Ok(())
}

/// The promise ids a request names, which a tombstone answers for.
///
/// Every request whose subject is a promise (or the task that shares its id)
/// names it here: a request naming a tombstoned id answers
/// [`EXPIRED_STATUS`] rather than acting on, or re-creating, what was pruned.
/// `task.heartbeat` is left out on purpose: it is a batch over the caller's
/// leases, and a lease on a pruned task has nothing left to renew, which a
/// heartbeat already ignores. Searches and schedules name no promise.
///
/// Read off the request's JSON so that every store applies the same list,
/// whatever it deserializes into. Fields that are absent or not strings are
/// skipped: validation, which runs first, is what refuses malformed data.
pub fn subject_ids(kind: &str, data: &serde_json::Value) -> Vec<String> {
    let s = |v: &serde_json::Value| v.as_str().map(str::to_string);
    let mut out = Vec::new();
    match kind {
        "promise.get" | "promise.create" | "promise.settle" | "task.get" | "task.acquire"
        | "task.release" | "task.fulfill" | "task.halt" | "task.continue" => {
            out.extend(s(&data["id"]));
        }
        "promise.register_callback" => {
            out.extend(s(&data["awaited"]));
            out.extend(s(&data["awaiter"]));
        }
        "promise.register_listener" => {
            out.extend(s(&data["awaited"]));
        }
        "task.create" => {
            out.extend(s(&data["action"]["data"]["id"]));
        }
        "task.fence" => {
            out.extend(s(&data["id"]));
            out.extend(s(&data["action"]["data"]["id"]));
        }
        "task.suspend" => {
            out.extend(s(&data["id"]));
            if let Some(actions) = data["actions"].as_array() {
                for a in actions {
                    out.extend(s(&a["data"]["awaited"]));
                }
            }
        }
        _ => {}
    }
    out.sort();
    out.dedup();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn tags(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn rule(tag: Option<&str>, prefixes: &[&str], age: u64) -> RetentionRule {
        RetentionRule {
            tag: tag.map(str::to_string),
            prefixes: prefixes.iter().map(|p| p.to_string()).collect(),
            age,
        }
    }

    fn policy(rules: Vec<RetentionRule>) -> Retention {
        Retention {
            rules,
            tombstone_horizon: 1000,
            batch: 10,
            interval: 1000,
        }
    }

    #[test]
    fn a_rule_without_a_tag_selects_everything() {
        let p = policy(vec![rule(None, &[], 5)]);
        assert_eq!(p.age_for_tags(&tags(&[])), Some(5));
    }

    #[test]
    fn a_tag_rule_needs_the_tag_and_one_of_its_prefixes() {
        let p = policy(vec![rule(Some("loams:ns"), &["ns1.", "ns2."], 5)]);
        assert_eq!(p.age_for_tags(&tags(&[("loams:ns", "ns1.default")])), Some(5));
        assert_eq!(p.age_for_tags(&tags(&[("loams:ns", "ns2.x")])), Some(5));
        assert_eq!(p.age_for_tags(&tags(&[("loams:ns", "ns3.x")])), None);
        assert_eq!(p.age_for_tags(&tags(&[("loams:run", "ns1.x")])), None);
    }

    #[test]
    fn a_tag_rule_without_prefixes_takes_any_value() {
        let p = policy(vec![rule(Some("loams:run"), &[], 5)]);
        assert_eq!(p.age_for_tags(&tags(&[("loams:run", "")])), Some(5));
    }

    #[test]
    fn the_shortest_selecting_age_wins() {
        let p = policy(vec![
            rule(Some("loams:run"), &[], 50),
            rule(Some("loams:ns"), &["fast."], 5),
            rule(Some("other"), &[], 1),
        ]);
        let t = tags(&[("loams:run", "r"), ("loams:ns", "fast.x")]);
        assert_eq!(p.age_for_tags(&t), Some(5));
        assert_eq!(p.min_age(), 1);
    }

    #[test]
    fn validation_refuses_what_cannot_be_meant() {
        assert!(policy(vec![]).validate().is_err());
        assert!(policy(vec![rule(None, &["x"], 1)]).validate().is_err());
        assert!(policy(vec![rule(Some("a'b"), &[], 1)]).validate().is_err());
        assert!(policy(vec![rule(Some("a"), &["x\"y"], 1)]).validate().is_err());
        assert!(policy(vec![rule(Some(""), &[], 1)]).validate().is_err());
        let mut p = policy(vec![rule(Some("loams:run"), &["ns.1-2"], 1)]);
        assert!(p.validate().is_ok());
        p.tombstone_horizon = 0;
        assert!(p.validate().is_err());
        p.tombstone_horizon = 1;
        p.batch = 0;
        assert!(p.validate().is_err());
    }

    #[test]
    fn the_policy_reads_from_toml_shaped_json_with_defaults() {
        let p: Retention = serde_json::from_value(json!({
            "rules": [{ "tag": "loams:run", "age": 259200000u64 }]
        }))
        .unwrap();
        assert_eq!(p.tombstone_horizon, DEFAULT_TOMBSTONE_HORIZON_MS);
        assert_eq!(p.batch, DEFAULT_BATCH);
        assert!(p.rules[0].prefixes.is_empty());
    }

    #[test]
    fn every_request_names_its_subjects() {
        assert_eq!(subject_ids("promise.get", &json!({"id": "a"})), vec!["a"]);
        assert_eq!(
            subject_ids("promise.create", &json!({"id": "a", "timeoutAt": 1})),
            vec!["a"]
        );
        assert_eq!(
            subject_ids(
                "promise.register_callback",
                &json!({"awaited": "o:b", "awaiter": "o:a"})
            ),
            vec!["o:a", "o:b"]
        );
        assert_eq!(
            subject_ids("promise.register_listener", &json!({"awaited": "b", "address": "x"})),
            vec!["b"]
        );
        assert_eq!(
            subject_ids(
                "task.create",
                &json!({"pid": "p", "ttl": 1, "action": {"kind": "promise.create", "head": {}, "data": {"id": "t"}}})
            ),
            vec!["t"]
        );
        assert_eq!(
            subject_ids(
                "task.fence",
                &json!({"id": "o:t", "version": 1, "action": {"kind": "promise.create", "data": {"id": "o:c"}}})
            ),
            vec!["o:c", "o:t"]
        );
        assert_eq!(
            subject_ids(
                "task.suspend",
                &json!({"id": "o:t", "version": 1, "actions": [
                    {"kind": "promise.register_callback", "data": {"awaited": "o:a", "awaiter": "o:t"}},
                    {"kind": "promise.register_callback", "data": {"awaited": "o:b", "awaiter": "o:t"}}
                ]})
            ),
            vec!["o:a", "o:b", "o:t"]
        );
        for kind in ["task.get", "task.acquire", "task.release", "task.fulfill", "task.halt", "task.continue"] {
            assert_eq!(subject_ids(kind, &json!({"id": "t"})), vec!["t"], "{kind}");
        }
        assert!(subject_ids("task.heartbeat", &json!({"pid": "p", "tasks": [{"id": "t"}]})).is_empty());
        assert!(subject_ids("promise.search", &json!({})).is_empty());
        assert!(subject_ids("schedule.create", &json!({"promiseId": "x"})).is_empty());
        assert!(subject_ids("promise.get", &json!({"id": 3})).is_empty());
    }
}
