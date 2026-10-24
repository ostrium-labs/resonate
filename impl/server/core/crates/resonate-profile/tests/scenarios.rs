//! The gateway-facing protocol profile (`docs/protocol-profile.md`), as
//! scenarios. Each one mirrors a row of design 74 §5.3 and runs on every
//! store the harness can reach (see `resonate_profile::backends`).
//!
//! ```text
//! cargo test -p resonate-profile
//! TEST_POSTGRES_URL=postgres://resonate:resonate@127.0.0.1:5432/resonate cargo test -p resonate-profile
//! TEST_TRACE_DIR=/some/dir cargo test -p resonate-profile     # NDJSON traces for spec/valid
//! ```
//!
//! The ids follow the profile's grammar: one origin per workflow run
//! (`<ns>.<wf>.<run>`, no ':'), and one ':' before the lineage
//! (`<kind>.<event>[.<attempt>]`).

use resonate_profile::{backends, Client, Res, Routed};
use serde_json::{json, Value};

const RUN: &str = "n1.order-42.r1";
const TQ: &str = "poll://any@tq.orders";
const GW: &str = "poll://any@gw";
const MIN: i64 = 60_000;
const HOUR: i64 = 60 * MIN;

fn id(lineage: &str) -> String {
    format!("{RUN}:{lineage}")
}

/// Run `body` once per store, each from an empty store at `T0`.
macro_rules! scenario {
    ($name:ident, $body:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        async fn $name() {
            let stores = backends().await;
            assert!(!stores.is_empty(), "no store selected");
            for b in stores {
                let mut c = Client::start(b, stringify!($name)).await;
                $body(&mut c).await;
                c.finish();
            }
        }
    };
}

/// `expect` with the answer evaluated first, so a call on `c` can be an
/// argument.
macro_rules! ex {
    ($c:expr, $what:expr, &$res:expr, $status:expr $(,)?) => {{
        let r = &$res;
        expect($c, $what, r, $status);
    }};
}

#[track_caller]
fn expect(c: &Client, what: &str, r: &Res, status: i32) {
    assert_eq!(
        r.status,
        status,
        "[{}] {what}: expected {status}, got {} {}",
        c.name(),
        r.status,
        r.data
    );
}

#[track_caller]
fn nothing_routed(c: &Client, what: &str) {
    let got = c.routed();
    assert!(got.is_empty(), "[{}] {what}: routed {got:?}", c.name());
}

#[track_caller]
fn expect_state(c: &Client, what: &str, promise: &Value, state: &str) {
    assert_eq!(
        promise["state"],
        json!(state),
        "[{}] {what}: promise {promise}",
        c.name()
    );
}

#[track_caller]
fn expect_routed(c: &Client, what: &str, got: &[Routed], want: &[(&str, &str, &str)]) {
    let got: Vec<(String, String, String)> = got
        .iter()
        .map(|r| {
            let id = match r.kind() {
                "execute" => r.message["data"]["task"]["id"].as_str(),
                _ => r.message["data"]["promise"]["id"].as_str(),
            };
            (
                r.address.clone(),
                r.kind().to_string(),
                id.unwrap_or_default().to_string(),
            )
        })
        .collect();
    let want: Vec<(String, String, String)> = want
        .iter()
        .map(|(a, k, i)| (a.to_string(), k.to_string(), i.to_string()))
        .collect();
    assert_eq!(got, want, "[{}] {what}: routed messages", c.name());
}

// ---------------------------------------------------------------------------
// Workflow start as a promise, and idempotent start
// ---------------------------------------------------------------------------

async fn workflow_start(c: &mut Client) {
    let t0 = c.now();
    let deadline = t0 + HOUR;
    let create = json!({
        "id": RUN,
        "timeoutAt": deadline,
        "param": { "headers": { "h": "1" }, "data": "aW5wdXQ=" },
        "tags": { "resonate:scope": "global", "loams:kind": "start" },
    });

    let first = c.call("promise.create", create.clone()).await;
    ex!(c, "start", &first, 200);
    let p = first.promise().clone();
    expect_state(c, "start", &p, "pending");
    assert_eq!(p["createdAt"], json!(t0), "[{}] createdAt is now", c.name());
    assert!(
        p.get("settledAt").is_none(),
        "[{}] no settledAt while pending",
        c.name()
    );
    assert_eq!(
        p["value"],
        json!({}),
        "[{}] pending has an empty value",
        c.name()
    );

    // The relay retries: same request, later. Same answer, same record.
    c.advance(10);
    let again = c.call("promise.create", create.clone()).await;
    ex!(c, "start retried", &again, 200);
    assert_eq!(
        again.promise(),
        &p,
        "[{}] a retry returns the stored record",
        c.name()
    );

    // Different param, tags and deadline under the same id: still 200, and
    // still the stored record. The server never reports the conflict; the
    // caller has to compare.
    let other = c
        .call(
            "promise.create",
            json!({
                "id": RUN,
                "timeoutAt": deadline + HOUR,
                "param": { "data": "b3RoZXI=" },
                "tags": { "resonate:timer": "true", "loams:kind": "other" },
            }),
        )
        .await;
    ex!(c, "start with other params", &other, 200);
    assert_eq!(other.promise(), &p, "[{}] the stored record wins", c.name());

    let got = c.promise_get(RUN).await;
    ex!(c, "get", &got, 200);
    assert_eq!(
        got.promise(),
        &p,
        "[{}] get returns the same record",
        c.name()
    );

    // The gateway waits on the run's result with a listener.
    let l = c.register_listener(RUN, GW).await;
    ex!(c, "listener", &l, 200);
    expect_state(c, "listener", l.promise(), "pending");

    c.advance(1_000);
    let done = c.promise_settle(RUN, "resolved", Some("b3V0")).await;
    ex!(c, "complete", &done, 200);
    expect_state(c, "complete", done.promise(), "resolved");
    assert_eq!(
        done.promise()["settledAt"],
        json!(c.now()),
        "[{}] settledAt",
        c.name()
    );
    let routed = c.routed();
    expect_routed(c, "completion", &routed, &[(GW, "unblock", RUN)]);
    assert_eq!(
        routed[0].message["data"]["promise"]["value"]["data"],
        json!("b3V0"),
        "[{}] the unblock carries the value",
        c.name()
    );

    // A start after completion answers with the completed record: no new run.
    c.advance(1_000);
    let late = c.call("promise.create", create).await;
    ex!(c, "start after completion", &late, 200);
    expect_state(c, "start after completion", late.promise(), "resolved");
    assert_eq!(
        late.promise()["value"]["data"],
        json!("b3V0"),
        "[{}]",
        c.name()
    );
    nothing_routed(c, "nothing routed");
}
scenario!(workflow_start_is_an_idempotent_promise, workflow_start);

// ---------------------------------------------------------------------------
// Activity as a task with a lease
// ---------------------------------------------------------------------------

async fn activity_lease(c: &mut Client) {
    let act = id("act.5.1");
    let created = c
        .promise_create(
            &act,
            c.now() + 10 * MIN,
            json!({ "resonate:target": TQ, "loams:kind": "act" }),
        )
        .await;
    ex!(c, "schedule activity", &created, 200);
    expect_state(c, "schedule activity", created.promise(), "pending");
    let routed = c.routed();
    expect_routed(c, "dispatch", &routed, &[(TQ, "execute", &act)]);
    assert_eq!(
        routed[0].message,
        json!({
            "kind": "execute",
            "head": { "serverUrl": resonate_profile::SERVER_URL },
            "data": { "task": { "id": act, "version": 0 } },
        }),
        "[{}] the execute message",
        c.name()
    );

    let t = c.task_get(&act).await;
    ex!(c, "task.get", &t, 200);
    assert_eq!(t.task()["state"], json!("pending"), "[{}]", c.name());
    assert_eq!(t.task()["version"], json!(0), "[{}]", c.name());

    // Claim with a 10 s lease.
    let a = c.task_acquire(&act, 0, "gw-a", 10_000).await;
    ex!(c, "acquire", &a, 200);
    assert_eq!(
        a.task(),
        &json!({ "id": act, "state": "acquired", "version": 1, "resumes": 0, "ttl": 10_000, "pid": "gw-a" }),
        "[{}] acquired task",
        c.name()
    );
    expect_state(c, "acquire", a.promise(), "pending");
    assert_eq!(
        a.data["preload"],
        json!([]),
        "[{}] no preload without resonate:branch",
        c.name()
    );

    // A second claimant loses, whatever version it names.
    ex!(
        c,
        "acquire again, old version",
        &c_acquire(c, &act, 0, "gw-b").await,
        409
    );
    ex!(
        c,
        "acquire again, new version",
        &c_acquire(c, &act, 1, "gw-b").await,
        409
    );

    // Heartbeat at 8 s moves the lease to 18 s.
    c.advance(8_000);
    let hb = c.task_heartbeat("gw-a", &[(&act, 1)]).await;
    ex!(c, "heartbeat", &hb, 200);
    assert_eq!(hb.data, json!({}), "[{}] heartbeat answers {{}}", c.name());

    // Heartbeats that match nothing are 200 too, and change nothing.
    ex!(
        c,
        "heartbeat, other pid",
        &c.task_heartbeat("gw-b", &[(&act, 1)]).await,
        200
    );
    ex!(
        c,
        "heartbeat, old version",
        &c.task_heartbeat("gw-a", &[(&act, 0)]).await,
        200
    );
    ex!(
        c,
        "heartbeat, unknown task",
        &c.task_heartbeat("gw-a", &[(&id("act.404.1"), 1)]).await,
        200,
    );

    // Past the first lease, before the extended one: still held.
    c.tick(7_000).await; // t = 15 s
    let t = c.task_get(&act).await;
    assert_eq!(
        t.task()["state"],
        json!("acquired"),
        "[{}] held at 15 s",
        c.name()
    );
    nothing_routed(c, "nothing routed at 15 s");

    // The extended lease runs out at 18 s: the task goes back to pending at
    // the same version and is dispatched again.
    c.tick(3_000).await; // t = 18 s
    let t = c.task_get(&act).await;
    assert_eq!(
        t.task(),
        &json!({ "id": act, "state": "pending", "version": 1, "resumes": 0 }),
        "[{}] redelivered task",
        c.name()
    );
    let routed = c.routed();
    expect_routed(c, "redelivery", &routed, &[(TQ, "execute", &act)]);
    assert_eq!(
        routed[0].message["data"]["task"]["version"],
        json!(1),
        "[{}]",
        c.name()
    );

    // The old holder's completion is refused.
    ex!(
        c,
        "fulfill after losing the lease",
        &c.task_fulfill(&act, 1, "resolved", None).await,
        409
    );
    ex!(
        c,
        "heartbeat after losing the lease",
        &c.task_heartbeat("gw-a", &[(&act, 1)]).await,
        200
    );

    // Another replica claims it, at the version the execute message named.
    let b = c.task_acquire(&act, 1, "gw-b", 10_000).await;
    ex!(c, "reacquire", &b, 200);
    assert_eq!(b.task()["version"], json!(2), "[{}]", c.name());

    c.advance(500);
    let f = c.task_fulfill(&act, 2, "resolved", Some("cmVzdWx0")).await;
    ex!(c, "fulfill", &f, 200);
    expect_state(c, "fulfill", f.promise(), "resolved");
    assert_eq!(
        f.promise()["value"]["data"],
        json!("cmVzdWx0"),
        "[{}]",
        c.name()
    );
    let t = c.task_get(&act).await;
    assert_eq!(
        t.task(),
        &json!({ "id": act, "state": "fulfilled", "version": 2, "resumes": 0 }),
        "[{}] fulfilled task",
        c.name()
    );
    ex!(
        c,
        "fulfill twice",
        &c.task_fulfill(&act, 2, "resolved", None).await,
        409
    );
    ex!(
        c,
        "acquire fulfilled",
        &c_acquire(c, &act, 2, "gw-c").await,
        409
    );
    nothing_routed(c, "nothing routed after fulfill");
}

async fn c_acquire(c: &mut Client, id: &str, version: i64, pid: &str) -> Res {
    c.task_acquire(id, version, pid, 10_000).await
}
scenario!(activity_is_a_task_with_a_lease, activity_lease);

async fn activity_release(c: &mut Client) {
    let act = id("act.6.1");
    ex!(
        c,
        "create",
        &c.promise_create(&act, c.now() + 10 * MIN, json!({ "resonate:target": TQ }))
            .await,
        200,
    );
    c.routed();
    ex!(
        c,
        "acquire",
        &c.task_acquire(&act, 0, "gw-a", 60_000).await,
        200
    );

    // Release hands it back at once, at the same version.
    let r = c.task_release(&act, 1).await;
    ex!(c, "release", &r, 200);
    assert_eq!(r.data, json!({}), "[{}]", c.name());
    let routed = c.routed();
    expect_routed(c, "release redispatches", &routed, &[(TQ, "execute", &act)]);
    assert_eq!(
        routed[0].message["data"]["task"]["version"],
        json!(1),
        "[{}]",
        c.name()
    );
    ex!(c, "release again", &c.task_release(&act, 1).await, 409);
    ex!(
        c,
        "release unknown",
        &c.task_release(&id("act.404.1"), 1).await,
        404
    );

    // Nobody claims it: the dispatch is repeated every retry timeout.
    c.tick(resonate_profile::RETRY_TIMEOUT - 1).await;
    nothing_routed(c, "not yet");
    c.tick(1).await;
    expect_routed(c, "retry dispatch", &c.routed(), &[(TQ, "execute", &act)]);
}
scenario!(
    release_redispatches_at_once_and_then_every_retry_timeout,
    activity_release
);

// ---------------------------------------------------------------------------
// Timers and timeouts
// ---------------------------------------------------------------------------

async fn timer(c: &mut Client) {
    let tmr = id("tmr.9");
    let fire_at = c.now() + 5_000;
    let p = c
        .promise_create(
            &tmr,
            fire_at,
            json!({ "resonate:timer": "true", "loams:kind": "tmr" }),
        )
        .await;
    ex!(c, "timer", &p, 200);
    expect_state(c, "timer", p.promise(), "pending");
    nothing_routed(c, "a timer dispatches nothing");
    ex!(c, "task.get on a timer", &c.task_get(&tmr).await, 404);

    ex!(c, "listener", &c.register_listener(&tmr, GW).await, 200);
    // The same listener twice is one listener.
    ex!(
        c,
        "listener again",
        &c.register_listener(&tmr, GW).await,
        200
    );

    c.tick(4_999).await;
    nothing_routed(c, "not before the deadline");
    {
        let g = c.promise_get(&tmr).await;
        expect_state(c, "before", g.promise(), "pending");
    }

    c.tick(1).await;
    let routed = c.routed();
    expect_routed(c, "timer fired", &routed, &[(GW, "unblock", &tmr)]);
    let fired = &routed[0].message["data"]["promise"];
    expect_state(c, "timer fired", fired, "resolved");
    assert_eq!(
        fired["settledAt"],
        json!(fire_at),
        "[{}] settledAt is the deadline",
        c.name()
    );
    assert_eq!(
        fired["value"],
        json!({}),
        "[{}] a timer resolves with no value",
        c.name()
    );
    assert_eq!(
        routed[0].message["head"],
        json!({}),
        "[{}] unblock head is empty",
        c.name()
    );

    let got = c.promise_get(&tmr).await;
    expect_state(c, "after", got.promise(), "resolved");

    // A listener on a settled promise answers with the record and sends nothing.
    let late = c.register_listener(&tmr, "poll://any@gw2").await;
    ex!(c, "late listener", &late, 200);
    expect_state(c, "late listener", late.promise(), "resolved");
    nothing_routed(c, "no unblock for a late listener");
}
scenario!(
    timer_resolves_at_its_deadline_and_unblocks_its_listener,
    timer
);

async fn timeout(c: &mut Client) {
    // A timeout is a deadline that rejects: no timer tag, but external so it
    // arms (`resonate:scope = global`).
    let to = id("to.5.s2c");
    let at = c.now() + 2_000;
    ex!(
        c,
        "timeout",
        &c.promise_create(
            &to,
            at,
            json!({ "resonate:scope": "global", "loams:kind": "to" })
        )
        .await,
        200,
    );
    ex!(c, "listener", &c.register_listener(&to, GW).await, 200);
    c.tick(2_000).await;
    let routed = c.routed();
    expect_routed(c, "timeout fired", &routed, &[(GW, "unblock", &to)]);
    let p = &routed[0].message["data"]["promise"];
    expect_state(c, "timeout fired", p, "rejected_timedout");
    assert_eq!(p["settledAt"], json!(at), "[{}]", c.name());

    // Without a tick, a read past the deadline still sees the verdict.
    let lazy = id("to.6.s2c");
    let at = c.now() + 1_000;
    ex!(
        c,
        "timeout 2",
        &c.promise_create(&lazy, at, json!({ "resonate:scope": "global" }))
            .await,
        200,
    );
    c.advance(1_000);
    let got = c.promise_get(&lazy).await;
    expect_state(
        c,
        "read at the deadline",
        got.promise(),
        "rejected_timedout",
    );
    assert_eq!(got.promise()["settledAt"], json!(at), "[{}]", c.name());

    // A deadline already past at create: created settled.
    let past = id("to.7.s2c");
    let at = c.now() - 10;
    let p = c
        .promise_create(&past, at, json!({ "resonate:scope": "global" }))
        .await;
    ex!(c, "create past deadline", &p, 200);
    expect_state(c, "create past deadline", p.promise(), "rejected_timedout");
    assert_eq!(
        p.promise()["createdAt"],
        json!(at),
        "[{}] createdAt = timeoutAt",
        c.name()
    );
    assert_eq!(p.promise()["settledAt"], json!(at), "[{}]", c.name());
}
scenario!(timeout_rejects_timedout_at_its_deadline, timeout);

// ---------------------------------------------------------------------------
// Retry with backoff: delayed creation
// ---------------------------------------------------------------------------

async fn retry_with_backoff(c: &mut Client) {
    // Attempt 1 fails.
    let a1 = id("act.5.1");
    ex!(
        c,
        "attempt 1",
        &c.promise_create(&a1, c.now() + 10 * MIN, json!({ "resonate:target": TQ }))
            .await,
        200,
    );
    c.routed();
    ex!(
        c,
        "acquire 1",
        &c.task_acquire(&a1, 0, "gw-a", 10_000).await,
        200
    );
    let f = c.task_fulfill(&a1, 1, "rejected", Some("ZXJy")).await;
    ex!(c, "fail 1", &f, 200);
    expect_state(c, "fail 1", f.promise(), "rejected");

    // Attempt 2, dispatched after a 2 s backoff. `resonate:delay` is an
    // absolute time in ms, not a duration.
    let a2 = id("act.5.2");
    let due = c.now() + 2_000;
    let p = c
        .promise_create(
            &a2,
            c.now() + 10 * MIN,
            json!({ "resonate:target": TQ, "resonate:delay": due.to_string() }),
        )
        .await;
    ex!(c, "attempt 2", &p, 200);
    expect_state(c, "attempt 2", p.promise(), "pending");
    let t = c.task_get(&a2).await;
    assert_eq!(t.task()["state"], json!("pending"), "[{}]", c.name());
    nothing_routed(c, "not dispatched before the delay");
    c.tick(1_999).await;
    nothing_routed(c, "not at delay - 1");
    c.tick(1).await;
    let routed = c.routed();
    expect_routed(
        c,
        "dispatched at the delay",
        &routed,
        &[(TQ, "execute", &a2)],
    );
    assert_eq!(
        routed[0].message["data"]["task"]["version"],
        json!(0),
        "[{}]",
        c.name()
    );
    ex!(
        c,
        "acquire 2",
        &c.task_acquire(&a2, 0, "gw-a", 10_000).await,
        200
    );

    // What the delay rejects.
    let bad = |delay: &str| json!({ "resonate:target": TQ, "resonate:delay": delay });
    let deadline = c.now() + MIN;
    for (what, tags) in [
        ("delay = timeoutAt", bad(&deadline.to_string())),
        ("delay after timeoutAt", bad(&(deadline + 1).to_string())),
        ("negative delay", bad("-1")),
        ("non-numeric delay", bad("2s")),
        (
            "delay without a target",
            json!({ "resonate:delay": (c.now() + 1).to_string() }),
        ),
    ] {
        ex!(
            c,
            what,
            &c.promise_create(&id("act.9.1"), deadline, tags).await,
            400
        );
    }
    // A delay already past dispatches at once.
    let a3 = id("act.5.3");
    ex!(
        c,
        "past delay",
        &c.promise_create(&a3, deadline, bad(&(c.now() - 1).to_string()))
            .await,
        200,
    );
    expect_routed(c, "past delay", &c.routed(), &[(TQ, "execute", &a3)]);
}
scenario!(retry_with_backoff_is_a_delayed_create, retry_with_backoff);

async fn delay_gates_dispatch_not_claim(c: &mut Client) {
    // The delay holds back the execute message only. The task is pending
    // from the start, so a caller that knows the id can claim it early.
    let a = id("act.8.2");
    let due = c.now() + 5_000;
    ex!(
        c,
        "delayed",
        &c.promise_create(
            &a,
            c.now() + MIN,
            json!({ "resonate:target": TQ, "resonate:delay": due.to_string() }),
        )
        .await,
        200,
    );
    nothing_routed(c, "a delayed task is not dispatched at create");
    let early = c.task_acquire(&a, 0, "gw-a", 10_000).await;
    ex!(c, "acquire before the delay", &early, 200);
    // And the dispatch that was due is not sent for a task already held.
    c.tick(5_000).await;
    nothing_routed(c, "no dispatch for a held task");
}
scenario!(
    delay_holds_back_dispatch_but_not_acquire,
    delay_gates_dispatch_not_claim
);

// ---------------------------------------------------------------------------
// Signal: settle a promise someone waits on
// ---------------------------------------------------------------------------

async fn signal(c: &mut Client) {
    let sig = id("sig.approve.1");
    ex!(
        c,
        "signal promise",
        &c.promise_create(&sig, c.now() + HOUR, json!({ "resonate:scope": "global" }))
            .await,
        200,
    );
    ex!(c, "listener", &c.register_listener(&sig, GW).await, 200);
    ex!(
        c,
        "second listener",
        &c.register_listener(&sig, "poll://uni@gw/replica-2").await,
        200
    );

    c.advance(100);
    let s = c.promise_settle(&sig, "resolved", Some("eWVz")).await;
    ex!(c, "signal", &s, 200);
    expect_state(c, "signal", s.promise(), "resolved");
    let routed = c.routed();
    expect_routed(
        c,
        "signal delivered to both listeners",
        &routed,
        &[
            (GW, "unblock", &sig),
            ("poll://uni@gw/replica-2", "unblock", &sig),
        ],
    );

    // The first settle wins. A second one is 200 with the stored record.
    let again = c.promise_settle(&sig, "rejected", Some("bm8=")).await;
    ex!(c, "settle again", &again, 200);
    expect_state(c, "settle again", again.promise(), "resolved");
    assert_eq!(
        again.promise()["value"]["data"],
        json!("eWVz"),
        "[{}]",
        c.name()
    );
    nothing_routed(c, "nothing routed on a repeat");

    ex!(
        c,
        "settle unknown",
        &c.promise_settle(&id("sig.none"), "resolved", None).await,
        404
    );
}
scenario!(signal_is_a_settle_of_a_waited_promise, signal);

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------

async fn cancel(c: &mut Client) {
    // A timer cancelled before it fires.
    let tmr = id("tmr.11");
    let at = c.now() + 5_000;
    ex!(
        c,
        "timer",
        &c.promise_create(&tmr, at, json!({ "resonate:timer": "true" }))
            .await,
        200
    );
    ex!(c, "listener", &c.register_listener(&tmr, GW).await, 200);
    c.advance(1_000);
    let x = c.promise_settle(&tmr, "rejected_canceled", None).await;
    ex!(c, "cancel timer", &x, 200);
    expect_state(c, "cancel timer", x.promise(), "rejected_canceled");
    expect_routed(c, "cancel delivered", &c.routed(), &[(GW, "unblock", &tmr)]);
    c.tick(10_000).await;
    nothing_routed(c, "a cancelled timer never fires");
    {
        let g = c.promise_get(&tmr).await;
        expect_state(c, "after deadline", g.promise(), "rejected_canceled");
    }

    // A cancel that loses the race to the deadline.
    let tmr2 = id("tmr.12");
    let at2 = c.now() + 1_000;
    ex!(
        c,
        "timer 2",
        &c.promise_create(&tmr2, at2, json!({ "resonate:timer": "true" }))
            .await,
        200
    );
    c.tick(1_000).await;
    let late = c.promise_settle(&tmr2, "rejected_canceled", None).await;
    ex!(c, "late cancel", &late, 200);
    expect_state(c, "late cancel", late.promise(), "resolved");

    // Cancelling a running activity: settle its promise. The task is
    // fulfilled, and the holder finds out on its next fulfill.
    let act = id("act.13.1");
    ex!(
        c,
        "activity",
        &c.promise_create(&act, c.now() + 10 * MIN, json!({ "resonate:target": TQ }))
            .await,
        200,
    );
    c.routed();
    ex!(
        c,
        "acquire",
        &c.task_acquire(&act, 0, "gw-a", 10_000).await,
        200
    );
    let x = c.promise_settle(&act, "rejected_canceled", None).await;
    ex!(c, "cancel activity", &x, 200);
    expect_state(c, "cancel activity", x.promise(), "rejected_canceled");
    let t = c.task_get(&act).await;
    assert_eq!(t.task()["state"], json!("fulfilled"), "[{}]", c.name());
    ex!(
        c,
        "holder heartbeats",
        &c.task_heartbeat("gw-a", &[(&act, 1)]).await,
        200
    );
    ex!(
        c,
        "holder fulfills",
        &c.task_fulfill(&act, 1, "resolved", None).await,
        409
    );
    c.tick(MIN).await;
    nothing_routed(c, "no redelivery after cancel");
}
scenario!(cancel_is_a_settle_rejected_canceled, cancel);

// ---------------------------------------------------------------------------
// task.create: claim in one step
// ---------------------------------------------------------------------------

async fn task_create(c: &mut Client) {
    let wft = id("wft.3");
    let tags = json!({ "resonate:target": "poll://any@ltq.0.q" });
    let deadline = c.now() + 10 * MIN;
    let r = c
        .task_create("gw-a", 10_000, &wft, deadline, tags.clone())
        .await;
    ex!(c, "task.create", &r, 200);
    assert_eq!(
        r.task(),
        &json!({ "id": wft, "state": "acquired", "version": 1, "resumes": 0, "ttl": 10_000, "pid": "gw-a" }),
        "[{}]",
        c.name()
    );
    expect_state(c, "task.create", r.promise(), "pending");
    nothing_routed(c, "task.create dispatches nothing");

    // Not retry-safe while held: the retry is 409.
    let again = c
        .task_create("gw-a", 10_000, &wft, deadline, tags.clone())
        .await;
    ex!(c, "task.create retried", &again, 409);

    // Once the lease has lapsed the task is pending, and task.create claims it.
    c.tick(10_000).await;
    c.routed();
    let r = c
        .task_create("gw-b", 10_000, &wft, deadline, tags.clone())
        .await;
    ex!(c, "task.create on a pending task", &r, 200);
    assert_eq!(r.task()["version"], json!(2), "[{}]", c.name());
    assert_eq!(r.task()["pid"], json!("gw-b"), "[{}]", c.name());

    // After completion it answers with the fulfilled task.
    ex!(
        c,
        "fulfill",
        &c.task_fulfill(&wft, 2, "resolved", None).await,
        200
    );
    let r = c.task_create("gw-c", 10_000, &wft, deadline, tags).await;
    ex!(c, "task.create on a fulfilled task", &r, 200);
    assert_eq!(r.task()["state"], json!("fulfilled"), "[{}]", c.name());

    // On a promise that exists without a target it is refused.
    let plain = id("plain.1");
    ex!(
        c,
        "plain",
        &c.promise_create(&plain, deadline, json!({})).await,
        200
    );
    let r = c
        .task_create(
            "gw-a",
            10_000,
            &plain,
            deadline,
            json!({ "resonate:target": TQ }),
        )
        .await;
    ex!(c, "task.create on a promise without a target", &r, 422);

    // What validation refuses.
    ex!(
        c,
        "task.create without a target",
        &c.task_create("gw-a", 10_000, &id("wft.4"), deadline, json!({}))
            .await,
        400,
    );
    ex!(
        c,
        "task.create with a delay",
        &c.task_create(
            "gw-a",
            10_000,
            &id("wft.4"),
            deadline,
            json!({ "resonate:target": TQ, "resonate:delay": (c.now() + 1).to_string() }),
        )
        .await,
        400,
    );
    ex!(
        c,
        "task.create with ttl 0",
        &c.task_create(
            "gw-a",
            0,
            &id("wft.4"),
            deadline,
            json!({ "resonate:target": TQ })
        )
        .await,
        400,
    );
}
scenario!(task_create_claims_in_one_step, task_create);

// ---------------------------------------------------------------------------
// Schedules
// ---------------------------------------------------------------------------

async fn schedule(c: &mut Client) {
    let sid = "n1.sched-a";
    let create = json!({
        "id": sid,
        "cron": "* * * * *",
        "promiseId": "n1.sched-a.{{.timestamp}}",
        "promiseTimeout": MIN,
        "promiseParam": { "data": "e30=" },
        "promiseTags": { "resonate:target": "poll://any@gw-sched", "loams:kind": "start" },
    });
    let s = c.call("schedule.create", create.clone()).await;
    ex!(c, "schedule.create", &s, 200);
    let rec = s.data["schedule"].clone();
    let next = c.now() - c.now() % MIN + MIN;
    assert_eq!(
        rec["nextRunAt"],
        json!(next),
        "[{}] next minute boundary",
        c.name()
    );
    assert_eq!(rec["createdAt"], json!(c.now()), "[{}]", c.name());
    assert!(rec.get("lastRunAt").is_none(), "[{}]", c.name());

    // Same id, other fields: 200 and the stored schedule.
    let mut other = create.clone();
    other["cron"] = json!("0 * * * *");
    let again = c.call("schedule.create", other).await;
    ex!(c, "schedule.create again", &again, 200);
    assert_eq!(
        again.data["schedule"],
        rec,
        "[{}] stored schedule wins",
        c.name()
    );

    // It fires on the minute: a promise from the template, dispatched.
    c.tick(next - c.now()).await;
    let pid = format!("n1.sched-a.{next}");
    expect_routed(
        c,
        "first run",
        &c.routed(),
        &[("poll://any@gw-sched", "execute", &pid)],
    );
    let p = c.promise_get(&pid).await;
    ex!(c, "fired promise", &p, 200);
    let p = p.promise();
    assert_eq!(p["createdAt"], json!(next), "[{}]", c.name());
    assert_eq!(p["timeoutAt"], json!(next + MIN), "[{}]", c.name());
    assert_eq!(p["param"]["data"], json!("e30="), "[{}]", c.name());
    for (k, v) in [
        ("resonate:schedule", sid.to_string()),
        ("resonate:origin", pid.clone()),
        ("resonate:target", "poll://any@gw-sched".to_string()),
        ("loams:kind", "start".to_string()),
    ] {
        assert_eq!(p["tags"][k], json!(v), "[{}] tag {k}", c.name());
    }

    // On time, every minute: one promise and one dispatch per run.
    for k in 1..=2 {
        c.tick(MIN).await;
        let pid = format!("n1.sched-a.{}", next + k * MIN);
        expect_routed(
            c,
            "run on time",
            &c.routed(),
            &[("poll://any@gw-sched", "execute", &pid)],
        );
    }

    // A late tick (the server was down for three runs). What a store does
    // with the missed runs differs, so the profile does not rely on it; see
    // "Schedules" in the profile.
    c.tick(3 * MIN).await;
    let late: Vec<String> = c
        .routed()
        .iter()
        .map(|r| {
            r.message["data"]["task"]["id"]
                .as_str()
                .unwrap_or_default()
                .to_string()
        })
        .collect();
    let mut runs = Vec::new();
    for k in 3..=5 {
        let pid = format!("n1.sched-a.{}", next + k * MIN);
        let g = c.promise_get(&pid).await;
        runs.push((k, g.status, g.promise()["state"].clone()));
    }
    let r = |k: i64| format!("n1.sched-a.{}", next + k * MIN);
    let expired = json!("rejected_timedout");
    match c.name() {
        // Every missed run is created. Runs whose promiseTimeout has passed
        // are created timed out; the model still dispatches them, the blob
        // server does not.
        "oracle" | "blob" | "tikv" => {
            let st: Vec<_> = runs.iter().map(|(_, s, st)| (*s, st.clone())).collect();
            assert_eq!(
                st,
                vec![
                    (200, expired.clone()),
                    (200, expired.clone()),
                    (200, json!("pending"))
                ],
                "[{}] runs",
                c.name()
            );
            let want = if c.name() == "oracle" {
                vec![r(3), r(4), r(5)]
            } else {
                vec![r(5)]
            };
            assert_eq!(late, want, "[{}] dispatched", c.name());
        }
        // One missed run per sweep, oldest first; this one is already past
        // its promiseTimeout, so it is created timed out and not dispatched.
        _ => {
            let st: Vec<_> = runs.iter().map(|(_, s, _)| *s).collect();
            assert_eq!(st, vec![200, 404, 404], "[{}] runs {runs:?}", c.name());
            assert_eq!(runs[0].2, expired, "[{}]", c.name());
            assert!(late.is_empty(), "[{}] dispatched {late:?}", c.name());
        }
    }
    let g = c.call("schedule.get", json!({ "id": sid })).await;
    ex!(c, "schedule.get", &g, 200);
    let (last, nxt) = if matches!(c.name(), "oracle" | "blob" | "tikv") {
        (r(5), r(6))
    } else {
        (r(3), r(4))
    };
    assert_eq!(
        format!("n1.sched-a.{}", g.data["schedule"]["lastRunAt"]),
        last,
        "[{}] lastRunAt",
        c.name()
    );
    assert_eq!(
        format!("n1.sched-a.{}", g.data["schedule"]["nextRunAt"]),
        nxt,
        "[{}] nextRunAt",
        c.name()
    );

    let d = c.call("schedule.delete", json!({ "id": sid })).await;
    ex!(c, "schedule.delete", &d, 200);
    ex!(
        c,
        "schedule.get deleted",
        &c.call("schedule.get", json!({ "id": sid })).await,
        404
    );
    ex!(
        c,
        "schedule.delete again",
        &c.call("schedule.delete", json!({ "id": sid })).await,
        404
    );
    c.tick(5 * MIN).await;
    nothing_routed(c, "a deleted schedule stops");

    // What validation refuses.
    let mut bad = create.clone();
    bad["id"] = json!("n1:sched");
    ex!(
        c,
        "schedule id with ':'",
        &c.call("schedule.create", bad).await,
        400
    );
    let mut bad = create.clone();
    bad["promiseTags"] = json!({});
    ex!(
        c,
        "schedule without a target",
        &c.call("schedule.create", bad).await,
        400
    );
    let mut bad = create;
    bad["cron"] = json!("every minute");
    ex!(c, "bad cron", &c.call("schedule.create", bad).await, 400);
}
scenario!(schedule_fires_start_promises, schedule);

// ---------------------------------------------------------------------------
// Answer codes the profile names, and the id rules
// ---------------------------------------------------------------------------

async fn codes(c: &mut Client) {
    let far = c.now() + HOUR;
    let missing = id("none.1");
    let cases: Vec<(&str, &str, Value, i32)> = vec![
        ("get missing", "promise.get", json!({ "id": missing }), 404),
        ("get empty id", "promise.get", json!({ "id": "" }), 400),
        (
            "create empty id",
            "promise.create",
            json!({ "id": "", "timeoutAt": far }),
            400,
        ),
        (
            "create NUL",
            "promise.create",
            json!({ "id": "a\u{0}b", "timeoutAt": far }),
            400,
        ),
        (
            "create negative deadline",
            "promise.create",
            json!({ "id": "x", "timeoutAt": -1 }),
            400,
        ),
        (
            "create no deadline",
            "promise.create",
            json!({ "id": "x" }),
            400,
        ),
        (
            "create bad target",
            "promise.create",
            json!({ "id": "x", "timeoutAt": far, "tags": { "resonate:target": "not a url" } }),
            400,
        ),
        (
            "create origin tag mismatch",
            "promise.create",
            json!({ "id": "a:b", "timeoutAt": far, "tags": { "resonate:origin": "z" } }),
            400,
        ),
        (
            "settle rejected_timedout",
            "promise.settle",
            json!({ "id": missing, "state": "rejected_timedout", "value": {} }),
            400,
        ),
        (
            "settle missing",
            "promise.settle",
            json!({ "id": missing, "state": "resolved", "value": {} }),
            404,
        ),
        (
            "listener missing",
            "promise.register_listener",
            json!({ "awaited": missing, "address": GW }),
            404,
        ),
        (
            "listener bad address",
            "promise.register_listener",
            json!({ "awaited": missing, "address": "nope" }),
            400,
        ),
        (
            "task.get missing",
            "task.get",
            json!({ "id": missing }),
            404,
        ),
        (
            "acquire missing",
            "task.acquire",
            json!({ "id": missing, "version": 0, "pid": "p", "ttl": 1 }),
            404,
        ),
        (
            "acquire ttl 0",
            "task.acquire",
            json!({ "id": missing, "version": 0, "pid": "p", "ttl": 0 }),
            400,
        ),
        (
            "heartbeat no tasks",
            "task.heartbeat",
            json!({ "pid": "p", "tasks": [] }),
            400,
        ),
        (
            "heartbeat across origins",
            "task.heartbeat",
            json!({ "pid": "p", "tasks": [ { "id": "a:x", "version": 1 }, { "id": "b:x", "version": 1 } ] }),
            400,
        ),
        (
            "fulfill action id mismatch",
            "task.fulfill",
            json!({ "id": missing, "version": 1, "action": { "kind": "promise.settle", "head": {}, "data": { "id": "other", "state": "resolved", "value": {} } } }),
            400,
        ),
        (
            "fulfill missing",
            "task.fulfill",
            json!({ "id": missing, "version": 1, "action": { "kind": "promise.settle", "head": {}, "data": { "id": missing, "state": "resolved", "value": {} } } }),
            404,
        ),
        ("unknown kind", "promise.frobnicate", json!({}), 400),
    ];
    for (what, kind, data, status) in cases {
        let r = c.call(kind, data).await;
        ex!(c, what, &r, status);
        assert!(
            r.data.is_string(),
            "[{}] {what}: an error carries a string",
            c.name()
        );
    }

    // A listener is refused on a promise nobody can wait on.
    let internal = id("internal.1");
    ex!(
        c,
        "internal",
        &c.promise_create(&internal, far, json!({})).await,
        200
    );
    ex!(
        c,
        "listener on internal",
        &c.register_listener(&internal, GW).await,
        422
    );
}
scenario!(answer_codes, codes);

/// A noisy (incompressible) printable id of `n` bytes.
fn noisy(n: usize, seed: u64) -> String {
    let alphabet = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._";
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            alphabet[(x % alphabet.len() as u64) as usize] as char
        })
        .collect()
}

async fn id_rules(c: &mut Client) {
    let far = c.now() + HOUR;
    let name = c.name().to_string();
    let pg = name == "postgres";
    let mysql = name == "mysql";

    // 255 bytes fits every store, as a root and as origin:lineage.
    let root = noisy(255, 1);
    ex!(
        c,
        "255-byte root id",
        &c.promise_create(&root, far, json!({})).await,
        200
    );
    let lin = format!("{}:{}", noisy(100, 2), noisy(154, 3));
    assert_eq!(lin.len(), 255);
    ex!(
        c,
        "255-byte origin:lineage id",
        &c.promise_create(&lin, far, json!({})).await,
        200
    );
    let tgt = format!("{}:{}", noisy(100, 4), noisy(154, 5));
    ex!(
        c,
        "255-byte targeted id",
        &c.promise_create(&tgt, far, json!({ "resonate:target": TQ }))
            .await,
        200,
    );
    c.routed();

    // Every byte but NUL: '/', '%', space, non-ASCII, control characters.
    for (k, x) in ["a/b", "a%3Ab", "a b", "ä€😀", "a\u{1}b", ".-_~!*'()"]
        .iter()
        .enumerate()
    {
        let i = format!("{RUN}:chars.{k}.{x}");
        let r = c.promise_create(&i, far, json!({})).await;
        ex!(c, &format!("id with {x:?}"), &r, 200);
        assert_eq!(r.promise()["id"], json!(i), "[{}] id round-trips", c.name());
    }

    // More than one ':' is an id like any other; the origin is the text
    // before the first one.
    for i in ["a:b:c", "wft:n1:order-42:r1:3", "x::y", "::"] {
        let r = c.promise_create(i, far, json!({})).await;
        ex!(c, &format!("id {i:?}"), &r, 200);
        assert_eq!(r.promise()["id"], json!(i), "[{}] id round-trips", c.name());
    }

    // Over 255 bytes: MySQL's VARCHAR(255).
    let r = c.promise_create(&noisy(256, 6), far, json!({})).await;
    ex!(c, "256-byte id", &r, if mysql { 400 } else { 200 });

    // A target and a listener address past 255 bytes. MySQL/TiDB stores
    // both in VARCHAR(255) columns and keeps the first 255 bytes without an
    // error, so the messages go to a truncated address (a known bug, pinned
    // here; the profile limit of 255 bytes keeps clear of it).
    let long = format!("poll://any@{}", noisy(300, 10));
    let want = if mysql { 255 } else { long.len() };
    let act = id("act.long.1");
    ex!(
        c,
        "311-byte target",
        &c.promise_create(&act, far, json!({ "resonate:target": long }))
            .await,
        200
    );
    let first: Vec<usize> = c.routed().iter().map(|r| r.address.len()).collect();
    assert_eq!(first, vec![long.len()], "[{}] first dispatch", c.name());
    c.tick(resonate_profile::RETRY_TIMEOUT).await;
    let again: Vec<usize> = c
        .routed()
        .iter()
        .filter(|r| r.message["data"]["task"]["id"] == json!(act))
        .map(|r| r.address.len())
        .collect();
    assert_eq!(
        again,
        vec![want],
        "[{}] redispatch address length",
        c.name()
    );
    let g = id("glob.long");
    ex!(
        c,
        "global",
        &c.promise_create(&g, far, json!({ "resonate:scope": "global" }))
            .await,
        200
    );
    ex!(
        c,
        "311-byte listener",
        &c.register_listener(&g, &long).await,
        200
    );
    ex!(
        c,
        "settle",
        &c.promise_settle(&g, "resolved", None).await,
        200
    );
    let unblock: Vec<usize> = c.routed().iter().map(|r| r.address.len()).collect();
    assert_eq!(unblock, vec![want], "[{}] unblock address length", c.name());

    // The Postgres btree limit: 2704-byte index rows. An incompressible id
    // of 2692 bytes fits; 2693 does not. (A compressible one goes further.)
    if !mysql {
        let r = c.promise_create(&noisy(2692, 7), far, json!({})).await;
        ex!(c, "2692-byte id", &r, 200);
        let r = c.promise_create(&noisy(2693, 8), far, json!({})).await;
        ex!(c, "2693-byte id", &r, if pg { 500 } else { 200 });
        // TiKV refuses keys over 8 KiB (`max-key-size`), and the id is in the
        // document's key: the write fails at once and the request has no
        // answer (503), so the TiKV store is not asked.
        if name != "tikv" {
            let r = c.promise_create(&noisy(100_000, 9), far, json!({})).await;
            ex!(c, "100 000-byte id", &r, if pg { 500 } else { 200 });
        }
    }

    // `resonate:timer` means a timer only when it is exactly "true".
    let not_timer = id("tmr.notreally");
    let at = c.now() + 1_000;
    ex!(
        c,
        "timer = \"1\"",
        &c.promise_create(
            &not_timer,
            at,
            json!({ "resonate:timer": "1", "resonate:scope": "global" })
        )
        .await,
        200,
    );
    c.tick(1_000).await;
    let g = c.promise_get(&not_timer).await;
    expect_state(
        c,
        "timer = \"1\" at its deadline",
        g.promise(),
        "rejected_timedout",
    );
}
scenario!(id_rules_and_limits, id_rules);

/// A timer with a target is outside the specification
/// (`well_formed_promise_timer_not_targeted` in spec/02-abstract/properties.lean).
/// Postgres enforces it as a schema check (500); the oracle, SQLite and blob
/// accept the create, so their trace of this scenario is refuted by the
/// trace checkers at this event, as it should be. The TypeScript SDK's
/// `ctx.sleep` sends exactly this pair.
async fn timer_with_target(c: &mut Client) {
    let pg = c.name() == "postgres";
    let r = c
        .promise_create(
            &id("tmr.tgt"),
            c.now() + HOUR,
            json!({ "resonate:timer": "true", "resonate:target": TQ }),
        )
        .await;
    ex!(c, "timer with a target", &r, if pg { 500 } else { 200 });
}
scenario!(
    timer_with_a_target_is_refused_only_by_postgres,
    timer_with_target
);

/// Ids with several ':' (design 74 §5.4's shape) through a whole lifecycle:
/// a workflow task dispatched, claimed, heartbeated, lost, reclaimed and
/// completed; a timer; and a listener. The origin of each is the text before
/// its first ':' (`wft`, `tmr`), which matters for where a store keeps them,
/// never for whether it accepts them.
async fn several_colons(c: &mut Client) {
    let wft = "wft:n1.0:order%3A42:r1:3";
    let tmr = "tmr:n1.0:order%3A42:r1:7";
    let q = "poll://any@ltq.0.q";
    let p = c
        .promise_create(wft, c.now() + 10 * MIN, json!({ "resonate:target": q }))
        .await;
    ex!(c, "create", &p, 200);
    assert_eq!(p.promise()["id"], json!(wft), "[{}]", c.name());
    expect_routed(c, "dispatch", &c.routed(), &[(q, "execute", wft)]);
    ex!(
        c,
        "acquire",
        &c.task_acquire(wft, 0, "gw-a", 5_000).await,
        200
    );
    ex!(
        c,
        "heartbeat",
        &c.task_heartbeat("gw-a", &[(wft, 1)]).await,
        200
    );
    c.tick(5_000).await;
    expect_routed(c, "redelivery", &c.routed(), &[(q, "execute", wft)]);
    ex!(
        c,
        "reacquire",
        &c.task_acquire(wft, 1, "gw-b", 5_000).await,
        200
    );
    let f = c.task_fulfill(wft, 2, "resolved", None).await;
    ex!(c, "fulfill", &f, 200);
    expect_state(c, "fulfill", f.promise(), "resolved");

    let at = c.now() + 1_000;
    ex!(
        c,
        "timer",
        &c.promise_create(tmr, at, json!({ "resonate:timer": "true" }))
            .await,
        200
    );
    ex!(c, "listener", &c.register_listener(tmr, GW).await, 200);
    c.tick(1_000).await;
    expect_routed(c, "timer fired", &c.routed(), &[(GW, "unblock", tmr)]);
    // Re-create is idempotent for these ids too.
    let again = c
        .promise_create(tmr, at, json!({ "resonate:timer": "true" }))
        .await;
    ex!(c, "re-create", &again, 200);
    expect_state(c, "re-create", again.promise(), "resolved");
}
scenario!(ids_with_several_colons_work_on_every_store, several_colons);
