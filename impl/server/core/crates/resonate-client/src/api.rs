//! One typed method per protocol request kind.
//!
//! Each builds the request's `data`, sends it through [`Client::exchange`],
//! checks the status, and decodes the payload. Success is `200` everywhere,
//! plus `300` for `task.suspend` (an awaited promise already settled); every
//! other status is [`Error::Rejected`].

use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::client::Client;
use crate::error::Error;
use crate::model::*;
use crate::transport::Raw;

/// Every request kind this client has a typed method for. The round-trip
/// tests check each one against a server.
pub const KINDS: &[&str] = &[
    "promise.get",
    "promise.create",
    "promise.settle",
    "promise.register_callback",
    "promise.register_listener",
    "promise.search",
    "task.get",
    "task.create",
    "task.acquire",
    "task.release",
    "task.fulfill",
    "task.suspend",
    "task.fence",
    "task.heartbeat",
    "task.halt",
    "task.continue",
    "task.search",
    "schedule.get",
    "schedule.create",
    "schedule.delete",
    "schedule.search",
    "debug.reset",
    "debug.snap",
    "debug.tick",
];

fn to_value<T: serde::Serialize>(v: &T) -> Value {
    serde_json::to_value(v).expect("request bodies are plain data and always serialize")
}

impl Client {
    /// Send, and accept only the statuses in `ok`.
    async fn call(&self, kind: &str, data: Value, ok: &[u16]) -> Result<(Raw, u32), Error> {
        let (raw, attempts) = self.exchange(self.outgoing(kind, data)).await?;
        if ok.contains(&raw.status) {
            return Ok((raw, attempts));
        }
        let message = match &raw.data {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        Err(Error::Rejected {
            kind: raw.kind,
            corr_id: raw.corr_id,
            status: raw.status,
            message,
            attempts,
        })
    }

    /// Send, require `200`, and decode the payload as `W`.
    async fn call_as<W: DeserializeOwned>(
        &self,
        kind: &str,
        data: Value,
    ) -> Result<Response<W>, Error> {
        let (raw, attempts) = self.call(kind, data, &[200]).await?;
        decode(kind, raw, attempts)
    }

    /// Send, require `200`, and ignore the payload.
    async fn call_unit(&self, kind: &str, data: Value) -> Result<Response<()>, Error> {
        let (raw, attempts) = self.call(kind, data, &[200]).await?;
        Ok(Response {
            status: raw.status,
            corr_id: raw.corr_id,
            data: (),
            attempts,
        })
    }

    // --- promise.* ---

    /// `promise.get`: the promise as it is now (a pending promise past its
    /// deadline reads as timed out). `404` if there is none.
    pub async fn promise_get(&self, id: &str) -> Result<Response<PromiseRecord>, Error> {
        let r = self
            .call_as::<PromiseData>("promise.get", json!({ "id": id }))
            .await?;
        Ok(r.map(|d| d.promise))
    }

    /// `promise.create`: create a promise, or, if `id` exists, answer the
    /// existing one unchanged. The server answers `200` either way — compare
    /// the record with what you asked for if the difference matters.
    pub async fn promise_create(
        &self,
        create: &PromiseCreate,
    ) -> Result<Response<PromiseRecord>, Error> {
        let r = self
            .call_as::<PromiseData>("promise.create", to_value(create))
            .await?;
        Ok(r.map(|d| d.promise))
    }

    /// `promise.settle`: settle a pending promise, or answer a settled one as
    /// it is (`200` either way). `404` if there is none.
    pub async fn promise_settle(
        &self,
        settle: &PromiseSettle,
    ) -> Result<Response<PromiseRecord>, Error> {
        let r = self
            .call_as::<PromiseData>("promise.settle", to_value(settle))
            .await?;
        Ok(r.map(|d| d.promise))
    }

    /// `promise.register_callback`: resume the task `awaiter` when `awaited`
    /// settles. Answers the awaited promise.
    pub async fn promise_register_callback(
        &self,
        awaited: &str,
        awaiter: &str,
    ) -> Result<Response<PromiseRecord>, Error> {
        let r = self
            .call_as::<PromiseData>(
                "promise.register_callback",
                json!({ "awaited": awaited, "awaiter": awaiter }),
            )
            .await?;
        Ok(r.map(|d| d.promise))
    }

    /// `promise.register_listener`: send an `unblock` message to `address`
    /// when `awaited` settles. Answers the awaited promise.
    pub async fn promise_register_listener(
        &self,
        awaited: &str,
        address: &str,
    ) -> Result<Response<PromiseRecord>, Error> {
        let r = self
            .call_as::<PromiseData>(
                "promise.register_listener",
                json!({ "awaited": awaited, "address": address }),
            )
            .await?;
        Ok(r.map(|d| d.promise))
    }

    /// `promise.search`: one page of promises.
    pub async fn promise_search(
        &self,
        search: &PromiseSearch,
    ) -> Result<Response<Page<PromiseRecord>>, Error> {
        let r = self
            .call_as::<PromisePage>("promise.search", to_value(search))
            .await?;
        Ok(r.map(|p| Page {
            items: p.promises,
            cursor: p.cursor,
        }))
    }

    // --- task.* ---

    /// `task.get`. `404` if there is none.
    pub async fn task_get(&self, id: &str) -> Result<Response<TaskRecord>, Error> {
        let r = self
            .call_as::<TaskData>("task.get", json!({ "id": id }))
            .await?;
        Ok(r.map(|d| d.task))
    }

    /// `task.create`: create a promise with a task and take the task's lease
    /// as `pid` for `ttl` ms, in one step. `promise` must carry
    /// `resonate:target`.
    ///
    /// On an existing task the server answers by the task's state: a pending
    /// one is acquired, a fulfilled one is answered as it is, and any other
    /// is `409`. A promise that exists without a task is `422`.
    pub async fn task_create(
        &self,
        pid: &str,
        ttl: i64,
        promise: &PromiseCreate,
    ) -> Result<Response<TaskClaim>, Error> {
        self.call_as(
            "task.create",
            json!({
                "pid": pid,
                "ttl": ttl,
                "action": { "kind": "promise.create", "head": {}, "data": to_value(promise) },
            }),
        )
        .await
    }

    /// `task.acquire`: take a pending task at `version` as `pid` for `ttl` ms.
    /// `409` if it is not pending at that version.
    pub async fn task_acquire(
        &self,
        id: &str,
        version: i64,
        pid: &str,
        ttl: i64,
    ) -> Result<Response<TaskClaim>, Error> {
        self.call_as(
            "task.acquire",
            json!({ "id": id, "version": version, "pid": pid, "ttl": ttl }),
        )
        .await
    }

    /// `task.release`: give an acquired task back, to be dispatched again.
    pub async fn task_release(&self, id: &str, version: i64) -> Result<Response<()>, Error> {
        self.call_unit("task.release", json!({ "id": id, "version": version }))
            .await
    }

    /// `task.fulfill`: settle the task's own promise and finish the task.
    /// Answers the settled promise.
    pub async fn task_fulfill(
        &self,
        id: &str,
        version: i64,
        state: SettleState,
        value: PromiseValue,
    ) -> Result<Response<PromiseRecord>, Error> {
        let action = PromiseSettle {
            id: id.to_string(),
            state,
            value,
        };
        let r = self
            .call_as::<PromiseData>(
                "task.fulfill",
                json!({
                    "id": id,
                    "version": version,
                    "action": { "kind": "promise.settle", "head": {}, "data": to_value(&action) },
                }),
            )
            .await?;
        Ok(r.map(|d| d.promise))
    }

    /// `task.suspend`: suspend an acquired task until one of `awaited`
    /// settles. If one already has, the task is not suspended
    /// ([`Suspension::Resume`], status `300`).
    pub async fn task_suspend(
        &self,
        id: &str,
        version: i64,
        awaited: &[&str],
    ) -> Result<Response<Suspension>, Error> {
        let actions: Vec<Value> = awaited
            .iter()
            .map(|a| {
                json!({
                    "kind": "promise.register_callback",
                    "head": {},
                    "data": { "awaited": a, "awaiter": id },
                })
            })
            .collect();
        let (raw, attempts) = self
            .call(
                "task.suspend",
                json!({ "id": id, "version": version, "actions": actions }),
                &[200, 300],
            )
            .await?;
        let status = raw.status;
        let r = if status == 300 {
            decode::<PreloadData>("task.suspend", raw, attempts)?
                .map(|d| Suspension::Resume { preload: d.preload })
        } else {
            Response {
                status,
                corr_id: raw.corr_id,
                data: Suspension::Suspended,
                attempts,
            }
        };
        Ok(r)
    }

    /// `task.fence`: create or settle a promise only while this task's lease
    /// at `version` is held. The action's own outcome is in
    /// [`Fenced::action`]; the fence itself is `409` if the lease is not held.
    pub async fn task_fence(
        &self,
        id: &str,
        version: i64,
        action: &FenceAction,
    ) -> Result<Response<Fenced>, Error> {
        let data = match action {
            FenceAction::Create(c) => to_value(c),
            FenceAction::Settle(s) => to_value(s),
        };
        let r = self
            .call_as::<FenceData>(
                "task.fence",
                json!({
                    "id": id,
                    "version": version,
                    "action": { "kind": action.kind(), "head": {}, "data": data },
                }),
            )
            .await?;
        Ok(r.map(|d| {
            let (promise, message) = match d.action.data {
                Value::String(m) => (None, Some(m)),
                other => (
                    serde_json::from_value::<PromiseData>(other)
                        .ok()
                        .map(|p| p.promise),
                    None,
                ),
            };
            Fenced {
                action: FenceOutcome {
                    kind: d.action.kind,
                    status: d.action.head.status,
                    promise,
                    message,
                },
                preload: d.preload,
            }
        }))
    }

    /// `task.heartbeat`: extend the leases `pid` holds on `tasks`
    /// (`(id, version)` pairs, all of one origin). Leases that are not held
    /// are skipped silently; the answer is `200` regardless.
    pub async fn task_heartbeat(
        &self,
        pid: &str,
        tasks: &[(&str, i64)],
    ) -> Result<Response<()>, Error> {
        let tasks: Vec<Value> = tasks
            .iter()
            .map(|(id, version)| json!({ "id": id, "version": version }))
            .collect();
        self.call_unit("task.heartbeat", json!({ "pid": pid, "tasks": tasks }))
            .await
    }

    /// `task.halt`: stop dispatching a task until it is continued. `409` if
    /// it is fulfilled; `200` if it is already halted.
    pub async fn task_halt(&self, id: &str) -> Result<Response<()>, Error> {
        self.call_unit("task.halt", json!({ "id": id })).await
    }

    /// `task.continue`: dispatch a halted task again. `409` if it is not
    /// halted.
    pub async fn task_continue(&self, id: &str) -> Result<Response<()>, Error> {
        self.call_unit("task.continue", json!({ "id": id })).await
    }

    /// `task.search`: one page of tasks.
    pub async fn task_search(
        &self,
        search: &TaskSearch,
    ) -> Result<Response<Page<TaskRecord>>, Error> {
        let r = self
            .call_as::<TaskPage>("task.search", to_value(search))
            .await?;
        Ok(r.map(|p| Page {
            items: p.tasks,
            cursor: p.cursor,
        }))
    }

    // --- schedule.* ---

    /// `schedule.get`. `404` if there is none.
    pub async fn schedule_get(&self, id: &str) -> Result<Response<ScheduleRecord>, Error> {
        let r = self
            .call_as::<ScheduleData>("schedule.get", json!({ "id": id }))
            .await?;
        Ok(r.map(|d| d.schedule))
    }

    /// `schedule.create`: create a schedule, or answer the existing one.
    pub async fn schedule_create(
        &self,
        create: &ScheduleCreate,
    ) -> Result<Response<ScheduleRecord>, Error> {
        let r = self
            .call_as::<ScheduleData>("schedule.create", to_value(create))
            .await?;
        Ok(r.map(|d| d.schedule))
    }

    /// `schedule.delete`. `404` if there is none.
    pub async fn schedule_delete(&self, id: &str) -> Result<Response<()>, Error> {
        self.call_unit("schedule.delete", json!({ "id": id })).await
    }

    /// `schedule.search`: one page of schedules.
    pub async fn schedule_search(
        &self,
        search: &ScheduleSearch,
    ) -> Result<Response<Page<ScheduleRecord>>, Error> {
        let r = self
            .call_as::<SchedulePage>("schedule.search", to_value(search))
            .await?;
        Ok(r.map(|p| Page {
            items: p.schedules,
            cursor: p.cursor,
        }))
    }

    // --- debug.* (servers started in debug mode only) ---

    /// `debug.reset`: forget everything. A server not in debug mode refuses.
    pub async fn debug_reset(&self) -> Result<Response<()>, Error> {
        self.call_unit("debug.reset", json!({})).await
    }

    /// `debug.snap`: the server's whole state, as it reports it.
    pub async fn debug_snap(&self) -> Result<Response<Value>, Error> {
        self.call_as("debug.snap", json!({})).await
    }

    /// `debug.tick`: run every deadline due at `time`. When the client has a
    /// debug clock, it must read `time` for this call (the server requires
    /// `resonate:debug_time` to equal `data.time`).
    pub async fn debug_tick(&self, time: i64) -> Result<Response<Value>, Error> {
        self.call_as("debug.tick", json!({ "time": time })).await
    }
}

fn decode<W: DeserializeOwned>(kind: &str, raw: Raw, attempts: u32) -> Result<Response<W>, Error> {
    let data = serde_json::from_value::<W>(raw.data).map_err(|e| Error::Protocol {
        kind: kind.to_string(),
        message: format!("cannot decode a {} answer: {e}", raw.status),
    })?;
    Ok(Response {
        status: raw.status,
        corr_id: raw.corr_id,
        data,
        attempts,
    })
}
