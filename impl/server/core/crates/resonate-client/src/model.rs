//! What the typed methods send and what they hand back.
//!
//! The records — [`PromiseRecord`], [`TaskRecord`], [`ScheduleRecord`] — and
//! the states are `resonate-core`'s, so a value read here is the same type a
//! server built. The request bodies are this crate's own: `core`'s `*Data`
//! structs are the server's parsing side (they deserialize and validate), and
//! several cannot be serialized at all. The round-trip tests send every body
//! here to a real server, which parses and validates it with `core`'s structs.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

pub use resonate_core::types::{
    PromiseRecord, PromiseState, PromiseValue, ScheduleRecord, SettleState, TaskRecord, TaskState,
};

/// A completed exchange the server answered with success (`2xx`, or `300`
/// for a suspend that found an awaited promise already settled).
///
/// Everything else the server can say — not found, conflict, forbidden — is
/// [`Error::Rejected`](crate::Error::Rejected), with its status.
#[derive(Debug, Clone, PartialEq)]
pub struct Response<T> {
    /// `head.status` as the server sent it.
    pub status: u16,
    /// The correlation id the request carried, echoed by the server.
    pub corr_id: String,
    /// The typed payload.
    pub data: T,
    /// How many attempts the call took (see [`RetryPolicy`](crate::RetryPolicy)).
    pub attempts: u32,
}

impl<T> Response<T> {
    /// The same response with its payload mapped.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Response<U> {
        Response {
            status: self.status,
            corr_id: self.corr_id,
            data: f(self.data),
            attempts: self.attempts,
        }
    }
}

// --- Requests ---

/// `promise.create`, and the `promise.create` inside `task.create` and
/// `task.fence`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PromiseCreate {
    pub id: String,
    /// Absolute deadline, in milliseconds since the epoch.
    #[serde(rename = "timeoutAt")]
    pub timeout_at: i64,
    pub param: PromiseValue,
    pub tags: HashMap<String, String>,
}

impl PromiseCreate {
    /// A promise `id` that times out at `timeout_at`, with no param and no tags.
    pub fn new(id: impl Into<String>, timeout_at: i64) -> Self {
        Self {
            id: id.into(),
            timeout_at,
            ..Default::default()
        }
    }

    /// Set the param's data.
    pub fn data(mut self, data: impl Into<String>) -> Self {
        self.param.data = Some(data.into());
        self
    }

    /// Set the whole param.
    pub fn param(mut self, param: PromiseValue) -> Self {
        self.param = param;
        self
    }

    /// Add one tag.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// `resonate:target`: the address a task for this promise is dispatched to.
    pub fn target(self, address: impl Into<String>) -> Self {
        self.tag("resonate:target", address)
    }
}

/// `promise.settle`, and the settle inside `task.fence`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PromiseSettle {
    pub id: String,
    pub state: SettleState,
    pub value: PromiseValue,
}

impl PromiseSettle {
    /// Settle `id` to `state` with an empty value.
    pub fn new(id: impl Into<String>, state: SettleState) -> Self {
        Self {
            id: id.into(),
            state,
            value: PromiseValue::default(),
        }
    }

    /// Set the value's data.
    pub fn data(mut self, data: impl Into<String>) -> Self {
        self.value.data = Some(data.into());
        self
    }

    /// Set the whole value.
    pub fn value(mut self, value: PromiseValue) -> Self {
        self.value = value;
        self
    }
}

/// `promise.search`. Every field is optional; a page holds at most `limit`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PromiseSearch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<PromiseState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// `task.search`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct TaskSearch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<TaskState>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// `schedule.search`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScheduleSearch {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tags: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// `schedule.create`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScheduleCreate {
    pub id: String,
    /// Five fields (minute first) or six (second first).
    pub cron: String,
    /// The template for each run's promise id; `{{.timestamp}}` and
    /// `{{.id}}` are filled in by the server.
    #[serde(rename = "promiseId")]
    pub promise_id: String,
    /// Each run's promise times out this many milliseconds after it is created.
    #[serde(rename = "promiseTimeout")]
    pub promise_timeout: i64,
    #[serde(rename = "promiseParam")]
    pub promise_param: PromiseValue,
    /// Must carry `resonate:target`.
    #[serde(rename = "promiseTags")]
    pub promise_tags: HashMap<String, String>,
}

impl ScheduleCreate {
    /// A schedule `id` on `cron` that creates `promise_id` with a timeout of
    /// `promise_timeout` ms and dispatches it to `target`.
    pub fn new(
        id: impl Into<String>,
        cron: impl Into<String>,
        promise_id: impl Into<String>,
        promise_timeout: i64,
        target: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            cron: cron.into(),
            promise_id: promise_id.into(),
            promise_timeout,
            promise_param: PromiseValue::default(),
            promise_tags: HashMap::from([("resonate:target".to_string(), target.into())]),
        }
    }

    /// Add one tag to every run's promise.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.promise_tags.insert(key.into(), value.into());
        self
    }
}

/// What a `task.fence` performs while the task's lease is held.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceAction {
    Create(PromiseCreate),
    Settle(PromiseSettle),
}

impl FenceAction {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            FenceAction::Create(_) => "promise.create",
            FenceAction::Settle(_) => "promise.settle",
        }
    }
}

// --- Responses ---

/// One page of a search, and the cursor for the next when there is one.
#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub cursor: Option<String>,
}

/// A task held under a lease: what `task.create` and `task.acquire` answer.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TaskClaim {
    pub task: TaskRecord,
    pub promise: PromiseRecord,
    /// Promises of the same origin the server sent along, so the worker does
    /// not have to ask for them.
    #[serde(default)]
    pub preload: Vec<PromiseRecord>,
}

/// What `task.suspend` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Suspension {
    /// `200`: the task is suspended until an awaited promise settles.
    Suspended,
    /// `300`: an awaited promise has already settled, so the task was not
    /// suspended and the caller keeps its lease and carries on.
    Resume { preload: Vec<PromiseRecord> },
}

/// The answer to the action inside a `task.fence`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceOutcome {
    /// The action's kind, `promise.create` or `promise.settle`.
    pub kind: String,
    /// The action's own status.
    pub status: u16,
    /// The promise, when the action answered with one.
    pub promise: Option<PromiseRecord>,
    /// The message, when the action answered with an error.
    pub message: Option<String>,
}

/// What `task.fence` answers: the action's outcome and a preload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fenced {
    pub action: FenceOutcome,
    pub preload: Vec<PromiseRecord>,
}

// Wire shapes the typed methods decode through.

#[derive(Deserialize)]
pub(crate) struct PromiseData {
    pub promise: PromiseRecord,
}

#[derive(Deserialize)]
pub(crate) struct TaskData {
    pub task: TaskRecord,
}

#[derive(Deserialize)]
pub(crate) struct ScheduleData {
    pub schedule: ScheduleRecord,
}

#[derive(Deserialize)]
pub(crate) struct PromisePage {
    pub promises: Vec<PromiseRecord>,
    pub cursor: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct TaskPage {
    pub tasks: Vec<TaskRecord>,
    pub cursor: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct SchedulePage {
    pub schedules: Vec<ScheduleRecord>,
    pub cursor: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct PreloadData {
    #[serde(default)]
    pub preload: Vec<PromiseRecord>,
}

#[derive(Deserialize)]
pub(crate) struct FenceData {
    pub action: FenceActionEnvelope,
    #[serde(default)]
    pub preload: Vec<PromiseRecord>,
}

#[derive(Deserialize)]
pub(crate) struct FenceActionEnvelope {
    pub kind: String,
    pub head: FenceActionHead,
    pub data: serde_json::Value,
}

#[derive(Deserialize)]
pub(crate) struct FenceActionHead {
    pub status: u16,
}
