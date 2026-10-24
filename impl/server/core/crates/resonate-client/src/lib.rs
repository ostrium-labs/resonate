//! A typed client for the Resonate protocol.
//!
//! [`Client`] sends every protocol request — `promise.*`, `task.*`,
//! `schedule.*` (and `debug.*` for servers in debug mode) — as a typed method,
//! and decodes the answer into `resonate-core`'s records. It has two
//! transports behind the one type:
//!
//! - [`Client::http`]: `POST` of one envelope to a gateway's protocol
//!   endpoint, with per-attempt timeouts and retries.
//! - [`Client::local`]: a call on an `Arc<dyn ResonateServer>` in this
//!   process, with the same retries, auth, ids and observation.
//!
//! `Client` is itself a [`ResonateServer`], so it can stand wherever the port
//! is taken.
//!
//! ```no_run
//! use resonate_client::{Client, PromiseCreate, PromiseSettle, SettleState};
//!
//! # async fn run() -> Result<(), resonate_client::Error> {
//! let client = Client::http("http://localhost:8001/")
//!     .auth(|| Some(mint_token()))
//!     .build()?;
//!
//! let created = client
//!     .promise_create(&PromiseCreate::new("order-42", i64::MAX).data("e30="))
//!     .await?;
//! assert_eq!(created.status, 200);
//!
//! match client.promise_get("order-41").await {
//!     Err(e) if e.is_not_found() => { /* never created */ }
//!     other => { other?; }
//! }
//!
//! client
//!     .promise_settle(&PromiseSettle::new("order-42", SettleState::Resolved))
//!     .await?;
//! # Ok(()) }
//! # fn mint_token() -> String { String::new() }
//! ```
//!
//! # Answers
//!
//! A typed method returns [`Response`] for a success — `200`, or `300` for a
//! `task.suspend` that found an awaited promise already settled — and
//! [`Error::Rejected`] with the status for anything else the server says.
//! [`Error::Unavailable`] means there was no answer within the
//! [`RetryPolicy`], and that the request may have been applied anyway.
//!
//! # Retries
//!
//! Connection failures, timeouts, `503`, and a proxy's bare `502`/`504` are
//! retried with jittered exponential backoff, bounded by attempts and a
//! deadline. See [`RetryPolicy`] for which requests that is harmless for.

mod api;
mod auth;
mod client;
mod corr;
mod error;
mod model;
mod retry;
pub mod trace;
mod transport;

pub use api::KINDS;
pub use auth::{StaticToken, TokenSource};
pub use client::{Builder, Client};
pub use corr::{CorrIds, Sequential};
pub use error::Error;
pub use model::*;
pub use retry::RetryPolicy;

pub use resonate_core::ResonateServer;
