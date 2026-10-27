//! What can go wrong with a call.

use std::fmt;

/// Why a call did not produce a typed response.
///
/// Two of these are answers and the rest are the absence of one, and the
/// difference matters to a caller: [`Error::Rejected`] is the server saying
/// something definite about the request (it is not found, it conflicts, it is
/// forbidden), while [`Error::Unavailable`] is the client giving up on getting
/// an answer at all — after which the request **may already have been
/// applied** (see [`resonate_core::Unavailable`] for that contract).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// The server answered with a status this method does not treat as
    /// success: `400`, `401`, `403`, `404`, `409`, `422`, `500`, …
    Rejected {
        kind: String,
        corr_id: String,
        status: u16,
        /// The server's message: a non-2xx answer carries `data` as a string.
        message: String,
        /// How many attempts the call took. Above 1, an earlier attempt may
        /// have been applied, and this answer may be about its effect (see
        /// [`RetryPolicy`](crate::RetryPolicy)).
        attempts: u32,
    },
    /// No answer within the retry policy: every attempt failed to connect,
    /// timed out, or was answered `503`.
    Unavailable {
        kind: String,
        corr_id: String,
        attempts: u32,
        /// What the last attempt saw.
        message: String,
    },
    /// The token source failed, so no request was sent.
    Auth(String),
    /// The peer answered with something that is not a protocol response for
    /// this request: not an envelope, a mismatched `kind` or `corrId`, or a
    /// success payload of the wrong shape.
    Protocol { kind: String, message: String },
    /// The client could not be built (a bad URL, a TLS backend error), or a
    /// poll subscription could not be (an empty group or id).
    Build(String),
    /// This client cannot do that over its transport: polling over
    /// [`Client::local`](crate::Client::local).
    Unsupported(String),
}

impl Error {
    /// The protocol status, when the server answered with one.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Rejected { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// `404`: the thing named does not exist.
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(404)
    }

    /// `409`: the request conflicts with the current state — a task that is
    /// not in the state or version the request assumed.
    pub fn is_conflict(&self) -> bool {
        self.status() == Some(409)
    }

    /// `422`: the request is well formed but the state it names cannot
    /// accept it — an awaited promise that is not awaitable, say.
    pub fn is_unprocessable(&self) -> bool {
        self.status() == Some(422)
    }

    /// No answer was obtained; the request may or may not have been applied.
    pub fn is_unavailable(&self) -> bool {
        matches!(self, Error::Unavailable { .. })
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Rejected {
                kind,
                corr_id,
                status,
                message,
                ..
            } => write!(f, "{kind} ({corr_id}) answered {status}: {message}"),
            Error::Unavailable {
                kind,
                corr_id,
                attempts,
                message,
            } => write!(
                f,
                "{kind} ({corr_id}) unavailable after {attempts} attempt(s): {message}"
            ),
            Error::Auth(m) => write!(f, "token source failed: {m}"),
            Error::Protocol { kind, message } => write!(f, "{kind}: bad answer: {message}"),
            Error::Build(m) => write!(f, "cannot build client: {m}"),
            Error::Unsupported(m) => write!(f, "unsupported: {m}"),
        }
    }
}

impl std::error::Error for Error {}
