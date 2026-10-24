//! The auth hook: where each request's token comes from.

use async_trait::async_trait;

/// Supplies the bearer token for one request.
///
/// Called once per attempt, so a source that mints short-lived tokens is asked
/// again on a retry rather than replaying a token that may have expired. Over
/// HTTP the token goes in `Authorization: Bearer <token>`; in process it goes
/// in `head.auth`, which is where the HTTP gateway would have put it.
///
/// `Ok(None)` sends the request without a token. `Err` stops the call before
/// anything is sent ([`Error::Auth`](crate::Error::Auth)).
///
/// A plain closure `Fn() -> Option<String>` is a source; implement the trait
/// for anything that has to await (an identity provider, a token cache).
#[async_trait]
pub trait TokenSource: Send + Sync {
    async fn token(&self) -> Result<Option<String>, String>;
}

/// The same token on every request.
#[derive(Clone)]
pub struct StaticToken(pub String);

impl std::fmt::Debug for StaticToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print a credential.
        f.write_str("StaticToken(..)")
    }
}

#[async_trait]
impl TokenSource for StaticToken {
    async fn token(&self) -> Result<Option<String>, String> {
        Ok(Some(self.0.clone()))
    }
}

#[async_trait]
impl<F> TokenSource for F
where
    F: Fn() -> Option<String> + Send + Sync,
{
    async fn token(&self) -> Result<Option<String>, String> {
        Ok(self())
    }
}
