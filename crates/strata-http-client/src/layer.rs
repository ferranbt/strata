//! Composable request layers — a tower-free take on alloy's `Layer`/`Service`
//! stack.
//!
//! The base [`HttpService`] performs the network call and status handling;
//! [`Layer`]s wrap it. [`RetryBackoffLayer`] is the built-in one. Stack them with
//! [`Builder::layer`](crate::Builder::layer).

use std::sync::Arc;
use std::time::Duration;

use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue, RETRY_AFTER};
use reqwest::{Request, Response, StatusCode};

use crate::BoxFuture;
use crate::auth::TokenSource;
use crate::error::{Error, Result};

/// A request-executing service — the unit that [`Layer`]s wrap. Returns a checked
/// (2xx) [`Response`] or a mapped [`Error`].
pub trait HttpService: Send + Sync {
    fn call(&self, request: Request) -> BoxFuture<'_, Result<Response>>;
}

/// Wraps an [`HttpService`] to add behavior. Composed by
/// [`Builder::layer`](crate::Builder::layer), the last added being outermost.
pub trait Layer: Send + Sync {
    fn layer(&self, inner: Arc<dyn HttpService>) -> Arc<dyn HttpService>;
}

/// The base service: execute the request and map failures to the [`Error`] set
/// (`429` → [`Error::RateLimited`], other non-2xx → [`Error::Status`]).
pub(crate) struct Execute {
    pub(crate) client: reqwest::Client,
}

impl HttpService for Execute {
    fn call(&self, request: Request) -> BoxFuture<'_, Result<Response>> {
        let client = self.client.clone();
        Box::pin(async move {
            let url = request.url().to_string();
            let response = client
                .execute(request)
                .await
                .map_err(|source| Error::Transport { url: url.clone(), source })?;
            let status = response.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                return Err(Error::RateLimited { url, retry_after: retry_after(response.headers()) });
            }
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                return Err(Error::Status { status: status.as_u16(), url, body });
            }
            Ok(response)
        })
    }
}

/// Attaches a fresh `Authorization: Bearer <token>` (from a [`TokenSource`]) to
/// each request. Composed innermost, so a retry layer above re-runs the token
/// fetch per attempt, refreshing an expired token.
pub(crate) struct AuthLayer {
    pub(crate) source: Arc<dyn TokenSource>,
}

impl Layer for AuthLayer {
    fn layer(&self, inner: Arc<dyn HttpService>) -> Arc<dyn HttpService> {
        Arc::new(AuthService { inner, source: self.source.clone() })
    }
}

struct AuthService {
    inner: Arc<dyn HttpService>,
    source: Arc<dyn TokenSource>,
}

impl HttpService for AuthService {
    fn call(&self, request: Request) -> BoxFuture<'_, Result<Response>> {
        Box::pin(async move {
            let mut request = request;
            let token = self.source.token().await?;
            if let Ok(mut value) = HeaderValue::from_str(&format!("Bearer {token}")) {
                value.set_sensitive(true);
                request.headers_mut().insert(AUTHORIZATION, value);
            }
            self.inner.call(request).await
        })
    }
}

/// Where an [`ApiTokenLayer`]'s value comes from.
#[derive(Debug, Clone)]
pub(crate) enum TokenValue {
    /// A literal value.
    Literal(String),
    /// Read from this environment variable at request time.
    Env(String),
}

impl TokenValue {
    fn resolve(&self) -> Result<String> {
        match self {
            TokenValue::Literal(value) => Ok(value.clone()),
            TokenValue::Env(var) => {
                std::env::var(var).map_err(|_| Error::Auth(format!("missing env var `{var}`")))
            }
        }
    }
}

/// Adds an API token as a query parameter to each request — the dual of
/// [`AuthLayer`] for APIs that authenticate by query string. Runs after the cache
/// key is computed, so the token never leaks into a cassette.
pub(crate) struct ApiTokenLayer {
    pub(crate) param: String,
    pub(crate) value: TokenValue,
}

impl Layer for ApiTokenLayer {
    fn layer(&self, inner: Arc<dyn HttpService>) -> Arc<dyn HttpService> {
        Arc::new(ApiTokenService { inner, param: self.param.clone(), value: self.value.clone() })
    }
}

struct ApiTokenService {
    inner: Arc<dyn HttpService>,
    param: String,
    value: TokenValue,
}

impl HttpService for ApiTokenService {
    fn call(&self, request: Request) -> BoxFuture<'_, Result<Response>> {
        Box::pin(async move {
            let mut request = request;
            let token = self.value.resolve()?;
            request.url_mut().query_pairs_mut().append_pair(&self.param, &token);
            self.inner.call(request).await
        })
    }
}

/// Decides which errors are retried and any server-suggested backoff.
pub trait RetryPolicy: Send + Sync {
    fn should_retry(&self, error: &Error) -> bool;
    /// A server-suggested backoff (e.g. from `Retry-After`), if any.
    fn backoff_hint(&self, error: &Error) -> Option<Duration>;
}

/// The default: retry rate limits (`429`), server errors (`5xx`), and transport
/// failures; honor a `Retry-After` on rate limits.
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultRetryPolicy;

impl RetryPolicy for DefaultRetryPolicy {
    fn should_retry(&self, error: &Error) -> bool {
        match error {
            Error::RateLimited { .. } | Error::Transport { .. } => true,
            Error::Status { status, .. } => *status >= 500,
            _ => false,
        }
    }

    fn backoff_hint(&self, error: &Error) -> Option<Duration> {
        match error {
            Error::RateLimited { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// Retries failed requests with exponential backoff, honoring a server
/// `Retry-After`.
pub struct RetryBackoffLayer<P = DefaultRetryPolicy> {
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    policy: P,
}

impl RetryBackoffLayer {
    /// Up to `max_retries` retries, starting at `initial_backoff` and doubling
    /// each attempt (capped at 30s), with the [`DefaultRetryPolicy`].
    pub fn new(max_retries: u32, initial_backoff: Duration) -> Self {
        RetryBackoffLayer {
            max_retries,
            initial_backoff,
            max_backoff: Duration::from_secs(30),
            policy: DefaultRetryPolicy,
        }
    }

    /// Cap the per-attempt backoff (default 30s).
    pub fn with_max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }
}

impl Default for RetryBackoffLayer {
    fn default() -> Self {
        RetryBackoffLayer::new(3, Duration::from_millis(250))
    }
}

impl<P: RetryPolicy> RetryBackoffLayer<P> {
    /// Like [`new`](Self::new) but with a custom [`RetryPolicy`].
    pub fn with_policy(max_retries: u32, initial_backoff: Duration, policy: P) -> Self {
        RetryBackoffLayer {
            max_retries,
            initial_backoff,
            max_backoff: Duration::from_secs(30),
            policy,
        }
    }
}

impl<P: RetryPolicy + Clone + 'static> Layer for RetryBackoffLayer<P> {
    fn layer(&self, inner: Arc<dyn HttpService>) -> Arc<dyn HttpService> {
        Arc::new(RetryBackoff {
            inner,
            max_retries: self.max_retries,
            initial_backoff: self.initial_backoff,
            max_backoff: self.max_backoff,
            policy: self.policy.clone(),
        })
    }
}

struct RetryBackoff<P> {
    inner: Arc<dyn HttpService>,
    max_retries: u32,
    initial_backoff: Duration,
    max_backoff: Duration,
    policy: P,
}

impl<P: RetryPolicy> HttpService for RetryBackoff<P> {
    fn call(&self, request: Request) -> BoxFuture<'_, Result<Response>> {
        Box::pin(async move {
            let mut attempt: u32 = 0;
            loop {
                // Clone the request per attempt; a non-cloneable (streaming) body
                // can't be retried, so run it once and return whatever we get.
                let Some(attempt_request) = request.try_clone() else {
                    return self.inner.call(request).await;
                };
                match self.inner.call(attempt_request).await {
                    Ok(response) => return Ok(response),
                    Err(error) => {
                        if attempt >= self.max_retries || !self.policy.should_retry(&error) {
                            return Err(error);
                        }
                        let delay = self
                            .policy
                            .backoff_hint(&error)
                            .unwrap_or_else(|| exponential(self.initial_backoff, attempt))
                            .min(self.max_backoff);
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                }
            }
        })
    }
}

/// `initial * 2^attempt`, saturating so it never overflows (the caller caps it).
fn exponential(initial: Duration, attempt: u32) -> Duration {
    initial.checked_mul(1u32 << attempt.min(16)).unwrap_or(Duration::MAX)
}

/// Parse a `Retry-After` header expressed in seconds. (The HTTP-date form is
/// ignored for now — callers can read the header off the error.)
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    headers
        .get(RETRY_AFTER)?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A service that fails with `429` for its first `fail_until` calls, then
    /// returns `200`.
    struct Flaky {
        calls: Arc<AtomicU32>,
        fail_until: u32,
    }

    impl HttpService for Flaky {
        fn call(&self, _request: Request) -> BoxFuture<'_, Result<Response>> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            let fail_until = self.fail_until;
            Box::pin(async move {
                if n < fail_until {
                    Err(Error::RateLimited { url: "http://x/".into(), retry_after: None })
                } else {
                    let http = http::Response::builder().status(200).body("ok".to_string()).unwrap();
                    Ok(Response::from(http))
                }
            })
        }
    }

    fn request() -> Request {
        reqwest::Client::new().get("http://localhost/").build().unwrap()
    }

    #[tokio::test]
    async fn retries_then_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let inner: Arc<dyn HttpService> = Arc::new(Flaky { calls: calls.clone(), fail_until: 2 });
        let service = RetryBackoffLayer::new(5, Duration::from_millis(1)).layer(inner);

        let response = service.call(request()).await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(calls.load(Ordering::SeqCst), 3); // 2 failures + 1 success
    }

    #[tokio::test]
    async fn gives_up_after_max_retries() {
        let calls = Arc::new(AtomicU32::new(0));
        let inner: Arc<dyn HttpService> = Arc::new(Flaky { calls: calls.clone(), fail_until: 100 });
        let service = RetryBackoffLayer::new(2, Duration::from_millis(1)).layer(inner);

        let error = service.call(request()).await.unwrap_err();
        assert!(matches!(error, Error::RateLimited { .. }));
        assert_eq!(calls.load(Ordering::SeqCst), 3); // initial + 2 retries
    }
}
