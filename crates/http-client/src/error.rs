//! The error set every call through [`HttpClient`](crate::HttpClient) returns, so
//! providers match one uniform set instead of raw `reqwest` errors. The status
//! code is kept as a number ([`status`](Error::status)) and `429` is split out
//! carrying `Retry-After`.

use std::time::Duration;

/// What a request can fail with.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The server returned a non-2xx status (other than 429). `status` is the raw
    /// HTTP code and `body` the response text, for diagnostics.
    #[error("HTTP {status} from {url}: {body}")]
    Status {
        status: u16,
        url: String,
        body: String,
    },

    /// Too Many Requests (429), with the server's `Retry-After` when supplied so a
    /// rate-limit/backoff layer can honor it.
    #[error("rate limited by {url} (retry_after: {retry_after:?})")]
    RateLimited {
        url: String,
        retry_after: Option<Duration>,
    },

    /// The request never completed (DNS, TLS, connect, timeout).
    #[error("request to {url} failed: {source}")]
    Transport {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// The response body didn't decode into the expected type.
    #[error("decoding response from {url}: {source}")]
    Decode {
        url: String,
        #[source]
        source: reqwest::Error,
    },

    /// The client or a request couldn't be constructed (bad header, bad URL).
    #[error("building HTTP client/request: {0}")]
    Build(#[source] reqwest::Error),

    /// Reading or writing the local response cache failed (record/replay mode).
    #[error("response cache: {0}")]
    Cache(String),

    /// Acquiring a bearer token failed (missing credentials, or the token
    /// endpoint rejected the request) — see [`TokenSource`](crate::TokenSource).
    #[error("authenticating: {0}")]
    Auth(String),
}

impl Error {
    /// The HTTP status code, when the error came from a server response
    /// (`Status` or `RateLimited`). `None` for transport/decode/build failures.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Status { status, .. } => Some(*status),
            Error::RateLimited { .. } => Some(429),
            _ => None,
        }
    }

    /// Whether the server answered `404 Not Found`.
    pub fn is_not_found(&self) -> bool {
        self.status() == Some(404)
    }

    /// Whether the failure was a rate limit (429).
    pub fn is_rate_limited(&self) -> bool {
        matches!(self, Error::RateLimited { .. })
    }
}

/// `Result` specialized to this crate's [`Error`].
pub type Result<T> = std::result::Result<T, Error>;
