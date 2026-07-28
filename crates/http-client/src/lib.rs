//! strata's shared HTTP client.
//!
//! Providers route every API request through [`HttpClient::send`] rather than a
//! bare `reqwest::Client`, so cross-cutting concerns — the uniform [`Error`] set,
//! response caching, auth, retry/backoff — live in one place. Requests are built
//! with the re-exported reqwest [`RequestBuilder`], but handed to
//! [`send`](HttpClient::send)/[`send_json`](HttpClient::send_json) rather than sent
//! directly.
//!
//! ```ignore
//! let http = HttpClient::builder().user_agent("strata/0.1").build()?;
//! let req = http.get("https://api.example.com/things").query(&[("limit", "20")]);
//! let things: Things = http.send_json(req).await?;
//! ```

mod auth;
mod client;
mod error;
mod layer;

pub use auth::{OAuth2, TokenParams, TokenSource};
pub use client::{Builder, HttpClient};
pub use error::{Error, Result};
pub use layer::{DefaultRetryPolicy, HttpService, Layer, RetryBackoffLayer, RetryPolicy};

// The reqwest surface providers need, so they don't depend on reqwest directly.
pub use reqwest::{self, IntoUrl, RequestBuilder, Response, StatusCode, header};

/// A boxed, owned future — keeps [`TokenSource`] object-safe.
pub type BoxFuture<'a, T> = std::pin::Pin<Box<dyn std::future::Future<Output = T> + Send + 'a>>;

/// Decode the array an API nests under `key` (`{"items": [...]}`, `{"bills":
/// [...]}`, …). A missing key decodes as empty — how these APIs report no results.
pub fn decode_envelope<T: serde::de::DeserializeOwned>(
    body: &serde_json::Value,
    key: &str,
) -> serde_json::Result<Vec<T>> {
    match body.get(key) {
        Some(items) => serde_json::from_value(items.clone()),
        None => Ok(Vec::new()),
    }
}
