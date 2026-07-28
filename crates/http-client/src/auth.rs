//! Pluggable authentication for a self-authenticating [`HttpClient`].
//!
//! A [`TokenSource`] yields a bearer token that the client injects on every
//! request; attach one with [`Builder::auth`](crate::Builder::auth). [`OAuth2`] is
//! the built-in source.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::Mutex;

use crate::BoxFuture;
use crate::client::HttpClient;
use crate::error::{Error, Result};

/// A source of bearer tokens for an [`HttpClient`]. Implementors cache/refresh as
/// they see fit; [`token`](Self::token) is called before each request (unless the
/// request is a cache hit).
pub trait TokenSource: Send + Sync {
    fn token(&self) -> BoxFuture<'_, Result<String>>;
}

/// Builds a token request's form parameters, fresh on each refresh so credentials
/// can be read lazily (e.g. from env). An error surfaces as [`Error::Auth`].
pub type TokenParams =
    Arc<dyn Fn() -> std::result::Result<Vec<(String, String)>, BoxError> + Send + Sync>;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// An OAuth2 token endpoint: POSTs form parameters, parses `{ access_token,
/// expires_in }`, and caches the token until ~60s before expiry. Refreshes are
/// serialized, so concurrent callers share one in-flight request. Uses its own
/// unauthenticated client, so it never recurses through the client it authenticates.
pub struct OAuth2 {
    http: HttpClient,
    token_url: String,
    params: TokenParams,
    cached: Mutex<Option<Cached>>,
}

struct Cached {
    token: String,
    expires_at: Instant,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

impl OAuth2 {
    /// A token endpoint at `token_url` whose form parameters are produced by
    /// `params` on each refresh. Reading credentials inside the closure keeps
    /// construction infallible — a missing one surfaces as [`Error::Auth`] on the
    /// first call, not at startup.
    ///
    /// ```ignore
    /// OAuth2::new("https://accounts.spotify.com/api/token", || Ok(vec![
    ///     ("grant_type".into(), "client_credentials".into()),
    ///     ("client_id".into(), std::env::var("SPOTIFY_CLIENT_ID")?),
    ///     ("client_secret".into(), std::env::var("SPOTIFY_CLIENT_SECRET")?),
    /// ]))
    /// ```
    pub fn new<F>(token_url: impl Into<String>, params: F) -> Self
    where
        F: Fn() -> std::result::Result<Vec<(String, String)>, BoxError> + Send + Sync + 'static,
    {
        OAuth2 {
            http: HttpClient::default(),
            token_url: token_url.into(),
            params: Arc::new(params),
            cached: Mutex::new(None),
        }
    }
}

impl TokenSource for OAuth2 {
    fn token(&self) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move {
            let mut cached = self.cached.lock().await;
            if let Some(current) = cached.as_ref()
                && current.expires_at.saturating_duration_since(Instant::now())
                    > Duration::from_secs(60)
            {
                return Ok(current.token.clone());
            }

            let params = (self.params)().map_err(|e| Error::Auth(e.to_string()))?;
            let request = self.http.post(&self.token_url).form(&params);
            let response: TokenResponse = self.http.send_json(request).await?;

            let token = response.access_token.clone();
            // 60s safety margin so we never serve a token that expires mid-request.
            *cached = Some(Cached {
                token: response.access_token,
                expires_at: Instant::now()
                    + Duration::from_secs(response.expires_in.saturating_sub(60)),
            });
            Ok(token)
        })
    }
}
