//! The client wrapper and its builder.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, RequestBuilder, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::auth::TokenSource;
use crate::error::{Error, Result};
use crate::layer::{ApiTokenLayer, AuthLayer, Execute, HttpService, Layer, TokenValue};

/// A wrapper over [`reqwest::Client`] that funnels every request through
/// [`send`](Self::send), where status handling, the [`Error`] set, the response
/// cache, and the layer stack apply. Build a request with
/// [`get`](Self::get)/[`post`](Self::post), then hand it to [`send`](Self::send)
/// or [`send_json`](Self::send_json) — calling `RequestBuilder::send` directly
/// bypasses all of that.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    cache: Option<PathBuf>,
    base_url: Option<String>,
    /// Applied on the `RequestBuilder` (not as a layer) so they're part of the
    /// request identity and participate in the cache key.
    query: Arc<Vec<(String, String)>>,
    /// The base network call wrapped by any [`Layer`]s (auth, api token, retry).
    service: Arc<dyn HttpService>,
}

impl HttpClient {
    /// Start configuring a client.
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// A client with default settings.
    pub fn new() -> Result<Self> {
        Builder::default().build()
    }

    /// Begin a `GET` request. With a [`base_url`](Builder::base_url) set, `target`
    /// is a path appended to it (e.g. `/bill/119/hr`); otherwise it's the full URL.
    pub fn get(&self, target: impl AsRef<str>) -> RequestBuilder {
        self.with_query(self.inner.get(self.resolve(target.as_ref())))
    }

    /// Begin a `POST` request. `target` is resolved like [`get`](Self::get).
    pub fn post(&self, target: impl AsRef<str>) -> RequestBuilder {
        self.with_query(self.inner.post(self.resolve(target.as_ref())))
    }

    fn resolve(&self, target: &str) -> String {
        match &self.base_url {
            Some(base) => format!("{base}{target}"),
            None => target.to_string(),
        }
    }

    fn with_query(&self, request: RequestBuilder) -> RequestBuilder {
        if self.query.is_empty() {
            request
        } else {
            request.query(self.query.as_ref())
        }
    }

    /// Execute `request`, mapping failures to the [`Error`] set (transport, `429`
    /// → [`Error::RateLimited`], other non-2xx → [`Error::Status`]) and applying
    /// the response cache when [enabled](Builder::cache).
    pub async fn send(&self, request: RequestBuilder) -> Result<Response> {
        // Key the cache off a probe clone *before* auth/api-token layers run, so a
        // hit short-circuits early and secrets never reach the key or recorded url.
        let cache_target = match &self.cache {
            Some(dir) => {
                let probe = request
                    .try_clone()
                    .ok_or_else(|| {
                        Error::Cache("request body is not cloneable for caching".into())
                    })?
                    .build()
                    .map_err(Error::Build)?;
                let cache_url = probe.url().to_string();
                let path = dir.join(format!("{}.json", cache_key(probe.method(), &cache_url)));
                if path.exists() {
                    return read_cache(&path);
                }
                Some((path, cache_url))
            }
            None => None,
        };

        let request = request.build().map_err(Error::Build)?;
        let response = self.service.call(request).await?;

        // Only successes reach here, so record then replay an equivalent response.
        if let Some((path, cache_url)) = &cache_target {
            let code = response.status().as_u16();
            let body = response.text().await.map_err(|source| Error::Transport {
                url: cache_url.clone(),
                source,
            })?;
            write_cache(path, cache_url, code, &body)?;
            return build_response(code, body);
        }
        Ok(response)
    }

    /// [`send`](Self::send), then decode the JSON body into `T`, also returning the
    /// response headers (for callers that page off a header, e.g. `Link`).
    pub async fn send_json2<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
    ) -> Result<ResultExtended<T>> {
        let response = self.send(request).await?;
        let headers = response.headers().clone();

        let url = response.url().to_string();
        let body = response
            .json::<T>()
            .await
            .map_err(|source| Error::Decode { url, source })?;

        Ok(ResultExtended { body, headers })
    }

    /// [`send`](Self::send), then decode the JSON body into `T`.
    pub async fn send_json<T: DeserializeOwned>(&self, request: RequestBuilder) -> Result<T> {
        let response = self.send(request).await?;
        let url = response.url().to_string();
        response
            .json::<T>()
            .await
            .map_err(|source| Error::Decode { url, source })
    }
}

pub struct ResultExtended<T> {
    pub body: T,
    pub headers: HeaderMap,
}

impl Default for HttpClient {
    /// A client with no shared settings — infallible, mirroring
    /// `reqwest::Client::new()`.
    fn default() -> Self {
        let inner = reqwest::Client::new();
        let service = Arc::new(Execute {
            client: inner.clone(),
        });
        HttpClient {
            inner,
            cache: None,
            base_url: None,
            query: Arc::new(Vec::new()),
            service,
        }
    }
}

impl std::fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpClient")
            .field("cache", &self.cache)
            .finish_non_exhaustive()
    }
}

/// Builder for an [`HttpClient`]: shared per-request settings, auth, the response
/// cache, and the layer stack.
#[derive(Default)]
pub struct Builder {
    user_agent: Option<String>,
    headers: HeaderMap,
    timeout: Option<Duration>,
    cache: Option<PathBuf>,
    no_cache: bool,
    base_url: Option<String>,
    query: Vec<(String, String)>,
    auth: Option<Arc<dyn TokenSource>>,
    api_token: Option<(String, TokenValue)>,
    layers: Vec<Box<dyn Layer>>,
}

impl Builder {
    /// Set the `User-Agent` sent on every request (some APIs require one).
    pub fn user_agent(mut self, user_agent: impl Into<String>) -> Self {
        self.user_agent = Some(user_agent.into());
        self
    }

    /// Set a default header sent on every request.
    pub fn default_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.headers.insert(name, value);
        self
    }

    /// Set a base URL that [`get`](HttpClient::get)/[`post`](HttpClient::post)
    /// targets are appended to, so providers pass just the path (`/bill/119/hr`).
    /// Resolution is plain concatenation, so give `base` without a trailing slash
    /// and `target` with a leading `/`. When unset, targets are full URLs.
    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = Some(base_url.into());
        self
    }

    /// Append fixed query params to every request (e.g. a target `database`).
    /// Accumulates across calls. Unlike [`api_token`](Self::api_token) these aren't
    /// secrets, so they're part of the cache key. Provider params stack on top.
    pub fn query<K: Into<String>, V: Into<String>>(
        mut self,
        params: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        self.query
            .extend(params.into_iter().map(|(k, v)| (k.into(), v.into())));
        self
    }

    /// Set a default `Authorization: Bearer <token>` header (marked sensitive so
    /// it's redacted from logs). A malformed token is ignored.
    pub fn bearer_auth(mut self, token: &str) -> Self {
        if let Ok(mut value) = HeaderValue::from_str(&format!("Bearer {token}")) {
            value.set_sensitive(true);
            self.headers.insert(AUTHORIZATION, value);
        }
        self
    }

    /// Set a default `Authorization: Basic <base64(user:password)>` header (marked
    /// sensitive so it's redacted from logs), for APIs that authenticate by HTTP
    /// Basic (e.g. ClickHouse). Pass `None` for a passwordless user.
    pub fn basic_auth(mut self, user: &str, password: Option<&str>) -> Self {
        let credentials = format!("{user}:{}", password.unwrap_or(""));
        let encoded = STANDARD.encode(credentials);
        if let Ok(mut value) = HeaderValue::from_str(&format!("Basic {encoded}")) {
            value.set_sensitive(true);
            self.headers.insert(AUTHORIZATION, value);
        }
        self
    }

    /// Set a per-request timeout.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Enable the local response cache at `dir`: a request's successful response is
    /// written keyed by method+URL, and identical requests later replay from disk
    /// without hitting the network — how integration tests run offline.
    pub fn cache(mut self, dir: impl Into<PathBuf>) -> Self {
        self.cache = Some(dir.into());
        self
    }

    /// Opt this client out of caching entirely, overriding both
    /// [`cache`](Self::cache) and the `HTTP_CLIENT_CACHE` env fallback. For
    /// providers whose HTTP responses shouldn't be recorded/replayed (e.g.
    /// ClickHouse's query interface, which happens to speak HTTP but is a live DB).
    pub fn no_cache(mut self) -> Self {
        self.no_cache = true;
        self
    }

    /// Inject a literal API token as the `param` query parameter, for APIs that
    /// authenticate by query string (`?api_key=…`) rather than a header. Added
    /// after the cache key is computed, so it never leaks into a cassette.
    pub fn api_token(mut self, param: impl Into<String>, value: impl Into<String>) -> Self {
        self.api_token = Some((param.into(), TokenValue::Literal(value.into())));
        self
    }

    /// Like [`api_token`](Self::api_token), but the value is read from environment
    /// variable `env_var` at request time — so construction stays infallible and a
    /// missing variable surfaces as an error on the first call.
    pub fn api_token_env(mut self, param: impl Into<String>, env_var: impl Into<String>) -> Self {
        self.api_token = Some((param.into(), TokenValue::Env(env_var.into())));
        self
    }

    /// Make the client self-authenticating: `send` attaches a fresh
    /// `Authorization: Bearer <token>` from `source` to every request. See
    /// [`OAuth2`](crate::OAuth2).
    pub fn auth(mut self, source: impl TokenSource + 'static) -> Self {
        self.auth = Some(Arc::new(source));
        self
    }

    /// Add a request [`Layer`] wrapping the network call (retry/backoff, logging,
    /// throttling, …). Layers compose in the order added — the last is outermost.
    /// E.g. `.layer(RetryBackoffLayer::default())`.
    pub fn layer(mut self, layer: impl Layer + 'static) -> Self {
        self.layers.push(Box::new(layer));
        self
    }

    /// Build the client. `#[track_caller]` so the `HTTP_CLIENT_CACHE` fallback can
    /// see which provider source file called it (see `cache_from_env`).
    #[track_caller]
    pub fn build(self) -> Result<HttpClient> {
        // Not `.or_else(..)`: a closure boundary would break `#[track_caller]`.
        let cache = if self.no_cache {
            None
        } else {
            match self.cache {
                Some(dir) => Some(dir),
                None => cache_from_env(),
            }
        };
        let mut builder = reqwest::Client::builder().default_headers(self.headers);
        if let Some(user_agent) = self.user_agent {
            builder = builder.user_agent(user_agent);
        }
        if let Some(timeout) = self.timeout {
            builder = builder.timeout(timeout);
        }
        let inner = builder.build().map_err(Error::Build)?;
        // Innermost first: api-token and auth sit closest to the wire so they
        // decorate every attempt; the user's layers (retry, …) wrap them.
        let mut service: Arc<dyn HttpService> = Arc::new(Execute {
            client: inner.clone(),
        });
        if let Some((param, value)) = self.api_token {
            service = ApiTokenLayer { param, value }.layer(service);
        }
        if let Some(source) = self.auth {
            service = AuthLayer { source }.layer(service);
        }
        for layer in self.layers {
            service = layer.layer(service);
        }
        Ok(HttpClient {
            inner,
            cache,
            base_url: self.base_url,
            query: Arc::new(self.query),
            service,
        })
    }
}

/// When `HTTP_CLIENT_CACHE` is set, cache into an `integration/` sibling of the
/// source file that built the client (else `None`). `Location::caller().file()` is
/// workspace-relative; this crate is two dirs down, which re-anchors it absolute.
#[track_caller]
fn cache_from_env() -> Option<PathBuf> {
    match std::env::var_os("HTTP_CLIENT_CACHE") {
        Some(v) if !v.is_empty() => {}
        _ => return None,
    }
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).parent()?.parent()?;
    let source = workspace.join(std::panic::Location::caller().file());
    Some(source.parent()?.join("integration"))
}

/// One recorded response on disk.
#[derive(Serialize, Deserialize)]
struct CacheEntry {
    /// Stored for readability only; not used on replay.
    url: String,
    status: u16,
    body: String,
}

/// Filename stem for a request. `DefaultHasher` has fixed keys, so the same
/// request hashes to the same file across runs.
fn cache_key(method: &Method, url: &str) -> String {
    let mut hasher = DefaultHasher::new();
    method.as_str().hash(&mut hasher);
    url.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn read_cache(path: &Path) -> Result<Response> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| Error::Cache(format!("read {}: {e}", path.display())))?;
    let entry: CacheEntry = serde_json::from_str(&text)
        .map_err(|e| Error::Cache(format!("parse {}: {e}", path.display())))?;
    build_response(entry.status, entry.body)
}

fn write_cache(path: &Path, url: &str, status: u16, body: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| Error::Cache(format!("create {}: {e}", parent.display())))?;
    }
    let entry = CacheEntry {
        url: url.to_string(),
        status,
        body: body.to_string(),
    };
    let text = serde_json::to_string_pretty(&entry)
        .map_err(|e| Error::Cache(format!("serialize: {e}")))?;
    std::fs::write(path, text).map_err(|e| Error::Cache(format!("write {}: {e}", path.display())))
}

/// Rebuild a [`Response`] from a recorded status + body, so a cache hit flows
/// through the same path as a live response.
fn build_response(status: u16, body: String) -> Result<Response> {
    let response = http::Response::builder()
        .status(status)
        .body(body)
        .map_err(|e| Error::Cache(format!("rebuild response: {e}")))?;
    Ok(Response::from(response))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A seeded cache entry replays without touching the network: the URL points
    /// at an unroutable port, so a cache miss would fail to connect.
    #[tokio::test]
    async fn replays_from_cache_without_network() {
        let dir = std::env::temp_dir().join(format!("strata-httpcache-{}", std::process::id()));
        let url = "http://127.0.0.1:9/thing";
        let entry = CacheEntry {
            url: url.to_string(),
            status: 200,
            body: r#"{"hello":"world"}"#.to_string(),
        };
        let path = dir.join(format!("{}.json", cache_key(&Method::GET, url)));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&path, serde_json::to_string(&entry).unwrap()).unwrap();

        let client = HttpClient::builder().cache(&dir).build().unwrap();
        let value: serde_json::Value = client.send_json(client.get(url)).await.unwrap();
        assert_eq!(value["hello"], "world");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn static_query_params_are_appended() {
        let client = HttpClient::builder()
            .query([("database", "analytics"), ("format", "json")])
            .build()
            .unwrap();
        let request = client
            .get("http://host/path")
            .query(&[("limit", "5")])
            .build()
            .unwrap();
        let pairs: Vec<(String, String)> = request
            .url()
            .query_pairs()
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        assert!(pairs.contains(&("database".into(), "analytics".into())));
        assert!(pairs.contains(&("format".into(), "json".into())));
        assert!(pairs.contains(&("limit".into(), "5".into())));
    }

    #[test]
    fn base_url_is_prepended_to_targets() {
        let based = HttpClient::builder()
            .base_url("https://api.example.com/v3")
            .build()
            .unwrap();
        let request = based.get("/bill/119/hr").build().unwrap();
        assert_eq!(
            request.url().as_str(),
            "https://api.example.com/v3/bill/119/hr"
        );

        let plain = HttpClient::builder().build().unwrap();
        let request = plain.get("https://host/thing").build().unwrap();
        assert_eq!(request.url().as_str(), "https://host/thing");
    }
}
