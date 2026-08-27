//! Google provider: Calendar, Drive/Docs, and Gmail — read-only, behind an
//! OAuth2 refresh-token flow. See `README.md` for credential setup.
//!
//! The provider owns one HTTP client and an OAuth token manager; every endpoint
//! goes through [`Google::api_get`] (JSON) or [`Google::api_get_text`] (raw),
//! which attach a fresh bearer token automatically.

mod calendar;
mod drive;
mod gmail;

use anyhow::Result;
use strata_sdk::config;
use strata_http_client::{HttpClient, OAuth2};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use strata_sdk::page::{Cursor, Page};
use strata_sdk::router::{Params, Router};

const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";

#[config]
struct Config {
    #[config(env = "GOOGLE_CLIENT_ID", description = "OAuth client ID")]
    client_id: String,
    #[config(env = "GOOGLE_SECRET", description = "OAuth client secret", secret)]
    secret: String,
    #[config(
        env = "GOOGLE_REFRESH_TOKEN",
        description = "OAuth refresh token",
        secret
    )]
    refresh_token: String,
}

impl Config {
    fn auth_source(&self) -> OAuth2 {
        let client_id = self.client_id.clone();
        let secret = self.secret.clone();
        let refresh_token = self.refresh_token.clone();

        OAuth2::new(TOKEN_URL, move || {
            Ok(vec![
                ("client_id".into(), client_id.clone()),
                ("client_secret".into(), secret.clone()),
                ("refresh_token".into(), refresh_token.clone()),
                ("grant_type".into(), "refresh_token".into()),
            ])
        })
    }
}

/// Provider state: a self-authenticating HTTP client (the bearer token is minted
/// and refreshed by the client via the OAuth2 refresh-token flow).
#[derive(strata_sdk::Provider)]
#[config(Config)]
pub struct Google {
    http: HttpClient,
}

impl Google {
    fn new(config: Config) -> Result<Self> {
        let http = HttpClient::builder().auth(config.auth_source()).build()?;
        Ok(Google { http })
    }

    fn routes(r: &mut Router<Self>) {
        calendar::register(r);
        drive::register(r);
        gmail::register(r);
    }

    /// GET returning the parsed JSON body. The client attaches the bearer token.
    async fn api_get<T: DeserializeOwned>(&self, url: &str, query: &[(&str, &str)]) -> Result<T> {
        let response = self.authed_get(url, query).await?;
        Ok(response.json().await?)
    }

    /// POST with a JSON `body`, returning the parsed JSON response (used for
    /// create/write endpoints).
    async fn api_post<T: DeserializeOwned>(&self, url: &str, body: &Value) -> Result<T> {
        let request = self.http.post(url).json(body);
        Ok(self.http.send_json(request).await?)
    }

    /// GET, returning the checked [`Response`](strata_http_client::Response) for the
    /// caller to decode as JSON or read as text.
    async fn authed_get(&self, url: &str, query: &[(&str, &str)]) -> Result<strata_http_client::Response> {
        let request = self.http.get(url).query(query);
        Ok(self.http.send(request).await?)
    }
}

/// This provider's cursor state: Google's opaque `nextPageToken`. Empty on the
/// first page (Google list APIs are forward-only — they don't page back).
#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct GoogleCursor {
    #[serde(default)]
    pub page_token: String,
}

/// A Google API's paging dialect: which query param carries the page size, and
/// that API's default and ceiling.
pub(crate) struct Paging {
    size_param: &'static str,
    default: u32,
    max: u32,
}

impl Paging {
    pub(crate) const fn new(size_param: &'static str, default: u32, max: u32) -> Self {
        Paging {
            size_param,
            default,
            max,
        }
    }
}

impl Google {
    pub(crate) async fn page<T>(
        &self,
        url: &str,
        key: &str,
        paging: &Paging,
        p: &Params,
        extra: &[(&str, &str)],
    ) -> Result<Page<T>>
    where
        T: for<'de> Deserialize<'de>,
    {
        let resume: GoogleCursor = p.cursor()?;
        let size = p.limit(paging.default, paging.max).to_string();

        let mut query: Vec<(&str, &str)> = extra.to_vec();
        query.push((paging.size_param, &size));
        if !resume.page_token.is_empty() {
            query.push(("pageToken", &resume.page_token));
        }

        let body: Value = self.api_get(url, &query).await?;
        let items = strata_http_client::decode_envelope(&body, key)?;
        let cursor = match body.get("nextPageToken").and_then(Value::as_str) {
            Some(token) => Cursor::new(&GoogleCursor {
                page_token: token.to_string(),
            })?,
            None => Cursor::empty(),
        };
        Ok(Page::new(items, cursor))
    }
}
