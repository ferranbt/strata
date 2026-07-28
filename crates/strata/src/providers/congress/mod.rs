use std::future::Future;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use config_macro::config;
use schema::{Date, HasSchema, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::page::{Cursor, ListStrategy, Page};
use crate::provider::Provider;
use crate::router::{Params, Route, Router};

const API_BASE: &str = "https://api.congress.gov/v3";
const LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 249; // 250 - end check

pub struct Congress {
    http: http_client::HttpClient,
}

impl Congress {
    async fn api_get<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        key: &str,
        query: Vec<(&str, String)>,
    ) -> Result<T> {
        let request = self.http.get(path).query(&query);
        let body: Value = self.http.send_json(request).await?;

        let raw = body
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow!("congress response missing `{key}`"))?;

        Ok(serde_json::from_value(raw)?)
    }
}

#[config]
struct Config {
    #[config(default_env = "CONGRESS_GOV_API")]
    api_key: String,
}

impl Provider for Congress {
    fn name() -> &'static str {
        "congress"
    }

    fn new(config: &crate::config::ProviderConfig) -> Result<Self> {
        let mut builder = http_client::HttpClient::builder()
            .base_url(API_BASE)
            .user_agent("strata/0.1")
            .query([("format", "json")]);

        let config: Config = config.decode()?;
        builder = builder.api_token("api_key", config.api_key);

        Ok(Congress {
            http: builder.build()?,
        })
    }

    fn register(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/bills")
                .list(list_bills)
                .strategy(ListStrategy::Offset),
        );
        r.add(
            Route::new()
                .path("/hearings")
                .list(list_hearings)
                .strategy(ListStrategy::Offset),
        );
        r.add(
            Route::new()
                .path("/members")
                .list(list_members)
                .strategy(ListStrategy::Offset),
        );
    }
}

// === Endpoint handlers ===
#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct CongressCursor {
    #[serde(default)]
    offset: u32,
}

async fn page_with<S, T, F, Fut>(
    c: Arc<Congress>,
    endpoint: &str,
    envelope: &str,
    p: Params,
    hydrate: F,
) -> Result<Page<T>>
where
    S: DeserializeOwned + Send,
    T: Send,
    F: Fn(Arc<Congress>, S) -> Fut,
    Fut: Future<Output = Result<T>> + Send,
{
    let cursor: CongressCursor = p.cursor()?;
    let limit = p.limit(LIMIT, MAX_LIMIT);

    let query = vec![
        ("offset", cursor.offset.to_string()),
        ("limit", (limit + 1).to_string()),
    ];

    let mut summaries: Vec<S> = c.api_get(endpoint, envelope, query).await?;
    let next = if summaries.len() as u32 > limit {
        summaries.truncate(limit as usize);
        Cursor::new(&CongressCursor {
            offset: cursor.offset + limit,
        })?
    } else {
        Cursor::empty()
    };

    let mut items = Vec::with_capacity(summaries.len());
    for summary in summaries {
        items.push(hydrate(c.clone(), summary).await?);
    }
    Ok(Page::new(items, next))
}

async fn page<T: DeserializeOwned + Send>(
    c: Arc<Congress>,
    endpoint: &str,
    envelope: &str,
    p: Params,
) -> Result<Page<T>> {
    page_with(
        c,
        endpoint,
        envelope,
        p,
        |_, item: T| async move { Ok(item) },
    )
    .await
}

/// Recent bills in a congress, across all bill types.
async fn list_bills(c: Arc<Congress>, p: Params) -> Result<Page<BillSummary>> {
    page(c, "/bill", "bills", p).await
}

/// Hearings in a congress — each summary is followed to its full hearing record.
async fn list_hearings(c: Arc<Congress>, p: Params) -> Result<Page<Hearing>> {
    page_with(
        c,
        "/hearing",
        "hearings",
        p,
        |c, s: HearingSummary| async move {
            let path = format!(
                "/hearing/{}/{}/{}",
                s.congress,
                s.chamber.to_lowercase(),
                s.jacket_number
            );
            c.api_get::<Hearing>(&path, "hearing", vec![]).await
        },
    )
    .await
}

async fn list_members(c: Arc<Congress>, p: Params) -> Result<Page<Member>> {
    page(c, "/member", "members", p).await
}

/// The latest action recorded against a bill, embedded in list and detail rows.
#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct LatestAction {
    text: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct BillSummary {
    #[schema(key)]
    congress: Option<u32>,
    #[schema(key)]
    number: Option<String>,
    #[schema(key)]
    #[serde(rename = "type")]
    bill_type: Option<String>,
    title: Option<String>,
    #[serde(rename = "originChamber")]
    origin_chamber: Option<String>,
    #[serde(rename = "updateDate")]
    update_date: Option<Date>,
    #[serde(rename = "latestAction")]
    latest_action: Option<LatestAction>,
    url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Member {
    #[serde(rename = "bioguideId")]
    bioguide_id: Option<String>,
    name: Option<String>,
    #[serde(rename = "partyName")]
    party_name: Option<String>,
    state: Option<String>,
    district: Option<u32>,
    url: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Hearing {
    chamber: String,
    citation: String,
    congress: u32,
    title: String,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct HearingSummary {
    chamber: String,
    congress: u32,
    #[serde(rename = "jacketNumber")]
    jacket_number: u64,
    #[serde(rename = "updateDate")]
    update_date: Option<Timestamp>,
    url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::Client;

    fn client() -> Result<Client<Congress>> {
        let config: crate::config::ProviderConfig =
            serde_json::from_value(serde_json::json!({ "backend": "congress" }))?;
        Client::<Congress>::mount(&config)
    }

    #[tokio::test]
    async fn list_bills() -> Result<()> {
        let mut stream = client()?.list("/bills/").await?;

        let bills: Vec<BillSummary> = stream.next().await?;
        assert!(!bills.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn list_hearings() -> Result<()> {
        let mut stream = client()?.list("/hearings").await?;

        let hearings: Vec<Hearing> = stream.next().await?;
        assert!(!hearings.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn list_members() -> Result<()> {
        let mut stream = client()?.list("/members").await?;

        let members: Vec<Member> = stream.next().await?;
        assert!(!members.is_empty());
        Ok(())
    }
}
