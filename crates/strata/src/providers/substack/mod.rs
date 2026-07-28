use std::sync::Arc;

use anyhow::Result;
use schema::{HasSchema, Timestamp};
use serde::{Deserialize, Serialize};

use crate::page::{Cursor, ListStrategy, Page};
use crate::provider::Provider;
use crate::router::{Params, Route, Router};

const LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 99; // 100 - end check

pub struct Substack {
    http: http_client::HttpClient,
}

impl Provider for Substack {
    fn name() -> &'static str {
        "substack"
    }

    fn new(_config: &crate::config::ProviderConfig) -> Result<Self> {
        let http = http_client::HttpClient::builder()
            .user_agent("Mozilla/5.0 (compatible; strata/0.1)")
            .build()?;
        Ok(Substack { http })
    }

    fn register(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/:publication/posts")
                .list(list_posts)
                .strategy(ListStrategy::Offset),
        );
    }
}

#[derive(Deserialize, Serialize)]
struct SubstackCursor {
    #[serde(default)]
    offset: u32,
}

async fn list_posts(s: Arc<Substack>, p: Params) -> Result<Page<PostSummary>> {
    let publication = p.get("publication")?;

    let limit = p.limit(LIMIT, MAX_LIMIT);
    let cursor: SubstackCursor = p.cursor()?;

    let url = format!("https://{publication}/api/v1/archive");
    let query = &[
        ("sort", "new"),
        ("limit", &(limit + 1).to_string()),
        ("offset", &cursor.offset.to_string()),
    ];

    let request = s.http.get(&url).query(query);
    let mut items: Vec<PostSummary> = s.http.send_json::<Vec<PostSummary>>(request).await?;

    let next = if items.len() as u32 > limit {
        items.truncate(limit as usize);
        Cursor::new(&SubstackCursor {
            offset: cursor.offset + limit,
        })?
    } else {
        Cursor::empty()
    };
    Ok(Page::new(items, next))
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct PostSummary {
    id: u64,
    slug: String,
    title: Option<String>,
    subtitle: Option<String>,
    post_date: Option<Timestamp>,
    audience: Option<String>,
    #[serde(rename = "type")]
    post_type: Option<String>,
    wordcount: Option<u64>,
    canonical_url: Option<String>,
    reaction_count: Option<u64>,
    comment_count: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::Client;

    fn client() -> Result<Client<Substack>> {
        let config: crate::config::ProviderConfig =
            serde_json::from_value(serde_json::json!({ "backend": "substack" }))?;
        Client::<Substack>::mount(&config)
    }

    #[tokio::test]
    async fn list_posts() -> Result<()> {
        let mut stream = client()?.list("/astralcodexten.substack.com/posts").await?;

        let posts: Vec<PostSummary> = stream.next().await?;
        assert!(!posts.is_empty());
        Ok(())
    }
}
