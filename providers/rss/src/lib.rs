use std::sync::Arc;

use anyhow::{Result, anyhow};
use strata_sdk::config;
use schema::{HasSchema, Timestamp};
use serde::{Deserialize, Serialize};

use strata_sdk::page::{Cursor, ListStrategy, Page};
use strata_sdk::router::{Params, Route, Router};

#[derive(strata_sdk::Provider)]
#[config(Config)]
pub struct Rss {
    http: http_client::HttpClient,
    config: Config,
}

#[config]
struct Config {
    #[config(env = "RSS_URL", description = "Feed URL")]
    url: String,
}

impl Rss {
    fn new(config: Config) -> Result<Self> {
        let http = http_client::HttpClient::builder()
            .user_agent("Mozilla/5.0 (compatible; strata/0.1)")
            .build()?;

        Ok(Rss { http, config })
    }

    fn routes(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/items")
                .list(list_items)
                .strategy(ListStrategy::Offset),
        );
    }
}

async fn list_items(rss: Arc<Rss>, _p: Params) -> Result<Page<FeedItem>> {
    let url = &rss.config.url.clone();
    let response = rss.http.send(rss.http.get(url)).await?;
    let bytes = response.bytes().await?;
    let feed =
        feed_rs::parser::parse(&bytes[..]).map_err(|e| anyhow!("parsing feed {url}: {e}"))?;

    let items: Vec<FeedItem> = feed.entries.iter().map(FeedItem::from).collect();
    Ok(Page::new(items, Cursor::empty()))
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct FeedItem {
    id: String,
    title: Option<String>,
    link: Option<String>,
    summary: Option<String>,
    content: Option<String>,
    published: Option<Timestamp>,
    authors: Vec<String>,
    categories: Vec<String>,
}

fn text(t: &Option<feed_rs::model::Text>) -> Option<String> {
    t.as_ref().map(|t| t.content.clone())
}

impl From<&feed_rs::model::Entry> for FeedItem {
    fn from(e: &feed_rs::model::Entry) -> Self {
        FeedItem {
            id: e.id.clone(),
            title: text(&e.title),
            link: e.links.first().map(|l| l.href.clone()),
            summary: text(&e.summary),
            content: e.content.as_ref().and_then(|c| c.body.clone()),
            published: e.published.or(e.updated).map(Timestamp),
            authors: e.authors.iter().map(|a| a.name.clone()).collect(),
            categories: e.categories.iter().map(|c| c.term.clone()).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_sdk::testkit::Client;

    fn client() -> Result<Client<Rss>> {
        let config: strata_sdk::config::ProviderConfig = serde_json::from_value(serde_json::json!({
            "backend": "rss",
            "url": "https://hnrss.org/frontpage",
        }))?;
        Client::<Rss>::mount(&config)
    }

    #[tokio::test]
    async fn list_items() -> Result<()> {
        let mut stream = client()?.list("/items").await?;

        let items: Vec<FeedItem> = stream.next().await?;
        assert!(!items.is_empty());
        Ok(())
    }
}
