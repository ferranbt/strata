use std::sync::Arc;

use strata_sdk::{
    Cursor, Page, Params, Provider, ProviderConfig, Router, page::ListStrategy, router::Route,
};
use anyhow::{Result, anyhow};
use config_macro::config;
use http::header::{AUTHORIZATION, CONTENT_TYPE};
use schema::{HasSchema, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

const BASE_URL: &str = "https://api.linear.app/graphql";

#[config]
pub struct Config {
    #[config(default_env = "LINEAR_TOKEN")]
    token: String,
}

pub struct Linear {
    http: http_client::HttpClient,
}

impl Provider for Linear {
    fn name() -> &'static str {
        "linear"
    }

    fn new(config: &ProviderConfig) -> Result<Self> {
        let config: Config = config.decode()?;

        let http = http_client::HttpClient::builder()
            .base_url(BASE_URL)
            .default_header(CONTENT_TYPE, "application/json".parse()?)
            .default_header(AUTHORIZATION, config.token.parse()?)
            .build()?;

        Ok(Self { http })
    }

    fn register(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/issues")
                .list(list_issues)
                .strategy(ListStrategy::Offset),
        );
    }
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Issue {
    id: String,
    title: String,
    #[serde(rename = "createdAt")]
    created_at: Timestamp,
    #[serde(rename = "updatedAt")]
    updated_at: Timestamp,
}

#[derive(Debug, Deserialize, Serialize, HasSchema)]
struct LinearCursor {
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,
}

const ISSUE_FIELDS: &str = "id title createdAt updatedAt";

async fn list_issues(c: Arc<Linear>, p: Params) -> Result<Page<Issue>> {
    fetch_connection(&c.http, p, "issues", ISSUE_FIELDS).await
}

async fn fetch_connection<I: DeserializeOwned>(
    http: &http_client::HttpClient,
    p: Params,
    key: &str,
    fields: &str,
) -> Result<Page<I>> {
    let after = p.cursor::<LinearCursor>()?.end_cursor;
    let query = format!(
        "query($first: Int!, $after: String) \
         {{ {key}(first: $first, after: $after) \
            {{ nodes {{ {fields} }} pageInfo {{ hasNextPage endCursor }} }} }}"
    );
    let variables = serde_json::json!({ "first": p.limit(50, 250), "after": after });
    let body = serde_json::json!({ "query": query, "variables": variables }).to_string();

    let mut value: Value = http.send_json(http.post("").body(body)).await?;
    let connection = value
        .pointer_mut(&format!("/data/{key}"))
        .map(Value::take)
        .ok_or_else(|| anyhow!("linear response missing `{key}` connection"))?;

    #[derive(Debug, Deserialize, Serialize, HasSchema)]
    struct GraphqlCursor {
        #[serde(rename = "hasNextPage", default)]
        has_next_page: bool,
        #[serde(rename = "endCursor")]
        end_cursor: Option<String>,
    }

    #[derive(Deserialize)]
    struct Connection<I> {
        nodes: Vec<I>,
        #[serde(rename = "pageInfo")]
        page_info: GraphqlCursor,
    }

    let conn: Connection<I> = serde_json::from_value(connection)?;
    let cursor = if conn.page_info.has_next_page {
        Cursor::new(&conn.page_info)?
    } else {
        Cursor::empty()
    };
    Ok(Page::new(conn.nodes, cursor))
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_sdk::testkit::Client;

    fn client() -> Result<Client<Linear>> {
        let config: strata_sdk::config::ProviderConfig =
            serde_json::from_value(serde_json::json!({ "backend": "linear" }))?;
        Client::<Linear>::mount(&config)
    }

    #[tokio::test]
    async fn get_issues() -> anyhow::Result<()> {
        let client = client()?;

        let mut stream = client.list("/issues").await?;
        let issues_0: Vec<Issue> = stream.next().await?;
        assert!(!issues_0.is_empty());

        Ok(())
    }
}
