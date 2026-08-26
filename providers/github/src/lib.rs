use std::sync::Arc;

use anyhow::Result;
use strata_sdk::config;
use http::HeaderMap;
use schema::HasSchema;
use serde::{Deserialize, Serialize};

use strata_sdk::page::{Cursor, ListStrategy, Page};
use strata_sdk::provider::Provider;
use strata_sdk::router::{Params, Route, Router};

const API_BASE: &str = "https://api.github.com";

#[config]
struct Config {
    #[config(default_env = "GITHUB_TOKEN")]
    api_key: String,
}

pub struct Github {
    http: http_client::HttpClient,
}

impl Github {
    async fn api<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        query: &[(&str, &str)],
    ) -> Result<T> {
        let request = self.http.get(format!("{API_BASE}{path}")).query(query);
        Ok(self.http.send_json::<T>(request).await?)
    }
}

impl Provider for Github {
    fn name() -> &'static str {
        "github"
    }

    fn new(config: &strata_sdk::config::ProviderConfig) -> Result<Self> {
        // GitHub requires a User-Agent on every request; retry rate limits / 5xx.
        let mut builder = http_client::HttpClient::builder()
            .user_agent("strata/0.1")
            .layer(http_client::RetryBackoffLayer::default());

        let config: Config = config.decode()?;
        builder = builder.bearer_auth(&config.api_key);

        Ok(Github {
            http: builder.build()?,
        })
    }

    fn register(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/repos/:owner/:name")
                .description("Fetch a repository by owner and name.")
                .get(get_repo),
        );
        r.add(
            Route::new()
                .path("/repos/:owner/:name/issues")
                .list(list_issues)
                .strategy(ListStrategy::Offset),
        );
        r.add(
            Route::new()
                .path("/repos/:owner/:name/comments")
                .list(list_comments)
                .strategy(ListStrategy::Offset),
        );
        r.add(
            Route::new()
                .path("/repos/:owner/:name/actions/runs")
                .list(list_workflow_runs)
                .strategy(ListStrategy::Offset),
        );
        r.add(Route::new().path("/users/:user").get(get_user));
    }
}

async fn get_repo(gh: Arc<Github>, p: Params) -> Result<Repository> {
    let owner = p.get("owner")?;
    let name = p.get("name")?;
    gh.api(&format!("/repos/{owner}/{name}"), &[]).await
}

#[derive(Debug, Deserialize, Serialize)]
struct GithubCursor {
    link: Option<String>,
}

/// Fetch one page following GitHub's `Link` header. If the cursor already carries
/// a `next` link, follow it; otherwise call `build` to construct the first
/// request. `envelope`, when set, names the array key to unwrap from the body
/// (`{ "<envelope>": [...] }`); when `None` the body is the array itself.
async fn page<T, F>(
    http: &http_client::HttpClient,
    p: Params,
    envelope: Option<String>,
    build: F,
) -> Result<Page<T>>
where
    T: for<'de> Deserialize<'de>,
    F: FnOnce() -> http_client::RequestBuilder,
{
    let cursor: GithubCursor = p.cursor()?;

    let request = match cursor.link {
        Some(link) => http.get(link),
        None => build(),
    };

    let response = http.send_json2::<serde_json::Value>(request).await?;
    let body = match &envelope {
        Some(key) => response.body.get(key).cloned().unwrap_or_default(),
        None => response.body,
    };
    let items: Vec<T> = serde_json::from_value(body)?;

    let next_url = extract_next_link_from_headers(response.headers);

    let next_cursor = GithubCursor { link: next_url };
    Ok(Page::new(items, Cursor::new(&next_cursor)?))
}

/// Issues for a repo
async fn list_issues(gh: Arc<Github>, p: Params) -> Result<Page<Issue>> {
    let owner = p.get("owner")?;
    let name = p.get("name")?;

    page(&gh.http, p.clone(), None, || {
        let query = vec![("state", "all"), ("sort", "updated"), ("direction", "asc")];

        let path = format!("/repos/{owner}/{name}/issues");
        gh.http.get(format!("{API_BASE}{path}")).query(&query)
    })
    .await
}

/// Comments for an issue.
async fn list_comments(gh: Arc<Github>, p: Params) -> Result<Page<Comment>> {
    let owner = p.get("owner")?;
    let name = p.get("name")?;

    page(&gh.http, p.clone(), None, || {
        let query = vec![("sort", "updated"), ("direction", "asc")];

        let path = format!("/repos/{owner}/{name}/comments");
        gh.http.get(format!("{API_BASE}{path}")).query(&query)
    })
    .await
}

async fn list_workflow_runs(gh: Arc<Github>, p: Params) -> Result<Page<WorkflowRun>> {
    let owner = p.get("owner")?;
    let name = p.get("name")?;

    page(
        &gh.http,
        p.clone(),
        Some("workflow_runs".to_string()),
        || {
            gh.http
                .get(format!("{API_BASE}/repos/{owner}/{name}/actions/runs"))
        },
    )
    .await
}

async fn get_user(gh: Arc<Github>, p: Params) -> Result<User> {
    let user = p.get("user")?;
    gh.api(&format!("/users/{user}"), &[]).await
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Repository {
    #[schema(description = "Owner and repo joined, e.g. rust-lang/rust")]
    full_name: String,
    description: Option<String>,
    language: Option<String>,
    stargazers_count: u64,
    forks_count: u64,
    open_issues_count: u64,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Comment {
    body: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Issue {
    number: u64,
    title: String,
    state: String,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct User {
    login: String,
    name: Option<String>,
    public_repos: u64,
    followers: u64,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct WorkflowRun {
    id: u64,
    name: Option<String>,
    head_branch: Option<String>,
    head_sha: String,
    run_number: u64,
    event: String,
    status: Option<String>,
    conclusion: Option<String>,
    workflow_id: u64,
    created_at: String,
    updated_at: String,
    run_started_at: Option<String>,
}

fn extract_next_link_from_headers(headers: HeaderMap) -> Option<String> {
    headers
        .get("link")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .split(',')
        .find(|part| part.contains("rel=\"next\""))
        .and_then(|part| part.split_once('<'))
        .and_then(|(_, rest)| rest.split_once('>'))
        .map(|(url, _)| url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_sdk::testkit::Client;

    fn client() -> Result<Client<Github>> {
        let config: strata_sdk::config::ProviderConfig =
            serde_json::from_value(serde_json::json!({ "backend": "github" }))?;
        Client::<Github>::mount(&config)
    }

    #[tokio::test]
    async fn get_repo() -> anyhow::Result<()> {
        let client = client()?;

        let repository: Repository = client.get("/repos/paradigmxyz/reth").await?;
        assert_eq!(repository.full_name, "paradigmxyz/reth".to_string());

        Ok(())
    }

    #[tokio::test]
    async fn get_issues() -> anyhow::Result<()> {
        let client = client()?;

        let mut stream = client.list("/repos/paradigmxyz/reth/issues").await?;
        let issues: Vec<Issue> = stream.next().await?;
        assert!(!issues.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn get_comments() -> anyhow::Result<()> {
        let client = client()?;

        let mut stream = client.list("/repos/paradigmxyz/reth/comments").await?;
        let comments: Vec<Comment> = stream.next().await?;
        assert!(!comments.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn get_workflow_runs() -> anyhow::Result<()> {
        let client = client()?;

        let mut stream = client.list("/repos/paradigmxyz/reth/actions/runs").await?;
        let workflows: Vec<WorkflowRun> = stream.next().await?;
        assert!(!workflows.is_empty());

        Ok(())
    }

    fn link_headers(value: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("link", value.parse().unwrap());
        headers
    }

    #[test]
    fn extracts_next_link_from_link_header() {
        // `next` is the first entry.
        let res = extract_next_link_from_headers(link_headers(
            "<https://api.github.com/repositories/1300192/issues?page=4>; rel=\"next\", \
            <https://api.github.com/repositories/1300192/issues?page=515>; rel=\"last\"",
        ));
        assert_eq!(
            res,
            Some("https://api.github.com/repositories/1300192/issues?page=4".to_string())
        );

        // `next` is not the first entry.
        let res = extract_next_link_from_headers(link_headers(
            "<https://api.github.com/repositories/1300192/issues?page=2>; rel=\"prev\", \
            <https://api.github.com/repositories/1300192/issues?page=4>; rel=\"next\"",
        ));
        assert_eq!(
            res,
            Some("https://api.github.com/repositories/1300192/issues?page=4".to_string())
        );

        // No `next` relation.
        let res = extract_next_link_from_headers(link_headers(
            "<https://api.github.com/repositories/1300192/issues?page=515>; rel=\"last\"",
        ));
        assert_eq!(res, None);

        // No `link` header at all.
        let res = extract_next_link_from_headers(HeaderMap::new());
        assert_eq!(res, None);
    }
}
