//! MCP server over the registry.

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::transport::streamable_http_server::StreamableHttpService;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::tower::StreamableHttpServerConfig;
use rmcp::{ErrorData, schemars, tool, tool_router};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::Registry;
use crate::router::Method;

pub async fn serve(registry: Arc<Registry>, addr: SocketAddr) -> Result<()> {
    let service = StreamableHttpService::new(
        move || {
            Ok(Strata {
                registry: registry.clone(),
            })
        },
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default()
            .with_stateful_mode(false)
            .with_json_response(true),
    );
    let app = axum::Router::new().nest_service("/mcp", service);
    tracing::info!("strata MCP server on http://{addr}/mcp");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

#[derive(Clone)]
struct Strata {
    registry: Arc<Registry>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ProviderArgs {
    /// Mount name of the provider, e.g. `github`.
    provider: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct EndpointArgs {
    /// Mount name of the provider, e.g. `local`.
    provider: String,
    /// Endpoint path within the provider, e.g. `/tables/headlines`.
    path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ListArgs {
    /// Mount name of the provider, e.g. `local`.
    provider: String,
    /// Endpoint path within the provider, e.g. `/tables/headlines`.
    path: String,
    /// Resume token from a previous call's `cursor.next`. Omit for the first page.
    cursor: Option<String>,
    /// Requested page size. A hint; the provider picks the real chunk size.
    limit: Option<u32>,
    /// Row predicate, as the JSON filter the endpoint accepts. Only some
    /// endpoints support it; see `metadata.queryable` from `describe_provider`.
    filter: Option<Value>,
    /// Subset of columns to return. Omit for the whole row.
    fields: Option<Vec<String>>,
}

fn failed(error: anyhow::Error) -> ErrorData {
    ErrorData::internal_error(format!("{error:#}"), None)
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Providers {
    providers: Vec<String>,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Endpoints {
    endpoints: Value,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct ResolvedEndpoint {
    method: String,
    path: String,
    params: Vec<String>,
    /// JSON Schema of one row of the response.
    response: Value,
    metadata: Value,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Entity {
    entity: Value,
}

#[derive(Debug, Serialize, schemars::JsonSchema)]
struct Rows {
    items: Vec<Value>,
    /// `next` is the token to pass back as `cursor`; null means the end.
    cursor: Option<Value>,
}

#[tool_router(server_handler)]
impl Strata {
    /// Every mounted provider, by mount name. Start here, then call
    /// `describe_provider` to see what one of them answers.
    #[tool]
    async fn list_providers(&self) -> Json<Providers> {
        Json(Providers {
            providers: self.registry.names(),
        })
    }

    /// Every endpoint of one provider: its verb, path, path parameters, and the
    /// JSON Schema of its request body and response. Schemas here are the static
    /// ones; for an endpoint whose type depends on the concrete path (a SQL
    /// table's columns), call `resolve_endpoint`.
    #[tool]
    async fn describe_provider(
        &self,
        Parameters(args): Parameters<ProviderArgs>,
    ) -> Result<Json<Endpoints>, ErrorData> {
        let described = self.registry.describe(&args.provider).map_err(failed)?;
        Ok(Json(Endpoints {
            endpoints: described.get("endpoints").cloned().unwrap_or(Value::Null),
        }))
    }

    /// The response schema of one concrete path, resolved by running the
    /// endpoint's dynamic resolver. This is how a SQL or Iceberg table's real
    /// columns become visible, without reading any rows.
    #[tool]
    async fn resolve_endpoint(
        &self,
        Parameters(args): Parameters<EndpointArgs>,
    ) -> Result<Json<ResolvedEndpoint>, ErrorData> {
        let provider = self.registry.get(&args.provider).map_err(failed)?;
        let endpoint = provider.resolve(&args.path).await.map_err(failed)?;
        Ok(Json(ResolvedEndpoint {
            method: endpoint.method.to_string(),
            path: endpoint.path,
            params: endpoint.params,
            response: endpoint.response.to_json_schema(),
            metadata: serde_json::to_value(&endpoint.metadata).unwrap_or(Value::Null),
        }))
    }

    /// Read one entity from a `get` endpoint.
    #[tool]
    async fn get(
        &self,
        Parameters(args): Parameters<EndpointArgs>,
    ) -> Result<Json<Entity>, ErrorData> {
        let provider = self.registry.get(&args.provider).map_err(failed)?;
        let response = provider
            .invoke(Method::Get, &args.path, None)
            .await
            .map_err(failed)?;
        Ok(Json(Entity {
            entity: response.entity.unwrap_or(response.output),
        }))
    }

    /// Read one page of rows from a `list` endpoint, as `{ items, cursor }`. Pass
    /// `cursor.next` back as `cursor` to continue; a `next` of null is the end.
    #[tool]
    async fn list(&self, Parameters(args): Parameters<ListArgs>) -> Result<Json<Rows>, ErrorData> {
        let provider = self.registry.get(&args.provider).map_err(failed)?;
        let path = read_path(&args);
        let stream = provider.read(&path).await.map_err(failed)?;
        let schema = stream.schema.clone();
        let page = stream.first().await.map_err(failed)?;
        let (items, cursor) = match page {
            Some(page) => (
                page.data.to_json_rows(&schema).map_err(failed)?,
                page.cursor.and_then(|c| serde_json::to_value(c).ok()),
            ),
            None => (Vec::new(), None),
        };
        Ok(Json(Rows { items, cursor }))
    }
}

fn read_path(args: &ListArgs) -> String {
    let mut params: Vec<(String, String)> = Vec::new();
    if let Some(cursor) = &args.cursor {
        params.push(("cursor".into(), cursor.clone()));
    }
    if let Some(limit) = args.limit {
        params.push(("limit".into(), limit.to_string()));
    }
    if let Some(filter) = &args.filter {
        params.push(("filter".into(), filter.to_string()));
    }
    if let Some(fields) = &args.fields
        && !fields.is_empty()
    {
        params.push(("fields".into(), fields.join(",")));
    }
    match serde_urlencoded::to_string(&params) {
        Ok(query) if !query.is_empty() => format!("{}?{query}", args.path),
        _ => args.path.clone(),
    }
}
