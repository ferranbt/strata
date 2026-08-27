mod client;
mod error;
mod types;

pub use client::Client;
pub use error::{Error, Kind, Result};
pub use types::*;

use std::future::Future;
use std::sync::Arc;

use axum::extract::{Path, Query as AxumQuery, State};
use axum::routing::{get, post, put};
use axum::{Json, Router};

pub trait Server: Send + Sync + 'static {
    fn mounts(&self) -> impl Future<Output = Result<MountsResponse>> + Send;
    fn list(&self, request: ListRequest) -> impl Future<Output = Result<ListResponse>> + Send;
    fn schema(&self, request: SchemaRequest) -> impl Future<Output = Result<SchemaResponse>> + Send;
    fn read(&self, request: ReadRequest) -> impl Future<Output = Result<ReadResponse>> + Send;
    fn get(&self, request: GetRequest) -> impl Future<Output = Result<GetResponse>> + Send;
    fn create(&self, request: CreateRequest) -> impl Future<Output = Result<CreateResponse>> + Send;
    fn put(&self, request: PutRequest) -> impl Future<Output = Result<PutResponse>> + Send;
    fn pipe(&self, request: PipeRequest) -> impl Future<Output = Result<PipeResponse>> + Send;
}

pub fn routes<S: Server>(server: Arc<S>) -> Router {
    Router::new()
        .route("/v1/mounts", get(mounts::<S>))
        .route("/v1/endpoints", get(list::<S>))
        .route("/v1/endpoints/{*path}", get(list::<S>))
        .route("/v1/schema", get(schema::<S>))
        .route("/v1/schema/{*path}", get(schema::<S>))
        .route("/v1/data/{*path}", get(read::<S>))
        .route("/v1/data/{*path}", post(create::<S>))
        .route("/v1/data/{*path}", put(put_data::<S>))
        .route("/v1/entity/{*path}", get(get_entity::<S>))
        .route("/v1/pipes/run", post(pipe::<S>))
        .with_state(server)
}

pub async fn serve<S: Server>(server: Arc<S>, addr: std::net::SocketAddr) -> anyhow::Result<()> {
    tracing::info!("strata control API on http://{addr}/v1");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, routes(server)).await?;
    Ok(())
}

fn absolute(path: String) -> String {
    match path.starts_with('/') {
        true => path,
        false => format!("/{path}"),
    }
}

async fn mounts<S: Server>(State(server): State<Arc<S>>) -> Result<Json<MountsResponse>> {
    Ok(Json(server.mounts().await?))
}

async fn list<S: Server>(
    State(server): State<Arc<S>>,
    path: Option<Path<String>>,
) -> Result<Json<ListResponse>> {
    let path = path.map(|Path(path)| absolute(path));
    Ok(Json(server.list(ListRequest { path }).await?))
}

async fn schema<S: Server>(
    State(server): State<Arc<S>>,
    path: Option<Path<String>>,
) -> Result<Json<SchemaResponse>> {
    let path = path.map(|Path(path)| absolute(path));
    Ok(Json(server.schema(SchemaRequest { path }).await?))
}

async fn read<S: Server>(
    State(server): State<Arc<S>>,
    Path(path): Path<String>,
    AxumQuery(query): AxumQuery<Query>,
) -> Result<Json<ReadResponse>> {
    Ok(Json(
        server
            .read(ReadRequest {
                path: absolute(path),
                query,
            })
            .await?,
    ))
}

async fn get_entity<S: Server>(
    State(server): State<Arc<S>>,
    Path(path): Path<String>,
) -> Result<Json<GetResponse>> {
    Ok(Json(
        server
            .get(GetRequest {
                path: absolute(path),
            })
            .await?,
    ))
}

async fn create<S: Server>(
    State(server): State<Arc<S>>,
    Path(path): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> Result<Json<CreateResponse>> {
    Ok(Json(
        server
            .create(CreateRequest {
                path: absolute(path),
                body,
            })
            .await?,
    ))
}

async fn put_data<S: Server>(
    State(server): State<Arc<S>>,
    Path(path): Path<String>,
    Json(request): Json<PutBody>,
) -> Result<Json<PutResponse>> {
    Ok(Json(
        server
            .put(PutRequest {
                path: absolute(path),
                rows: request.rows,
                schema: request.schema,
                disposition: request.disposition,
            })
            .await?,
    ))
}

#[derive(serde::Deserialize)]
struct PutBody {
    rows: Vec<serde_json::Value>,
    #[serde(default)]
    schema: Option<serde_json::Value>,
    #[serde(default)]
    disposition: Option<String>,
}

async fn pipe<S: Server>(
    State(server): State<Arc<S>>,
    Json(request): Json<PipeRequest>,
) -> Result<Json<PipeResponse>> {
    Ok(Json(server.pipe(request).await?))
}
