//! The registry side: a provider that lives in another process.
//!
//! [`RemoteProvider`] implements [`ProviderObject`], so once it is mounted the
//! registry, pipe, Flight, GraphQL, and MCP cannot tell it from an in-process
//! one — they all address it by mount and path exactly as before.


use anyhow::{Result, anyhow, bail};
use futures::StreamExt;
use strata_proto::provider_client::ProviderClient;
use strata_proto::{
    EndpointsRequest, InvokeRequest, MountRequest, ReadRequest, WriteRequest, read_response,
    write_request,
};
use tonic::transport::Channel;

use crate::config::ProviderConfig;
use crate::provider::ProviderObject;
use crate::record::DataStream;
use crate::router::{Body, BoxFuture, EndpointInfo, Method, Response};
use strata_proto::WriteStart;

use super::{decode_endpoint, decode_page, encode_method, encode_page};

/// Connect to a provider serving the protocol at `endpoint`, mount it with
/// `config`, and cache its endpoint listing.
///
/// The listing is fetched once here because [`ProviderObject::endpoints`] is
/// synchronous; it describes the routes a provider registers at startup, which
/// don't change while it runs.
pub async fn connect(
    mount: &str,
    endpoint: &str,
    config: &ProviderConfig,
) -> Result<RemoteProvider> {
    let mut client = ProviderClient::connect(endpoint.to_string())
        .await
        .map_err(|e| anyhow!("connecting to provider at `{endpoint}`: {e}"))?;

    client
        .mount(MountRequest {
            config: config.to_map(),
        })
        .await
        .map_err(|e| anyhow!("mounting `{mount}` at `{endpoint}`: {}", e.message()))?;

    let listed = client
        .endpoints(EndpointsRequest { path: None })
        .await
        .map_err(|e| anyhow!("listing endpoints of `{mount}`: {}", e.message()))?
        .into_inner();
    let endpoints = listed
        .endpoints
        .iter()
        .map(decode_endpoint)
        .collect::<Result<Vec<_>>>()?;

    Ok(RemoteProvider {
        mount: mount.to_string(),
        client,
        endpoints,
        process: None,
    })
}

pub struct RemoteProvider {
    mount: String,
    client: ProviderClient<Channel>,
    endpoints: Vec<EndpointInfo>,
    process: Option<super::catalog::Process>,
}

impl RemoteProvider {
    pub fn owning(mut self, process: super::catalog::Process) -> Self {
        self.process = Some(process);
        self
    }
}

impl ProviderObject for RemoteProvider {
    fn endpoints(&self) -> Vec<EndpointInfo> {
        self.endpoints.clone()
    }

    /// Asked over the wire with the concrete path, so the provider runs its own
    /// dynamic resolver and reports a table's real columns rather than whatever it
    /// declared statically.
    fn resolve<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<EndpointInfo>> {
        let mut client = self.client.clone();
        let path = path.to_string();
        Box::pin(async move {
            let listed = client
                .endpoints(EndpointsRequest {
                    path: Some(path.clone()),
                })
                .await
                .map_err(|e| anyhow!("{}", e.message()))?
                .into_inner();
            let endpoint = listed
                .endpoints
                .first()
                .ok_or_else(|| anyhow!("no read route matches `{path}` on `{}`", self.mount))?;
            decode_endpoint(endpoint)
        })
    }

    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DataStream>> {
        let mut client = self.client.clone();
        let path = path.to_string();
        Box::pin(async move {
            let mut stream = client
                .read(ReadRequest { path })
                .await
                .map_err(|e| anyhow!("{}", e.message()))?
                .into_inner();

            // The stream opens with its schema, then carries pages.
            let declared = match stream.message().await.map_err(|e| anyhow!("{}", e.message()))? {
                Some(message) => match message.message {
                    Some(read_response::Message::Schema(schema)) => schema,
                    _ => bail!("read stream did not open with its schema"),
                },
                None => bail!("read stream was empty"),
            };

            let chunks = stream
                .map(|message| {
                    let message = message.map_err(|e| anyhow!("{}", e.message()))?;
                    match message.message {
                        Some(read_response::Message::Page(page)) => decode_page(page),
                        _ => Err(anyhow!("expected a page")),
                    }
                })
                .boxed();

            Ok(DataStream {
                schema: serde_json::from_str(&declared)?,
                chunks,
            })
        })
    }

    fn invoke<'a>(
        &'a self,
        method: Method,
        path: &'a str,
        body: Option<Body>,
    ) -> BoxFuture<'a, Result<Response>> {
        let mut client = self.client.clone();
        let path = path.to_string();
        Box::pin(async move {
            if method == Method::Put {
                return put(client, path, body).await;
            }
            let meta = match &body {
                Some(body) => Some(serde_json::to_string(&body.meta)?),
                None => None,
            };
            let response = client
                .invoke(InvokeRequest {
                    method: encode_method(method) as i32,
                    path,
                    meta,
                })
                .await
                .map_err(|e| anyhow!("{}", e.message()))?
                .into_inner();
            Ok(Response {
                entity: response
                    .entity
                    .map(|entity| serde_json::from_str(&entity))
                    .transpose()?,
                output: serde_json::from_str(&response.output)?,
            })
        })
    }
}

/// Send a `put` across: the target opens the stream, then every page follows.
///
/// The pages are collected before sending rather than streamed lazily: a lazy
/// stream can't satisfy the bounds tonic puts on a streaming request from inside
/// a boxed trait future. Every caller today hands `put` a single page, so this
/// buffers one batch, but a genuinely long write would be held in memory.
async fn put(
    mut client: ProviderClient<Channel>,
    path: String,
    body: Option<Body>,
) -> Result<Response> {
    let stream = body
        .and_then(|body| body.data)
        .ok_or_else(|| anyhow!("put requires an Arrow dataset body"))?;
    let DataStream { schema, mut chunks } = stream;

    let mut outbound = vec![WriteRequest {
        message: Some(write_request::Message::Start(WriteStart {
            path,
            schema: serde_json::to_string(&schema)?,
        })),
    }];
    while let Some(page) = chunks.next().await {
        let page = encode_page(&schema, page?).map_err(|e| anyhow!("{}", e.message()))?;
        outbound.push(WriteRequest {
            message: Some(write_request::Message::Page(page)),
        });
    }

    let response = client
        .write(futures::stream::iter(outbound))
        .await
        .map_err(|e| anyhow!("{}", e.message()))?
        .into_inner();
    Ok(Response {
        entity: None,
        output: serde_json::from_str(&response.output)?,
    })
}
