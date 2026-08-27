//! Serving one provider as a `strata-proto` binary.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::{Arc, OnceLock};

use anyhow::Result;
use clap::Parser;
use futures::StreamExt;
use futures::stream::BoxStream;
use strata_proto::provider_server::{Provider as ProviderService, ProviderServer};
use strata_proto::{
    Cursor, EndpointInfo, EndpointsRequest, EndpointsResponse, InvokeRequest, InvokeResponse,
    Method as ProtoMethod, MountRequest, MountResponse, Page, ReadRequest, ReadResponse,
    WriteRequest, WriteResponse, read_response, write_request,
};
use tonic::{Request, Response, Status, Streaming};

pub mod catalog;
mod client;
pub use client::{RemoteProvider, connect};

use crate::config::ProviderConfig;
use crate::page::ListStrategy;
use crate::provider::{Provider, ProviderObject};
use crate::record::{Batch, BatchPage, DataStream, Disposition};
use crate::router::{Body, Method};

#[derive(Parser)]
#[command(about = "A strata provider, served over the strata provider protocol")]
struct Cli {
    /// Address to serve on.
    #[arg(long, default_value = "127.0.0.1:0")]
    addr: SocketAddr,
}

/// Run `P` as a command-line provider binary: parse the arguments, install
/// logging, then serve. This is the whole of a provider's `main`.
pub async fn serve<P: Provider>() -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
    serve_on::<P>(Cli::parse().addr).await
}

/// Serve `P` on `addr`, until the process is stopped.
///
/// Nothing is constructed here: the binary offers `P` as a *factory*, and the
/// host calls `Mount` with the config to build the instance. A provider that
/// needs credentials or a connection therefore fails at mount time, where the
/// host can report it, rather than at process start.
pub async fn serve_on<P: Provider>(addr: SocketAddr) -> Result<()> {
    let plugin = Plugin::<P> {
        mounted: OnceLock::new(),
        provider: PhantomData,
    };
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let bound = listener.local_addr()?;

    // stdout is the handshake channel: the host reads this line to learn where to
    // dial, so logging goes to stderr and nothing else may be printed.
    println!("{}", catalog::Handshake::new("tcp", bound.to_string()));
    use std::io::Write;
    std::io::stdout().flush()?;
    tracing::info!("strata provider `{}` listening on {bound}", P::name());

    tonic::transport::Server::builder()
        .add_service(ProviderServer::new(plugin))
        .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

struct Plugin<P> {
    mounted: OnceLock<(HashMap<String, String>, Arc<dyn ProviderObject>)>,
    provider: PhantomData<fn() -> P>,
}

impl<P: Provider> Plugin<P> {
    /// The mounted instance, or an error if `Mount` has not been called yet.
    fn mounted(&self) -> Result<Arc<dyn ProviderObject>, Status> {
        self.mounted
            .get()
            .map(|(_, provider)| provider.clone())
            .ok_or_else(|| Status::failed_precondition("provider is not mounted"))
    }
}

fn failed(error: anyhow::Error) -> Status {
    Status::internal(format!("{error:#}"))
}

/// Rebuild the [`ProviderConfig`] from the wire's string map. `backend` and
/// `mount` are named fields; everything else flattens into the provider's own
/// settings, exactly as a `[provider.<name>]` table decodes in-process.
fn decode_config(config: std::collections::HashMap<String, String>) -> Result<ProviderConfig, Status> {
    let map: serde_json::Map<String, serde_json::Value> = config
        .into_iter()
        .map(|(key, value)| (key, serde_json::Value::String(value)))
        .collect();
    serde_json::from_value(serde_json::Value::Object(map))
        .map_err(|e| Status::invalid_argument(format!("decoding provider config: {e}")))
}

#[tonic::async_trait]
impl<P: Provider> ProviderService for Plugin<P> {
    type ReadStream = BoxStream<'static, Result<ReadResponse, Status>>;

    /// Build the instance from `config`, the same [`ProviderConfig`] a
    /// `[provider.<name>]` table decodes to in-process.
    /// Idempotent: a provider served at a fixed address outlives any one host, so
    /// re-mounting it with the same config is a no-op. A *different* config is
    /// refused rather than silently ignored, since the caller would otherwise be
    /// talking to an instance it didn't configure.
    async fn mount(
        &self,
        request: Request<MountRequest>,
    ) -> Result<Response<MountResponse>, Status> {
        let settings = request.into_inner().config;
        if let Some((mounted, _)) = self.mounted.get() {
            if mounted == &settings {
                return Ok(Response::new(MountResponse {
            name: P::name().to_string(),
        }));
            }
            return Err(Status::failed_precondition(format!(
                "`{}` is already mounted with a different config",
                P::name()
            )));
        }
        let config = decode_config(settings.clone())?;
        let instance = crate::provider::instance::<P>(&config)
            .map_err(|e| Status::invalid_argument(format!("mounting `{}`: {e:#}", P::name())))?;
        let _ = self.mounted.set((settings, Arc::from(instance)));
        Ok(Response::new(MountResponse {
            name: P::name().to_string(),
        }))
    }

    async fn endpoints(
        &self,
        request: Request<EndpointsRequest>,
    ) -> Result<Response<EndpointsResponse>, Status> {
        let request = request.into_inner();
        let provider = self.mounted()?;

        let endpoints = match &request.path {
            Some(path) => vec![provider.resolve(path).await.map_err(failed)?],
            None => provider.endpoints(),
        };
        let endpoints = endpoints
            .iter()
            .map(encode_endpoint)
            .collect::<Result<Vec<_>>>()
            .map_err(failed)?;
        Ok(Response::new(EndpointsResponse { endpoints }))
    }

    async fn read(
        &self,
        request: Request<ReadRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let path = request.into_inner().path;
        let provider = self.mounted()?;
        let stream = provider.read(&path).await.map_err(failed)?;
        let DataStream { schema, chunks } = stream;

        let declared = serde_json::to_string(&schema).map_err(|e| Status::internal(e.to_string()))?;
        let head = futures::stream::once(async move {
            Ok(ReadResponse {
                message: Some(read_response::Message::Schema(declared)),
            })
        });
        let pages = chunks.map(move |page| {
            let page = page.map_err(failed)?;
            Ok(ReadResponse {
                message: Some(read_response::Message::Page(encode_page(&schema, page)?)),
            })
        });
        Ok(Response::new(head.chain(pages).boxed()))
    }

    async fn write(
        &self,
        request: Request<Streaming<WriteRequest>>,
    ) -> Result<Response<WriteResponse>, Status> {
        // The target rides on the first message only; the pages follow.
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .and_then(|m| m.message)
            .ok_or_else(|| Status::invalid_argument("write stream was empty"))?;
        let start = match first {
            write_request::Message::Start(start) => start,
            write_request::Message::Page(_) => {
                return Err(Status::invalid_argument(
                    "write stream must open with a `start` message",
                ));
            }
        };
        let schema = serde_json::from_str(&start.schema)
            .map_err(|e| Status::invalid_argument(format!("decoding schema: {e}")))?;

        let chunks = incoming
            .map(|message| {
                let page = match message?.message {
                    Some(write_request::Message::Page(page)) => page,
                    _ => return Err(anyhow::anyhow!("expected a `page` message")),
                };
                decode_page(page)
            })
            .boxed();

        let body = Body {
            data: Some(DataStream { schema, chunks }),
            meta: serde_json::Value::Null,
        };
        let provider = self.mounted()?;
        let response = provider
            .invoke(Method::Put, &start.path, Some(body))
            .await
            .map_err(failed)?;
        Ok(Response::new(WriteResponse {
            output: serde_json::to_string(&response.output)
                .map_err(|e| Status::internal(e.to_string()))?,
        }))
    }

    async fn invoke(
        &self,
        request: Request<InvokeRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        let request = request.into_inner();
        let method = decode_method(request.method())?;
        let body = match request.meta {
            Some(meta) => Some(Body {
                data: None,
                meta: serde_json::from_str(&meta)
                    .map_err(|e| Status::invalid_argument(format!("decoding meta: {e}")))?,
            }),
            None => None,
        };
        let provider = self.mounted()?;
        let response = provider
            .invoke(method, &request.path, body)
            .await
            .map_err(failed)?;
        Ok(Response::new(InvokeResponse {
            entity: response
                .entity
                .map(|entity| serde_json::to_string(&entity))
                .transpose()
                .map_err(|e| Status::internal(e.to_string()))?,
            output: serde_json::to_string(&response.output)
                .map_err(|e| Status::internal(e.to_string()))?,
        }))
    }
}

fn encode_endpoint(endpoint: &crate::router::EndpointInfo) -> Result<EndpointInfo> {
    Ok(EndpointInfo {
        method: encode_method(endpoint.method) as i32,
        path: endpoint.path.clone(),
        description: endpoint.description.clone(),
        params: endpoint.params.clone(),
        body_schema: serde_json::to_string(&endpoint.body)?,
        response_schema: serde_json::to_string(&endpoint.response)?,
        metadata: encode_metadata(&endpoint.metadata),
    })
}

/// The route's metadata as plain strings, so the contract doesn't have to mirror
/// every router enum. Absent keys mean the route declared nothing.
fn encode_metadata(metadata: &crate::router::RouterMetadata) -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    if let Some(strategy) = metadata.strategy {
        map.insert("strategy".to_string(), format!("{strategy:?}").to_lowercase());
    }
    map.insert(
        "disposition".to_string(),
        metadata.disposition.as_param().to_string(),
    );
    map.insert("queryable".to_string(), metadata.queryable.to_string());
    map
}

fn decode_endpoint(endpoint: &EndpointInfo) -> Result<crate::router::EndpointInfo> {
    Ok(crate::router::EndpointInfo {
        method: decode_method(endpoint.method()).map_err(|e| anyhow::anyhow!("{}", e.message()))?,
        path: endpoint.path.clone(),
        description: endpoint.description.clone(),
        params: endpoint.params.clone(),
        body: serde_json::from_str(&endpoint.body_schema)?,
        response: serde_json::from_str(&endpoint.response_schema)?,
        metadata: decode_metadata(&endpoint.metadata)?,
    })
}

fn decode_metadata(
    metadata: &std::collections::HashMap<String, String>,
) -> Result<crate::router::RouterMetadata> {
    let strategy = match metadata.get("strategy").map(String::as_str) {
        Some("offset") => Some(ListStrategy::Offset),
        Some("nextlink") => Some(ListStrategy::NextLink),
        Some(other) => anyhow::bail!("unknown list strategy `{other}`"),
        None => None,
    };
    Ok(crate::router::RouterMetadata {
        strategy,
        disposition: Disposition::from_param(metadata.get("disposition").map(String::as_str))?,
        queryable: metadata.get("queryable").map(String::as_str) == Some("true"),
    })
}

fn encode_method(method: Method) -> ProtoMethod {
    match method {
        Method::Get => ProtoMethod::Get,
        Method::List => ProtoMethod::List,
        Method::Create => ProtoMethod::Create,
        Method::Put => ProtoMethod::Put,
    }
}

fn decode_method(method: ProtoMethod) -> Result<Method, Status> {
    match method {
        ProtoMethod::Get => Ok(Method::Get),
        ProtoMethod::List => Ok(Method::List),
        ProtoMethod::Create => Ok(Method::Create),
        ProtoMethod::Put => Ok(Method::Put),
        ProtoMethod::Unspecified => Err(Status::invalid_argument("method is unspecified")),
    }
}

/// A page as a self-contained Arrow IPC stream: schema then batch, so the reader
/// decodes it without carrying state between messages.
fn encode_page(schema: &strata_schema::Schema, page: BatchPage) -> Result<Page, Status> {
    let batch = page.data.to_record_batch(schema).map_err(failed)?;
    let mut buffer = Vec::new();
    {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema())
            .map_err(|e| Status::internal(e.to_string()))?;
        writer
            .write(&batch)
            .map_err(|e| Status::internal(e.to_string()))?;
        writer
            .finish()
            .map_err(|e| Status::internal(e.to_string()))?;
    }
    Ok(Page {
        arrow_ipc: buffer,
        cursor: page.cursor.map(|cursor| Cursor {
            next: cursor.next,
            total: cursor.total,
        }),
    })
}

fn decode_page(page: Page) -> Result<BatchPage> {
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(page.arrow_ipc), None)?;
    let batches = reader.collect::<Result<Vec<_>, _>>()?;
    let batch = batches
        .first()
        .ok_or_else(|| anyhow::anyhow!("page carried no record batch"))?;
    Ok(BatchPage {
        data: Batch::from_record_batch(batch),
        cursor: page.cursor.map(|cursor| crate::page::Cursor {
            next: cursor.next,
            total: cursor.total,
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dummy::Dummy;
    use strata_schema::{DataType, SchemaBuilder};
    use strata_proto::provider_client::ProviderClient;

    /// Serve `dummy` over the wire and drive it with a generated gRPC client:
    /// nothing answers before `Mount`, the endpoint listing crosses with its
    /// metadata, and a read streams its schema followed by pages whose Arrow IPC
    /// decodes back to the rows the generator produced.
    #[tokio::test]
    async fn serves_a_provider_over_grpc() -> Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        drop(listener);
        tokio::spawn(async move { serve_on::<Dummy>(addr).await });

        let endpoint = format!("http://{addr}");
        let mut client = loop {
            match ProviderClient::connect(endpoint.clone()).await {
                Ok(client) => break client,
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        };

        assert_eq!(
            client
                .endpoints(EndpointsRequest { path: None })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        client
            .mount(MountRequest {
                config: [("backend".to_string(), "dummy".to_string())].into(),
            })
            .await?;

        let listed = client
            .endpoints(EndpointsRequest { path: None })
            .await?
            .into_inner();
        assert_eq!(listed.endpoints.len(), 1);
        assert_eq!(listed.endpoints[0].path, "/data");
        assert_eq!(listed.endpoints[0].method(), ProtoMethod::List);
        assert_eq!(
            listed.endpoints[0].metadata.get("strategy"),
            Some(&"offset".to_string())
        );

        let row_schema = SchemaBuilder::new()
            .column("id", DataType::Int64)
            .key()
            .column("name", DataType::String)
            .build();
        let encoded = urlencoding::encode(&serde_json::to_string(&row_schema)?).into_owned();
        let path = format!("/data?schema={encoded}&rows=25&limit=10");

        let mut stream = client.read(ReadRequest { path }).await?.into_inner();
        let declared = match stream.message().await?.and_then(|m| m.message) {
            Some(read_response::Message::Schema(schema)) => schema,
            other => panic!("expected the schema first, got {other:?}"),
        };
        assert_eq!(serde_json::from_str::<strata_schema::Schema>(&declared)?, row_schema);

        let mut rows = 0;
        let mut pages = 0;
        while let Some(message) = stream.message().await? {
            let Some(read_response::Message::Page(page)) = message.message else {
                panic!("expected a page");
            };
            rows += decode_page(page)?.data.row_count();
            pages += 1;
        }
        assert_eq!(rows, 25, "every generated row must cross the wire");
        assert_eq!(pages, 3, "25 rows at a limit of 10 is three pages");
        Ok(())
    }
}
