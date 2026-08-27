//! Arrow Flight server integrated with strata.
//!
//! Each provider endpoint is exposed as a "flight": a `FlightDescriptor` of kind
//! PATH `[provider, "/endpoint/path"]` identifies it — the same `(provider,
//! path)` pair the CLI's `call` uses. `GetSchema`/`GetFlightInfo` derive the
//! Arrow schema from the endpoint's resolved `DataType` (no fetch); `DoGet`
//! invokes the provider through the router and streams the result as Arrow
//! record batches (object → one row, array → N rows).

use std::net::SocketAddr;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::StreamExt;
use futures::stream::BoxStream;
use serde_json::Value;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

use crate::Registry;
use crate::record::{
    Batch, BatchPage, DataStream, arrow_schema_to_strata, strata_schema_to_arrow_schema,
};
use strata_schema::Schema as StrataSchema;
use crate::router::{Body, Method};

/// Start the Flight server on `addr`, serving every registered provider.
pub async fn serve(registry: Arc<Registry>, addr: SocketAddr) -> anyhow::Result<()> {
    tracing::info!("strata Arrow Flight server listening on {addr}");
    Server::builder()
        .add_service(FlightServiceServer::new(StrataFlight { registry }))
        .serve(addr)
        .await?;
    Ok(())
}

struct StrataFlight {
    registry: Arc<Registry>,
}

impl StrataFlight {
    /// Arrow schema for an endpoint, resolved (dynamically when the endpoint
    /// computes its own schema, e.g. a SQL table's columns) then converted.
    async fn arrow_schema(&self, provider: &str, path: &str) -> Result<Schema, Status> {
        let provider = self
            .registry
            .get(provider)
            .map_err(|e| Status::not_found(e.to_string()))?;

        let endpoint = provider
            .resolve(path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(strata_schema_to_arrow_schema(&endpoint.response))
    }
}

// === Descriptor / ticket encoding ===

/// Split `[provider, "/endpoint/path"]` (or `[provider, seg, seg, ...]`) into
/// `(provider, "/endpoint/path")`.
fn target_from_path(parts: &[String]) -> Result<(String, String), Status> {
    if parts.len() < 2 {
        return Err(Status::invalid_argument(
            "descriptor path must be [provider, /endpoint/path]",
        ));
    }
    let provider = parts[0].clone();
    let rest = &parts[1..];
    let joined = if rest.len() == 1 {
        rest[0].clone()
    } else {
        rest.join("/")
    };
    let path = if joined.starts_with('/') {
        joined
    } else {
        format!("/{joined}")
    };
    Ok((provider, path))
}

fn descriptor_target(descriptor: &FlightDescriptor) -> Result<(String, String), Status> {
    if descriptor.r#type == DescriptorType::Path as i32 {
        target_from_path(&descriptor.path)
    } else if descriptor.r#type == DescriptorType::Cmd as i32 {
        let cmd = String::from_utf8_lossy(&descriptor.cmd);
        let parts: Vec<String> = cmd.splitn(2, '\n').map(str::to_string).collect();
        target_from_path(&parts)
    } else {
        Err(Status::invalid_argument("unsupported descriptor type"))
    }
}

/// A ticket is `provider\n/endpoint/path`.
fn make_ticket(provider: &str, path: &str) -> Ticket {
    Ticket::new(format!("{provider}\n{path}"))
}

fn ticket_target(ticket: &Ticket) -> Result<(String, String), Status> {
    let raw = String::from_utf8_lossy(&ticket.ticket);
    raw.split_once('\n')
        .map(|(p, path)| (p.to_string(), path.to_string()))
        .ok_or_else(|| Status::invalid_argument("ticket must be `provider\\n/path`"))
}

type FlightStream<T> = BoxStream<'static, Result<T, Status>>;

/// Encode one [`BatchPage`]: its columns as Arrow `FlightData`
fn encode_chunk(chunk: BatchPage, schema: &StrataSchema) -> FlightStream<FlightData> {
    let BatchPage { data, cursor } = chunk;
    let batch = match data.to_record_batch(schema) {
        Ok(batch) => batch,
        Err(e) => {
            return futures::stream::once(async move { Err(Status::internal(e.to_string())) })
                .boxed();
        }
    };
    let arrow_schema = batch.schema();
    let batches = futures::stream::iter([Ok::<RecordBatch, FlightError>(batch)]);
    let data = FlightDataEncoderBuilder::new()
        .with_schema(arrow_schema)
        .build(batches)
        .map(|d| d.map_err(|e| Status::internal(e.to_string())));
    let trailer = cursor.map(|cursor| {
        serde_json::to_vec(&cursor)
            .map(|meta| FlightData {
                app_metadata: meta.into(),
                ..Default::default()
            })
            .map_err(|e| Status::internal(e.to_string()))
    });
    data.chain(futures::stream::iter(trailer)).boxed()
}

#[tonic::async_trait]
impl FlightService for StrataFlight {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoExchangeStream = FlightStream<FlightData>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let mut infos: Vec<Result<FlightInfo, Status>> = Vec::new();
        for name in self.registry.names() {
            let provider = self
                .registry
                .get(&name)
                .map_err(|e| Status::internal(e.to_string()))?;
            // Advertise the static schema per endpoint; endpoints whose schema
            // is dynamic or not record-shaped (e.g. a generic `Row`) are skipped.
            for endpoint in provider.endpoints() {
                let arrow_schema = strata_schema_to_arrow_schema(&endpoint.response);

                let descriptor = FlightDescriptor::new_path(vec![name.clone(), endpoint.path]);
                if let Ok(info) = FlightInfo::new().try_with_schema(&arrow_schema) {
                    infos.push(Ok(info.with_descriptor(descriptor)));
                }
            }
        }
        Ok(Response::new(futures::stream::iter(infos).boxed()))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let (provider, path) = descriptor_target(&descriptor)?;
        let schema = self.arrow_schema(&provider, &path).await?;
        let endpoint = FlightEndpoint::new().with_ticket(make_ticket(&provider, &path));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(e.to_string()))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_total_records(-1)
            .with_total_bytes(-1);
        Ok(Response::new(info))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let (provider, path) = descriptor_target(request.get_ref())?;
        let schema = self.arrow_schema(&provider, &path).await?;
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let result: SchemaResult = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .map_err(|e: arrow::error::ArrowError| Status::internal(e.to_string()))?;
        Ok(Response::new(result))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let (provider_name, path) = ticket_target(request.get_ref())?;
        let provider = self
            .registry
            .get(&provider_name)
            .map_err(|e| Status::not_found(e.to_string()))?;

        // The data plane is a stream: lazily pipe each chunk's batches out as
        // Arrow, forwarding the chunk's cursor as opaque `app_metadata` (the
        // per-batch checkpoint). `flat_map` is pull-based — a chunk (one page) is
        // fetched and encoded only when the client pulls it, never collected.
        let stream = provider
            .read(&path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let schema = stream.schema.clone();
        let out = stream
            .chunks
            .flat_map(move |chunk| match chunk {
                Ok(chunk) => encode_chunk(chunk, &schema),
                Err(e) => {
                    futures::stream::once(async move { Err(Status::internal(e.to_string())) })
                        .boxed()
                }
            })
            .boxed();
        Ok(Response::new(out))
    }

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Ok(Response::new(futures::stream::empty().boxed()))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info is not supported"))
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // The descriptor (provider mount + path) rides on the first message only —
        // read just that off the head to route, then stream the rest.
        let mut incoming = request.into_inner();
        let first = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("DoPut stream was empty"))?;
        let descriptor = first
            .flight_descriptor
            .clone()
            .ok_or_else(|| Status::invalid_argument("DoPut stream carried no FlightDescriptor"))?;
        let (provider_name, path) = descriptor_target(&descriptor)?;
        let provider = self
            .registry
            .get(&provider_name)
            .map_err(|e| Status::not_found(e.to_string()))?;

        // Decode the live stream lazily (buffered head + gRPC tail) so batches flow
        // wire → decode → sink without materializing the table.
        let head = futures::stream::once(async move { Ok::<_, FlightError>(first) });
        let tail = incoming.map(|m| m.map_err(|e| FlightError::ExternalError(Box::new(e))));
        let mut decoder = FlightRecordBatchStream::new_from_flight_data(head.chain(tail));

        // Pull the first batch to learn the schema — the stream's one-time init.
        let first_batch = decoder
            .next()
            .await
            .transpose()
            .map_err(|e| Status::internal(e.to_string()))?;
        let arrow_schema = decoder
            .schema()
            .ok_or_else(|| Status::invalid_argument("DoPut stream carried no schema"))?
            .clone();
        let schema = arrow_schema_to_strata(&arrow_schema);

        // One page per batch; the tail streams lazily. No cursor on the write path.
        // The first batch was pulled only to make the schema available, so it is put
        // back on the front here.
        let head_chunk = futures::stream::iter(first_batch.map(|batch| {
            Ok::<_, anyhow::Error>(BatchPage {
                data: Batch::from_record_batch(&batch),
                cursor: None,
            })
        }));
        let rest_chunks = decoder.map(|batch| {
            let batch = batch.map_err(|e| anyhow::anyhow!(e.to_string()))?;
            Ok(BatchPage {
                data: Batch::from_record_batch(&batch),
                cursor: None,
            })
        });
        let data = DataStream {
            schema,
            chunks: head_chunk.chain(rest_chunks).boxed(),
        };

        // Hand the Arrow stream straight to the `put` handler (no JSON body).
        let result = provider
            .invoke(
                Method::Put,
                &path,
                Some(Body {
                    data: Some(data),
                    meta: Value::Null,
                }),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Return the put result (JSON metadata: rows written, etc.) as
        // app_metadata — the metadata side-channel, like the DoGet cursor.
        let meta =
            serde_json::to_vec(&result.output).map_err(|e| Status::internal(e.to_string()))?;
        let put = PutResult {
            app_metadata: meta.into(),
        };
        Ok(Response::new(
            futures::stream::once(async move { Ok(put) }).boxed(),
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange is not supported"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action is not supported"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions is not supported"))
    }
}
