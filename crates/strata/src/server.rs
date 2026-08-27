use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use database::Database;
use serde_json::Value;

use crate::api::{self, Error};
use crate::provider::ProviderObject;
use crate::registry::Registry;
use crate::request::{ReadRequest as ReadPath, WriteRequest as WritePath};
use crate::router::Method;

pub struct Strata {
    registry: Arc<Registry>,
    db: Option<Database>,
}

impl Strata {
    pub fn db(&self) -> Option<&Database> {
        self.db.as_ref()
    }

    fn endpoints_under(&self, prefix: Option<&str>) -> Vec<crate::EndpointInfo> {
        let all = ProviderObject::endpoints(self.registry.as_ref());
        match prefix {
            None => all,
            Some(prefix) => all
                .into_iter()
                .filter(|endpoint| endpoint.path.starts_with(prefix))
                .collect(),
        }
    }

    fn path_with(&self, path: &str, query: &api::Query) -> String {
        let mut url = ReadPath::new(path)
            .with_cursor(query.cursor.clone())
            .with_cursor_field(query.cursor_field.clone())
            .path();
        for (key, value) in [
            ("limit", query.limit.map(|l| l.to_string())),
            ("filter", query.filter.clone()),
            ("fields", query.fields.clone()),
        ] {
            if let Some(value) = value {
                let separator = if url.contains('?') { '&' } else { '?' };
                url = format!("{url}{separator}{key}={}", urlencoding::encode(&value));
            }
        }
        url
    }
}

impl api::Server for Strata {
    async fn mounts(&self) -> api::Result<api::MountsResponse> {
        Ok(api::MountsResponse {
            mounts: self.registry.names(),
        })
    }

    async fn list(&self, request: api::ListRequest) -> api::Result<api::ListResponse> {
        Ok(api::ListResponse {
            endpoints: self
                .endpoints_under(request.path.as_deref())
                .iter()
                .map(|endpoint| Value::String(endpoint.path.clone()))
                .collect(),
        })
    }

    async fn schema(&self, request: api::SchemaRequest) -> api::Result<api::SchemaResponse> {
        let endpoints = self
            .endpoints_under(request.path.as_deref())
            .iter()
            .map(serde_json::to_value)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::internal(e.to_string()))?;
        Ok(api::SchemaResponse {
            schema: Value::Array(endpoints),
        })
    }

    async fn read(&self, request: api::ReadRequest) -> api::Result<api::ReadResponse> {
        let path = self.path_with(&request.path, &request.query);
        let stream = ProviderObject::read(self.registry.as_ref(), &path).await?;
        let schema = stream.schema.clone();
        match stream.first().await? {
            Some(chunk) => Ok(api::ReadResponse {
                rows: chunk
                    .data
                    .to_json_rows(&schema)
                    .map_err(|e| Error::internal(format!("{e:#}")))?,
                cursor: match chunk.cursor {
                    Some(cursor) => api::Cursor {
                        next: cursor.next,
                        total: cursor.total,
                    },
                    None => api::Cursor {
                        next: None,
                        total: None,
                    },
                },
            }),
            None => Ok(api::ReadResponse {
                rows: Vec::new(),
                cursor: api::Cursor {
                    next: None,
                    total: None,
                },
            }),
        }
    }

    async fn get(&self, request: api::GetRequest) -> api::Result<api::GetResponse> {
        let response =
            ProviderObject::invoke(self.registry.as_ref(), Method::Get, &request.path, None)
                .await?;
        Ok(api::GetResponse {
            entity: response.entity,
            output: response.output,
        })
    }

    async fn create(&self, request: api::CreateRequest) -> api::Result<api::CreateResponse> {
        let body = crate::Body {
            data: None,
            meta: request.body,
        };
        let response = ProviderObject::invoke(
            self.registry.as_ref(),
            Method::Create,
            &request.path,
            Some(body),
        )
        .await?;
        Ok(api::CreateResponse {
            entity: response.entity,
            output: response.output,
        })
    }

    async fn put(&self, request: api::PutRequest) -> api::Result<api::PutResponse> {
        let schema: schema::Schema = match request.schema {
            Some(schema) => serde_json::from_value(schema)
                .map_err(|e| Error::invalid(format!("schema: {e}")))?,
            None => return Err(Error::invalid("a put needs a `schema`")),
        };
        let batch = crate::record::Batch::encode(&schema, &request.rows)
            .map_err(|e| Error::invalid(format!("{e:#}")))?;
        let data = crate::DataStream::once(schema, batch);
        let path = match request.disposition {
            Some(disposition) => {
                let disposition = crate::Disposition::from_param(Some(&disposition))
                    .map_err(|e| Error::invalid(format!("{e:#}")))?;
                WritePath::new(&request.path)
                    .with_disposition(disposition)
                    .path()
            }
            None => request.path.clone(),
        };
        let body = crate::Body {
            data: Some(data),
            meta: Value::Null,
        };
        let response =
            ProviderObject::invoke(self.registry.as_ref(), Method::Put, &path, Some(body)).await?;
        Ok(api::PutResponse {
            output: response.output,
        })
    }

    async fn pipe(&self, request: api::PipeRequest) -> api::Result<api::PipeResponse> {
        let (source_mount, source_path) = crate::registry::split_mount(&request.source)?;
        let (destination_mount, destination_path) =
            crate::registry::split_mount(&request.destination)?;
        let mut pipe = strata_types::Pipe::new(
            strata_types::Endpoint::new(source_mount, source_path),
            strata_types::Endpoint::new(destination_mount, destination_path),
        );
        crate::pipe::run_pass(&self.registry, &crate::pipe::store::NoPipeStore, &mut pipe)
            .await
            .map_err(|e| Error::internal(format!("{e:#}")))?;
        Ok(api::PipeResponse { rows_written: 0 })
    }
}

pub struct Options {
    config: Option<String>,
    flight: SocketAddr,
    http: SocketAddr,
    graphql: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            config: None,
            flight: SocketAddr::from(([127, 0, 0, 1], 50051)),
            http: SocketAddr::from(([127, 0, 0, 1], 8080)),
            graphql: false,
        }
    }
}

impl Options {
    pub fn new() -> Self {
        Options::default()
    }

    pub fn config(mut self, path: impl Into<String>) -> Self {
        self.config = Some(path.into());
        self
    }

    pub fn flight(mut self, addr: SocketAddr) -> Self {
        self.flight = addr;
        self
    }

    pub fn http(mut self, addr: SocketAddr) -> Self {
        self.http = addr;
        self
    }

    pub fn graphql(mut self, graphql: bool) -> Self {
        self.graphql = graphql;
        self
    }
}

impl Strata {
    pub async fn start(options: Options) -> Result<()> {
        let config = options.config.as_deref();
        let registry = Arc::new(match config {
            Some(path) => crate::registry_from_config(path).await?,
            None => crate::registry().await?,
        });

        let pipes = match crate::config_path(config) {
            Some(path) => crate::Config::load(path)?.pipes().to_vec(),
            None => Vec::new(),
        };

        let db = match pipes.is_empty() {
            true => None,
            false => Some(Database::new(std::env::var("DATABASE_URL").ok().as_deref()).await?),
        };

        let strata = Arc::new(Strata {
            registry: registry.clone(),
            db,
        });
        tokio::try_join!(
            crate::flight::serve(registry, options.flight),
            strata.http(options.graphql, options.http),
        )?;

        Ok(())
    }

    async fn http(self: Arc<Self>, use_graphql: bool, addr: SocketAddr) -> Result<()> {
        let registry = self.registry.clone();
        let schemas = self
            .db
            .clone()
            .map(|db| Arc::new(db) as Arc<dyn crate::graphql::SchemaStore>);

        let mut app = api::routes(self.clone()).merge(crate::mcp::routes(registry.clone()));
        if use_graphql {
            app = app.merge(crate::graphql::routes(registry, schemas).await?);
        }

        tracing::info!("strata on http://{addr} (api /v1, graphql /graphql, mcp /mcp)");
        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }
}
