//! The router core: parse route patterns, match a path against them, and
//! dispatch to a typed async handler.
//!
//! A handler is a plain async fn `(Arc<S>, Params) -> Result<T>` where `T` is
//! any `Serialize + HasSchema` type. At registration we erase `T`, encoding its
//! rows to Arrow [`Records`](crate::record::Records) (the internal currency) and
//! boxing the future, so handlers with different return types live in one
//! `Router<S>`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use schema::{DataType, Field, HasSchema, Schema};
use serde::Serialize;
use serde_json::Value;

use futures::StreamExt;
use futures::stream::BoxStream;

use crate::DataStream;
use crate::page::{ListStrategy, Page};
use crate::record::{Batch, BatchPage, Disposition};

/// A boxed, owned future. `'static` because handlers take owned `Params` and an
/// `Arc<S>`, so nothing is borrowed across the await.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Reserved query-param name: the opaque token a caller sends back to resume a
/// list endpoint from a [`Cursor`].
pub const CURSOR_PARAM: &str = "cursor";

/// Reserved query-param name: the caller's requested page size. Framework-owned
/// alongside `cursor` — the two together are the paging contract. The router
/// re-applies it on every page of a [`DataStream`], so it holds for the whole scan.
const LIMIT_PARAM: &str = "limit";

/// Supplies an endpoint's persisted schema to the router at dispatch time. The
/// router depends only on this trait — `serve` wires in the concrete catalog. When
/// one is set, dispatch looks up the matched path's schema and hands it to the
/// handler via [`Params::schema`], so a handler reads its own annotations (key,
/// cursor) without touching the catalog itself.
pub trait SchemaSource: Send + Sync {
    /// The persisted schema for the endpoint at `path`, or `None` if none recorded.
    fn schema(&self, path: String) -> BoxFuture<'static, Result<Option<Schema>>>;
}

/// Inputs to a handler: path captures (e.g. `{owner: "rust-lang"}`) plus the
/// parsed query-string params from the call path (`?limit=20&cursor=…`), read by
/// name via [`query`](Params::query) or the framework accessors ([`cursor`],
/// [`limit`]).
///
/// [`cursor`]: Params::cursor
/// [`limit`]: Params::limit
#[derive(Debug, Clone, Default)]
pub struct Params {
    map: HashMap<String, String>,
    query: HashMap<String, String>,
    schema: Option<Schema>,
}

impl Params {
    /// Fetch a path capture by name, erroring if the route never bound it.
    pub fn get(&self, key: &str) -> Result<&str> {
        self.map
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| anyhow!("missing path parameter `{key}`"))
    }

    /// A single query param by name, or `None` if it wasn't supplied.
    pub fn query(&self, key: &str) -> Option<&str> {
        self.query.get(key).map(String::as_str)
    }

    pub fn schema(&self) -> Option<&Schema> {
        self.schema.as_ref()
    }

    /// The resume cursor, decoded into the provider's own cursor state `C` — the
    /// inverse of [`Cursor::new`]. On the first page (no `cursor` param) it decodes
    /// `{}`, so `C`'s `#[serde(default)]` fields supply the starting position.
    /// First-class and framework-owned: the reserved `cursor` param is never part
    /// of a route's typed params.
    pub fn cursor<C: serde::de::DeserializeOwned>(&self) -> Result<C> {
        let raw = self.query(CURSOR_PARAM).unwrap_or("{}");
        serde_json::from_str(raw).map_err(|e| anyhow!("decoding cursor: {e}"))
    }

    /// The caller's requested page size from the reserved `limit` param, else
    /// `default` — the backend's own default. Clamped to `max` (the backend's
    /// ceiling), so one call can't ask for the world. A non-numeric value falls
    /// back to `default` rather than erroring.
    ///
    /// Framework-owned like [`cursor`](Self::cursor): the page size is a *request*
    /// preference, never part of the cursor — a resume token encodes a position, so
    /// baking the batch size into it would freeze it for every later reader.
    pub fn limit(&self, default: u32, max: u32) -> u32 {
        self.query(LIMIT_PARAM)
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
            .clamp(1, max)
    }

    /// Attach the query string (the part after `?`) parsed from the call path.
    fn set_query(&mut self, raw: &str) {
        self.query = serde_urlencoded::from_str(raw).unwrap_or_default();
    }

    /// Set the reserved `cursor` param — how the `List` stream driver feeds each
    /// page's resume token back into the next handler call.
    fn set_cursor(&mut self, token: &str) {
        self.query
            .insert(CURSOR_PARAM.to_string(), token.to_string());
    }
}

/// One segment of a route pattern.
#[derive(Debug, Clone, PartialEq)]
enum Segment {
    /// A fixed segment that must match exactly, e.g. `repos`.
    Literal(String),
    /// A capture, written `:name`, that matches any single segment.
    Param(String),
    /// A catch-all, written `*name`, that captures the rest of the path —
    /// including embedded `/` — as one value (e.g. a full filesystem path).
    /// Must be the final segment of a pattern.
    Rest(String),
}

/// A parsed route pattern, e.g. `/repos/:owner/:name`.
#[derive(Debug, Clone)]
pub struct Pattern {
    raw: String,
    segments: Vec<Segment>,
}

impl Pattern {
    /// Parse a pattern string. Leading/trailing slashes are ignored, so
    /// `/repos/:owner/:name` and `repos/:owner/:name` are equivalent.
    pub fn parse(raw: &str) -> Pattern {
        let segments = split_path(raw)
            .map(|seg| {
                if let Some(name) = seg.strip_prefix(':') {
                    Segment::Param(name.to_string())
                } else if let Some(name) = seg.strip_prefix('*') {
                    Segment::Rest(name.to_string())
                } else {
                    Segment::Literal(seg.to_string())
                }
            })
            .collect();
        Pattern {
            raw: raw.to_string(),
            segments,
        }
    }

    /// The original pattern string, used for `list` and error messages.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// Names of the capture segments, in order (e.g. `["owner", "name"]`),
    /// used to describe an endpoint's inputs for introspection.
    pub fn param_names(&self) -> Vec<String> {
        self.segments
            .iter()
            .filter_map(|seg| match seg {
                Segment::Param(name) | Segment::Rest(name) => Some(name.clone()),
                Segment::Literal(_) => None,
            })
            .collect()
    }

    /// Try to match a concrete path. Returns the captured params on success, or
    /// `None` if this pattern doesn't match (a literal mismatch, or a length
    /// mismatch for patterns without a catch-all). A trailing `*name` segment
    /// captures all remaining segments — including `/` — joined back together
    /// (empty if none remain). Pure and sync, so it's trivially unit-testable.
    pub fn match_path(&self, path: &str) -> Option<Params> {
        let parts: Vec<&str> = split_path(path).collect();
        let mut map = HashMap::new();
        for (i, segment) in self.segments.iter().enumerate() {
            match segment {
                // Catch-all: consume everything left and stop. The parser only
                // ever produces this as the final segment.
                Segment::Rest(name) => {
                    map.insert(name.clone(), parts[i..].join("/"));
                    return Some(Params {
                        map,
                        ..Default::default()
                    });
                }
                _ if i >= parts.len() => return None,
                Segment::Literal(lit) if *lit == parts[i] => {}
                Segment::Literal(_) => return None,
                Segment::Param(name) => {
                    map.insert(name.clone(), parts[i].to_string());
                }
            }
        }
        // No catch-all matched: every path segment must have been consumed.
        (parts.len() == self.segments.len()).then_some(Params {
            map,
            ..Default::default()
        })
    }
}

/// Split a path into non-empty segments, ignoring leading/trailing slashes.
fn split_path(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|s| !s.is_empty())
}

/// The verb an endpoint answers. Reads (`Get`, `List`) take no body. The write
/// verbs take a body: `Create` a single typed entity (provider assigns
/// identity); `Put` a whole [`Dataset`] (schema + rows) — the write-dual of
/// `list`, used to pipe a reader's output into a sink. (An idempotent `Upsert`
/// verb is planned but not yet modeled.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Method {
    Get,
    List,
    Create,
    Put,
}

impl Method {
    /// Writes (`Create`/`Put`) carry a body; reads (`Get`/`List`) do not.
    pub fn is_write(self) -> bool {
        matches!(self, Method::Create | Method::Put)
    }
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Method::Get => "get",
            Method::List => "list",
            Method::Create => "create",
            Method::Put => "put",
        };
        f.write_str(s)
    }
}

/// The outcome of dispatching a call. Concrete fields, one per channel — never a
/// union, so the type is always known. A verb fills only its plane's field(s):
///
/// - **Entity plane** — `entity`: a `get`/`create`/`update`/`delete` result, ONE
///   JSON resource (possibly non-tabular). Off the Arrow stream channel.
/// - `output`: a `put`'s **execution metadata** as JSON (rows written, etc.).
///
/// The data plane doesn't ride here: a `list` is a [`DataStream`], served by
/// [`Router::dispatch_read`].
pub struct Response {
    pub entity: Option<Value>,
    pub output: Value,
}

/// The request payload for a write verb (`None` for reads). Same product shape as
/// [`Response`]: bulk input is Arrow, small input is JSON — never a union.
/// - `data`: a `put`'s dataset (rows as Arrow + the native schema contract).
/// - `meta`: a `create`'s single entity as JSON (user-supplied — a real JSON edge).
pub struct Body {
    pub data: Option<DataStream>,
    pub meta: Value,
}

/// A type-erased handler: takes provider state, params, and an optional request
/// [`Body`] (`Some` only for write verbs), and returns a [`Response`]. `Arc` (not
/// `Box`) so the `List` stream driver can clone and own it across the unfold.
type ErasedHandler<S> =
    Arc<dyn Fn(Arc<S>, Params, Option<Body>) -> BoxFuture<'static, Result<Response>> + Send + Sync>;

pub type Pages = BoxStream<'static, Result<BatchPage>>;

type ErasedPage<S> =
    Arc<dyn Fn(Arc<S>, Params) -> BoxFuture<'static, Result<BatchPage>> + Send + Sync>;

type ErasedStream<S> =
    Arc<dyn Fn(Arc<S>, Params) -> BoxFuture<'static, Result<Pages>> + Send + Sync>;

enum Handler<S> {
    Unary(ErasedHandler<S>),
    Stream(ErasedStream<S>),
}

/// Adapt a req/reply page handler into the stream primitive: call it, hand back
/// its page, then call it again with that page's `cursor.next` until the cursor
/// runs out. The sugar under [`Route::list`] and [`Route::list_records`].
fn paged_stream<S: Send + Sync + 'static>(
    handler: ErasedPage<S>,
    state: Arc<S>,
    base: Params,
) -> Pages {
    futures::stream::unfold(Some(None::<String>), move |token_state| {
        let handler = handler.clone();
        let state = state.clone();
        let base = base.clone();
        async move {
            let token = token_state?;
            let mut params = base.clone();
            if let Some(token) = &token {
                params.set_cursor(token);
            }
            let page = match handler(state, params).await {
                Ok(page) => page,
                Err(e) => return Some((Err(e), None)),
            };
            let next = page.cursor.as_ref().and_then(|c| c.next.clone());
            Some((Ok(page), next.map(Some)))
        }
    })
    .boxed()
}

/// A dynamic schema resolver: given state + the matched params, produces the
/// endpoint's response `DataType` at request time (e.g. a SQL table's columns).
type SchemaResolver<S> =
    Box<dyn Fn(Arc<S>, Params) -> BoxFuture<'static, Result<Schema>> + Send + Sync>;

/// One finalized route held by the router: pattern + handler + schema.
/// `response_schema` is the static schema captured from the handler's return
/// type; `schema_resolver`, when set, overrides it with a schema computed per
/// request (the value type stays whatever the handler returns — e.g. a generic
/// `Row` — while the declared schema is dynamic).
struct Entry<S> {
    pattern: Pattern,
    /// The verb this route answers; dispatch matches on `(pattern, method)`.
    method: Method,
    handler: Handler<S>,
    /// Schema of the request body for write verbs (`Json` for reads, which take
    /// no body).
    body_schema: Schema,
    response_schema: Schema,
    schema_resolver: Option<SchemaResolver<S>>,
    /// Human-readable description of the endpoint, for introspection.
    description: Option<String>,
    metadata: RouterMetadata,
}

impl<S> Entry<S> {
    /// Static description of this route (dynamic resolver not run).
    fn info(&self) -> EndpointInfo {
        EndpointInfo {
            method: self.method,
            path: self.pattern.as_str().to_string(),
            description: self.description.clone(),
            params: self.pattern.param_names(),
            body: self.body_schema.clone(),
            response: self.response_schema.clone(),
            metadata: self.metadata.clone(),
        }
    }
}

/// Erase a `get` handler `(Arc<S>, Params) -> T`: an entity-plane read. The result
/// is one JSON resource in [`Response::entity`] — off the Arrow stream channel, so
/// `T` need only be `Serialize` (it can be non-tabular). Takes no request body.
fn erase<S, T, F, Fut>(handler: F) -> ErasedHandler<S>
where
    S: Send + Sync + 'static,
    T: Serialize + 'static,
    F: Fn(Arc<S>, Params) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    Arc::new(move |state, params, _body| {
        let fut = handler(state, params);
        Box::pin(async move {
            let value = fut.await?;
            Ok(Response {
                entity: Some(serde_json::to_value(value)?),
                output: Value::Null,
            })
        })
    })
}

/// Erase a `create` handler `(Arc<S>, Params, B) -> T`: an entity-plane write.
/// Deserialize the incoming JSON entity from the body's `meta` into `B`, run the
/// handler, and return the created resource in [`Response::entity`].
fn erase_write<S, B, T, F, Fut>(handler: F) -> ErasedHandler<S>
where
    S: Send + Sync + 'static,
    B: serde::de::DeserializeOwned + Send + 'static,
    T: Serialize + 'static,
    F: Fn(Arc<S>, Params, B) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    Arc::new(move |state, params, body| {
        let parsed = body
            .ok_or_else(|| anyhow!("write endpoint requires a request body"))
            .and_then(|b| {
                serde_json::from_value::<B>(b.meta)
                    .map_err(|e| anyhow!("invalid request body: {e}"))
            });
        let fut = parsed.map(|b| handler(state, params, b));
        Box::pin(async move {
            Ok(Response {
                entity: Some(serde_json::to_value(fut?.await?)?),
                output: Value::Null,
            })
        })
    })
}

/// Erase a put handler `(Arc<S>, Params, DataStream) -> T`: take the Arrow
/// [`DataStream`] straight off the request body's `data` channel (no JSON) and run
/// the handler. Its result rides back as JSON `output` (rows written, etc.).
fn erase_put<S, T, F, Fut>(handler: F) -> ErasedHandler<S>
where
    S: Send + Sync + 'static,
    T: Serialize + 'static,
    F: Fn(Arc<S>, Params, DataStream) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<T>> + Send + 'static,
{
    Arc::new(move |state, params, body| {
        let stream = body
            .and_then(|b| b.data)
            .ok_or_else(|| anyhow!("put endpoint requires an Arrow dataset body"));
        let fut = stream.map(|d| handler(state, params, d));
        Box::pin(async move {
            // The write result rides as JSON `output`; no bulk data, no entity.
            Ok(Response {
                entity: None,
                output: serde_json::to_value(fut?.await?)?,
            })
        })
    })
}

/// Builder for one route. Configure it and hand it to [`Router::add`]:
///
/// ```ignore
/// r.add(Route::new().path("/repos/:owner/:name").get(get_repo));
/// r.add(Route::new().path("/bill/:congress").list(list_bills));
/// r.add(Route::new().path("/calendar/:cal/events").create(create_event));
/// r.add(Route::new().path("/tables/:t/data").list(read).data_type(schema));
/// ```
///
/// The verb method (`.get`/`.list`/`.create`/`.upsert`) fixes the state type `S`,
/// sets the route's [`Method`], and captures the static schemas from the
/// handler's body/return types; `.data_type` overrides the response schema with a
/// dynamic resolver. These are the extension points for routes — new endpoint
/// kinds become additional methods here rather than new `route_*` functions.
pub struct Route<S> {
    path: String,
    method: Method,
    handler: Option<ErasedHandler<S>>,
    stream_handler: Option<ErasedStream<S>>,
    body_schema: Option<Schema>,
    response_schema: Option<Schema>,
    schema_resolver: Option<SchemaResolver<S>>,
    description: Option<String>,
    strategy: Option<ListStrategy>,
    disposition: Option<Disposition>,
    queryable: bool,
}

impl<S: Send + Sync + 'static> Default for Route<S> {
    fn default() -> Self {
        Route {
            path: String::new(),
            method: Method::Get,
            handler: None,
            stream_handler: None,
            body_schema: None,
            response_schema: None,
            schema_resolver: None,
            description: None,
            strategy: None,
            disposition: None,
            queryable: false,
        }
    }
}

impl<S: Send + Sync + 'static> Route<S> {
    pub fn new() -> Self {
        Route::default()
    }

    /// The route pattern, e.g. `/repos/:owner/:name`.
    pub fn path(mut self, path: &str) -> Self {
        self.path = path.to_string();
        self
    }

    /// A `Get` endpoint: `(Arc<S>, Params) -> T`, returning a single value. Its
    /// return type `T` supplies the static response schema.
    pub fn get<T, F, Fut>(mut self, handler: F) -> Self
    where
        T: Serialize + HasSchema + 'static,
        F: Fn(Arc<S>, Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.method = Method::Get;
        self.handler = Some(erase(handler));
        self.response_schema = Some(T::schema());
        self
    }

    pub fn get_records<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Arc<S>, Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        self.method = Method::Get;
        self.handler = Some(erase(handler));
        self.response_schema = None;
        self
    }

    pub fn strategy(mut self, strategy: ListStrategy) -> Self {
        self.strategy = Some(strategy);
        self
    }

    pub fn writes(mut self, disposition: Disposition) -> Self {
        self.disposition = Some(disposition);
        self
    }

    pub fn queryable(mut self) -> Self {
        self.queryable = true;
        self
    }

    /// A `List` endpoint: `(Arc<S>, Params, Q) -> Page<T>`. `Q` is the typed query
    /// params (its schema is advertised as the endpoint's inputs, alongside the
    /// reserved `cursor`); `T` is the item type (the response schema is
    /// `List<T>`). The handler reads the resume cursor via `params.cursor()` and
    /// returns the next/prev cursor in the [`Page`].
    pub fn list<T, F, Fut>(self, handler: F) -> Self
    where
        T: Serialize + HasSchema + 'static,
        F: Fn(Arc<S>, Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Page<T>>> + Send + 'static,
    {
        let mut route = self.list_records(move |state, params| {
            let fut = handler(state, params);
            async move {
                let page = fut.await?;
                Ok(BatchPage {
                    data: Batch::encode(&T::schema(), &page.items)?,
                    cursor: Some(page.cursor),
                })
            }
        });
        route.response_schema = Some(T::schema());
        route
    }

    /// An Arrow-native `list`: `(Arc<S>, Params, Q) -> BatchPage`. The handler
    /// builds its own batch — for endpoints whose row schema is dynamic (a SQL
    /// table's columns), so the provider reads its schema and encodes directly.
    /// Pair with [`data_type`](Self::data_type) to advertise the per-request schema.
    pub fn list_records<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(Arc<S>, Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<BatchPage>> + Send + 'static,
    {
        let handler: ErasedPage<S> =
            Arc::new(move |state, params| Box::pin(handler(state, params)));
        self.list_stream(move |state, params| {
            let handler = handler.clone();
            async move { Ok(paged_stream(handler, state, params)) }
        })
    }

    /// The `List` primitive: `(Arc<S>, Params) -> Pages`. The provider owns the
    /// whole walk, for sources that already have a stream (a SQL cursor, an Arrow
    /// scan).
    pub fn list_stream<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(Arc<S>, Params) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Pages>> + Send + 'static,
    {
        self.method = Method::List;
        self.stream_handler = Some(Arc::new(move |state, params| {
            Box::pin(handler(state, params))
        }));
        self
    }

    /// A `Create` endpoint: `(Arc<S>, Params, B) -> T`. The provider assigns
    /// identity (insert/append). `B` is the typed request body (its schema is
    /// advertised); `T` is the created entity (the response schema).
    pub fn create<B, T, F, Fut>(mut self, handler: F) -> Self
    where
        B: serde::de::DeserializeOwned + HasSchema + Send + 'static,
        T: Serialize + HasSchema + 'static,
        F: Fn(Arc<S>, Params, B) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.method = Method::Create;
        self.handler = Some(erase_write(handler));
        self.body_schema = Some(B::schema());
        self.response_schema = Some(T::schema());
        self
    }

    /// A `Put` endpoint: `(Arc<S>, Params, DataStream) -> T`. Sinks a whole dataset
    /// (schema + streamed rows) — the write-dual of `list` — letting the provider use
    /// the schema as the contract (e.g. create a table if absent, then load rows).
    pub fn put<T, F, Fut>(mut self, handler: F) -> Self
    where
        T: Serialize + HasSchema + 'static,
        F: Fn(Arc<S>, Params, DataStream) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.method = Method::Put;
        self.handler = Some(erase_put(handler));
        // The body is a dataset envelope: a schema plus its rows.
        self.body_schema = Some(Schema::new(vec![
            Field::new("schema", DataType::Json, false),
            Field::new("rows", DataType::List(Box::new(DataType::Json)), false),
        ]));
        self.response_schema = Some(T::schema());
        self
    }

    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Override the declared schema with one resolved dynamically per request
    /// (e.g. a SQL table's columns), leaving the handler's value type as-is.
    pub fn data_type<SF, SFut>(mut self, resolver: SF) -> Self
    where
        SF: Fn(Arc<S>, Params) -> SFut + Send + Sync + 'static,
        SFut: Future<Output = Result<Schema>> + Send + 'static,
    {
        self.schema_resolver = Some(Box::new(move |state, params| {
            let fut = resolver(state, params);
            Box::pin(fut)
        }));
        self
    }

    fn into_entry(self) -> Entry<S> {
        Entry {
            pattern: Pattern::parse(&self.path),
            method: self.method,
            handler: match (self.handler, self.stream_handler) {
                (_, Some(stream)) => Handler::Stream(stream),
                (Some(unary), None) => Handler::Unary(unary),
                (None, None) => panic!(
                    "route is missing a handler; call `.get`/`.list`/`.list_stream`/`.create`/`.put`"
                ),
            },
            body_schema: self.body_schema.unwrap_or_else(Schema::empty),
            response_schema: self.response_schema.unwrap_or_else(Schema::empty),
            schema_resolver: self.schema_resolver,
            description: self.description,
            metadata: RouterMetadata {
                strategy: self.strategy,
                disposition: self.disposition.unwrap_or_default(),
                queryable: self.queryable,
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct RouterMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub strategy: Option<ListStrategy>,
    pub disposition: Disposition,
    pub queryable: bool,
}

#[derive(Debug, Clone)]
pub struct EndpointInfo {
    pub method: Method,
    /// The route pattern, e.g. `/repos/:owner/:name`.
    pub path: String,
    pub description: Option<String>,
    /// Names of the path captures the endpoint takes.
    pub params: Vec<String>,
    /// Request-body schema (empty for reads).
    pub body: Schema,
    /// Response schema.
    pub response: Schema,
    pub metadata: RouterMetadata,
}

impl Serialize for EndpointInfo {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry("method", &self.method)?;
        map.serialize_entry("path", &self.path)?;
        if let Some(description) = &self.description {
            map.serialize_entry("description", description)?;
        }
        map.serialize_entry("params", &self.params)?;
        let body = if self.method.is_write() {
            self.body.to_json_schema()
        } else {
            Value::Null
        };
        map.serialize_entry("body", &body)?;
        map.serialize_entry("response", &self.response.to_json_schema())?;
        map.serialize_entry("metadata", &self.metadata)?;
        map.end()
    }
}

/// Maps route patterns to handlers for one provider whose state is `S`.
pub struct Router<S> {
    routes: Vec<Entry<S>>,
    schema_source: Option<Arc<dyn SchemaSource>>,
}

impl<S> Default for Router<S> {
    fn default() -> Self {
        Router {
            routes: Vec::new(),
            schema_source: None,
        }
    }
}

impl<S: Send + Sync + 'static> Router<S> {
    pub fn new() -> Self {
        Router::default()
    }

    /// Wire the schema source (called by `serve` once the catalog DB is up), so
    /// dispatch can hand each handler the persisted schema for its endpoint.
    pub fn set_schema_source(&mut self, source: Arc<dyn SchemaSource>) {
        self.schema_source = Some(source);
    }

    /// Register a built [`Route`]. Panics at startup if the route has no
    /// handler (a registration bug, surfaced loudly).
    pub fn add(&mut self, route: Route<S>) {
        self.routes.push(route.into_entry());
    }

    /// Describe every endpoint (static schemas; dynamic resolvers not run) for
    /// introspection, the CLI listing, and Flight `ListFlights`.
    pub fn endpoints(&self) -> Vec<EndpointInfo> {
        self.routes.iter().map(Entry::info).collect()
    }

    pub async fn resolve(&self, state: Arc<S>, path: &str) -> Result<EndpointInfo> {
        // Prefer the List (data-plane) schema; fall back to Get for entity-only paths.
        let (route, params) = match self.match_route(path, Method::List).await {
            Ok(hit) => hit,
            Err(_) => self.match_route(path, Method::Get).await?,
        };
        let mut info = route.info();
        if let Some(resolver) = &route.schema_resolver {
            info.response = resolver(state, params).await?;
        }
        Ok(info)
    }

    async fn match_route(&self, path: &str, method: Method) -> Result<(&Entry<S>, Params)> {
        let (raw_path, query) = path.split_once('?').unwrap_or((path, ""));
        for route in &self.routes {
            if route.method == method
                && let Some(mut params) = route.pattern.match_path(raw_path)
            {
                params.set_query(query);
                if let Some(source) = &self.schema_source {
                    params.schema = source.schema(raw_path.to_string()).await?;
                }
                return Ok((route, params));
            }
        }
        bail!(
            "no {method} route matches `{raw_path}`. known routes:\n  {}",
            self.routes
                .iter()
                .map(|r| r.pattern.as_str())
                .collect::<Vec<_>>()
                .join("\n  ")
        )
    }

    /// Dispatch an explicit verb: find the route matching both `path` and
    /// `method`, and run it (with `body` for writes). This is how the same path
    /// can host several verbs — e.g. `get` and `list` on `/file/*path`.
    pub async fn dispatch(
        &self,
        state: Arc<S>,
        method: Method,
        path: &str,
        body: Option<Body>,
    ) -> Result<Response> {
        let (route, params) = self.match_route(path, method).await?;
        match &route.handler {
            Handler::Unary(handler) => handler(state, params, body).await,
            Handler::Stream(_) => bail!(
                "`{path}` is a data-plane read; call it through `read`, not `{method}` dispatch"
            ),
        }
    }

    /// Auto-pick the `List` route matching `path` and return it as a [`DataStream`]
    /// — the data plane, for callers that consume the Arrow stream (pipe, CLI,
    /// Flight `do_get`).
    pub async fn dispatch_read(&self, state: Arc<S>, path: &str) -> Result<DataStream> {
        let (route, base) = self.match_route(path, Method::List).await?;

        let schema = match &route.schema_resolver {
            Some(resolver) => resolver(state.clone(), base.clone()).await?,
            None => route.response_schema.clone(),
        };

        let chunks = match &route.handler {
            Handler::Stream(handler) => handler(state, base).await?,
            Handler::Unary(_) => bail!("`{path}` is not a data-plane read"),
        };

        Ok(DataStream { schema, chunks })
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        for route in &self.routes {
            if matches!(route.method, Method::List) && route.metadata.strategy.is_none() {
                anyhow::bail!(
                    "list method {:?} does not implement strategy",
                    route.pattern.as_str()
                )
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::Cursor;

    #[test]
    fn matches_and_captures_params() {
        let pattern = Pattern::parse("/repos/:owner/:name");
        let params = pattern.match_path("/repos/rust-lang/rust").unwrap();
        assert_eq!(params.get("owner").unwrap(), "rust-lang");
        assert_eq!(params.get("name").unwrap(), "rust");
    }

    #[test]
    fn rejects_literal_mismatch_and_wrong_length() {
        let pattern = Pattern::parse("/repos/:owner/:name");
        assert!(pattern.match_path("/users/torvalds").is_none());
        assert!(pattern.match_path("/repos/rust-lang").is_none());
        assert!(pattern.match_path("/repos/rust-lang/rust/extra").is_none());
    }

    #[test]
    fn missing_param_errors() {
        let pattern = Pattern::parse("/repos/:owner/:name");
        let params = pattern.match_path("/repos/a/b").unwrap();
        assert!(params.get("nope").is_err());
    }

    #[derive(Serialize, HasSchema)]
    struct Row {
        id: i64,
    }

    /// A write's response — metadata, never the entity.
    #[derive(Serialize, HasSchema)]
    struct WriteMeta {
        rows: i64,
    }

    async fn list_rows(_s: Arc<()>, _p: Params) -> Result<Page<Row>> {
        Ok(Page::new(Vec::new(), Cursor::empty()))
    }

    async fn get_row(_s: Arc<()>, _p: Params) -> Result<Row> {
        Ok(Row { id: 0 })
    }

    async fn put_rows(_s: Arc<()>, _p: Params, _d: DataStream) -> Result<WriteMeta> {
        Ok(WriteMeta { rows: 0 })
    }

    #[tokio::test]
    async fn resolve_picks_the_read_route_never_the_write() -> anyhow::Result<()> {
        let mut router: Router<()> = Router::new();
        // `put` is registered *first* on `/rows` — resolving must still answer with
        // the `list` row schema, not the write's `WriteMeta`.
        router.add(Route::new().path("/rows").put(put_rows));
        router.add(Route::new().path("/rows").list(list_rows));
        // A `get`-only path falls back to the entity plane.
        router.add(Route::new().path("/one").get(get_row));

        let rows = router.resolve(Arc::new(()), "/rows").await?;
        assert_eq!(rows.response, Row::schema());
        assert_eq!(rows.method, Method::List);

        let one = router.resolve(Arc::new(()), "/one").await?;
        assert_eq!(one.response, Row::schema());

        Ok(())
    }

    #[tokio::test]
    async fn resolve_ignores_write_only_paths() {
        let mut router: Router<()> = Router::new();
        router.add(Route::new().path("/sink").put(put_rows));
        assert!(router.resolve(Arc::new(()), "/sink").await.is_err());
    }
}
