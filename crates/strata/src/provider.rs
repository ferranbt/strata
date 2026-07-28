//! Providers and the registry that holds them.
//!
//! A provider defines its endpoints by registering routes on a `Router<Self>`.
//! Because each provider has its own state type `S`, the routers have different
//! types — so we erase `S` behind the object-safe [`ProviderObject`] trait and
//! store `Box<dyn ProviderObject>` in the [`Registry`].

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use schema::Schema;
use serde_json::{Value, json};

use crate::catalog::Catalog;
use crate::config::ProviderConfig;
use crate::dataset::{DataStream, Disposition};
use crate::page::ListStrategy;
use crate::router::{Body, BoxFuture, EndpointInfo, Method, Response, Router};

/// Implemented by each concrete provider. Knows its state type and how to wire
/// its routes. This is the typed, ergonomic surface provider authors write.
pub trait Provider: Sized + Send + Sync + 'static {
    /// The backend name, e.g. `github` — used as the default mount point and to
    /// select the backend from config.
    fn name() -> &'static str;

    /// Construct provider state (build HTTP clients, read credentials, etc.) from
    /// the instance's [`ProviderConfig`], falling back to env vars as needed.
    fn new(config: &ProviderConfig) -> Result<Self>;

    /// Register every endpoint on the router.
    fn register(router: &mut Router<Self>);
}

/// State-erased view of a provider, so providers with different state types can
/// share one collection. Object-safe: `call` returns a boxed future rather than
/// being an `async fn`.
pub trait ProviderObject: Send + Sync {
    fn name(&self) -> &str;
    /// Wire the mount-scoped [`Catalog`] into this provider's router, so its
    /// handlers can read their own persisted annotations.
    fn set_catalog(&mut self, catalog: Catalog);
    fn routes(&self) -> Vec<String>;
    /// Machine-readable description of every endpoint (path, params, response
    /// schema), for introspection.
    fn endpoints(&self) -> Vec<EndpointInfo>;
    /// Static response `DataType` of every endpoint, paired with its pattern.
    fn endpoint_schemas(&self) -> Vec<(String, Schema)>;
    /// Response `DataType` of the endpoint matching `path`, resolved (running a
    /// dynamic schema resolver if the endpoint has one).
    fn resolve_schema<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Schema>>;
    /// The declared [`ListStrategy`] of the `list` route matching `path`, for the
    /// external sync layer to drive its walk. `None` if no list route matches.
    fn strategy(&self, path: &str) -> Option<ListStrategy>;
    /// The declared write [`Disposition`] of the `list` route matching `path` —
    /// whether a pipe should merge (source re-emits updates) or append. Defaults to
    /// `Append`.
    fn disposition(&self, path: &str) -> Disposition;
    /// Auto-pick the `List` read by path and return it as a [`DataStream`] — the
    /// data plane, for callers that consume the Arrow stream (pipe, CLI, Flight
    /// `do_get`). The router loops the provider's single-page handler internally.
    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DataStream>>;
    /// Dispatch an explicit verb by `(method, path)`, with an optional request
    /// [`Body`] (`Some` for writes: JSON `meta` for create, an Arrow dataset for put).
    fn invoke<'a>(
        &'a self,
        method: Method,
        path: &'a str,
        body: Option<Body>,
    ) -> BoxFuture<'a, Result<Response>>;
}

/// Pairs a provider's state with its router. Generic bridge from a typed
/// [`Provider`] to the erased [`ProviderObject`].
struct Instance<S> {
    state: Arc<S>,
    router: Router<S>,
}

impl<S: Send + Sync + 'static> ProviderObject for Instance<S>
where
    S: Provider,
{
    fn name(&self) -> &str {
        S::name()
    }

    fn set_catalog(&mut self, catalog: Catalog) {
        self.router.set_catalog(catalog);
    }

    fn routes(&self) -> Vec<String> {
        self.router.patterns()
    }

    fn endpoints(&self) -> Vec<EndpointInfo> {
        self.router.endpoints()
    }

    fn endpoint_schemas(&self) -> Vec<(String, Schema)> {
        self.router.schemas()
    }

    fn resolve_schema<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<Schema>> {
        let state = self.state.clone();
        let path = path.to_string();
        Box::pin(async move {
            self.router
                .resolve_schema(state, &path)
                .await
                .ok_or_else(|| anyhow!("no endpoint matches `{path}`"))?
        })
    }

    fn strategy(&self, path: &str) -> Option<ListStrategy> {
        self.router.strategy(path)
    }

    fn disposition(&self, path: &str) -> Disposition {
        self.router.disposition(path)
    }

    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DataStream>> {
        let state = self.state.clone();
        let path = path.to_string();
        Box::pin(async move { self.router.dispatch_read(state, &path).await })
    }

    fn invoke<'a>(
        &'a self,
        method: Method,
        path: &'a str,
        body: Option<Body>,
    ) -> BoxFuture<'a, Result<Response>> {
        let state = self.state.clone();
        let path = path.to_string();
        Box::pin(async move { self.router.dispatch(state, method, &path, body).await })
    }
}

/// Holds all mounted provider instances, keyed by **mount point** (not backend
/// name) — so several instances of one backend can coexist at different mounts.
#[derive(Default)]
pub struct Registry {
    providers: HashMap<String, Box<dyn ProviderObject>>,
}

impl Registry {
    pub fn new() -> Self {
        Registry::default()
    }

    /// Construct provider `P` from `config` and mount it at `mount`. Fails if the
    /// mount point is already in use.
    pub fn mount<P: Provider>(&mut self, mount: &str, config: &ProviderConfig) -> Result<()> {
        if self.providers.contains_key(mount) {
            bail!("mount point `{mount}` is already in use");
        }
        let mut router = Router::new();
        P::register(&mut router);
        router.validate()?;
        let instance = Instance {
            state: Arc::new(P::new(config)?),
            router,
        };
        self.providers.insert(mount.to_string(), Box::new(instance));
        Ok(())
    }

    pub fn set_catalog(&mut self, db: database::Database) {
        for (mount, provider) in self.providers.iter_mut() {
            provider.set_catalog(Catalog::new(db.clone(), mount.clone()));
        }
    }

    /// Look up a mounted provider by its mount point.
    pub fn get(&self, mount: &str) -> Result<&dyn ProviderObject> {
        self.providers.get(mount).map(Box::as_ref).ok_or_else(|| {
            anyhow!(
                "nothing mounted at `{mount}`. mounted: {}",
                self.names().join(", ")
            )
        })
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.providers.keys().cloned().collect();
        names.sort();
        names
    }

    /// Describe one provider's endpoints as `{ "endpoints": [...] }`.
    pub fn describe(&self, name: &str) -> Result<Value> {
        let provider = self.get(name)?;
        Ok(json!({ "endpoints": provider.endpoints() }))
    }

    /// Describe every provider, keyed by name:
    /// `{ "<provider>": { "endpoints": [...] }, ... }`.
    pub fn describe_all(&self) -> Value {
        let mut map = serde_json::Map::new();
        for name in self.names() {
            // Safe: names() comes from the map we're iterating.
            if let Ok(desc) = self.describe(&name) {
                map.insert(name, desc);
            }
        }
        Value::Object(map)
    }
}
