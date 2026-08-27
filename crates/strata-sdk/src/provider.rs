//! Providers and the erased view of them.
//!
//! A provider defines its endpoints by registering routes on a `Router<Self>`.
//! Because each provider has its own state type `S`, the routers have different
//! types — so we erase `S` behind the object-safe [`ProviderObject`] trait and
//! hand out `Box<dyn ProviderObject>` instead.

use std::sync::Arc;

use anyhow::Result;

use crate::config::ProviderConfig;
use crate::record::DataStream;
use crate::router::{Body, BoxFuture, EndpointInfo, Method, Response, Router};

/// Implemented by each concrete provider. Knows its state type and how to wire
/// its routes. This is the typed, ergonomic surface provider authors write.
pub trait Provider: Sized + Send + Sync + 'static {
    /// The backend name, e.g. `github` — used as the default mount point and to
    /// select the backend from config.
    fn name() -> &'static str;

    /// The settings this provider takes, for a host to report before mounting.
    fn config_schema() -> schema::Schema {
        schema::Schema::empty()
    }

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
    /// Every endpoint, statically described (dynamic resolvers not run).
    fn endpoints(&self) -> Vec<EndpointInfo>;
    /// The read endpoint matching a concrete `path`, with its response schema
    /// resolved (running a dynamic resolver if the route has one).
    fn resolve<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<EndpointInfo>>;
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

/// Build `P`: wire its routes, check them, and construct its state from `config`.
pub fn instance<P: Provider>(config: &ProviderConfig) -> Result<Box<dyn ProviderObject>> {
    let mut router = Router::new();
    P::register(&mut router);
    router.validate()?;
    Ok(Box::new(Instance {
        state: Arc::new(P::new(config)?),
        router,
    }))
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
    fn endpoints(&self) -> Vec<EndpointInfo> {
        self.router.endpoints()
    }

    fn resolve<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<EndpointInfo>> {
        let state = self.state.clone();
        let path = path.to_string();
        Box::pin(async move { self.router.resolve(state, &path).await })
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

