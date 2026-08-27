//! Providers and the registry that holds them.
//!
//! A provider defines its endpoints by registering routes on a `Router<Self>`.
//! Because each provider has its own state type `S`, the routers have different
//! types — so we erase `S` behind the object-safe [`ProviderObject`] trait and
//! store `Box<dyn ProviderObject>` in the [`Registry`].

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};

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

    /// Mount an already-built provider — the seam an out-of-process one comes in
    /// through, since it has nothing to construct from a [`ProviderConfig`].
    pub fn mount_object(&mut self, mount: &str, provider: Box<dyn ProviderObject>) -> Result<()> {
        if self.providers.contains_key(mount) {
            bail!("mount point `{mount}` is already in use");
        }
        self.providers.insert(mount.to_string(), provider);
        Ok(())
    }

    /// Every mount, so a host can attach a schema source to each in turn.
    pub fn mounts(&self) -> Vec<String> {
        self.names()
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

fn split_mount(path: &str) -> Result<(&str, String)> {
    let (raw, query) = path.split_once('?').unwrap_or((path, ""));
    let mut segments = raw.split('/').filter(|s| !s.is_empty());
    let mount = segments
        .next()
        .ok_or_else(|| anyhow!("`{path}` names no mount"))?;
    let rest = format!("/{}", segments.collect::<Vec<_>>().join("/"));
    match query.is_empty() {
        true => Ok((mount, rest)),
        false => Ok((mount, format!("{rest}?{query}"))),
    }
}

impl ProviderObject for Registry {
    fn endpoints(&self) -> Vec<EndpointInfo> {
        let mut all = Vec::new();
        for mount in self.names() {
            let Ok(provider) = self.get(&mount) else {
                continue;
            };
            for mut endpoint in provider.endpoints() {
                endpoint.path = format!("/{mount}{}", endpoint.path);
                all.push(endpoint);
            }
        }
        all
    }

    fn resolve<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<EndpointInfo>> {
        Box::pin(async move {
            let (mount, rest) = split_mount(path)?;
            let mut endpoint = self.get(mount)?.resolve(&rest).await?;
            endpoint.path = format!("/{mount}{}", endpoint.path);
            Ok(endpoint)
        })
    }

    fn read<'a>(&'a self, path: &'a str) -> BoxFuture<'a, Result<DataStream>> {
        Box::pin(async move {
            let (mount, rest) = split_mount(path)?;
            self.get(mount)?.read(&rest).await
        })
    }

    fn invoke<'a>(
        &'a self,
        method: Method,
        path: &'a str,
        body: Option<Body>,
    ) -> BoxFuture<'a, Result<Response>> {
        Box::pin(async move {
            let (mount, rest) = split_mount(path)?;
            self.get(mount)?.invoke(method, &rest, body).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dummy::Dummy;

    fn registry() -> Result<Registry> {
        let mut registry = Registry::new();
        registry.mount::<Dummy>("gen", &ProviderConfig::default())?;
        Ok(registry)
    }

    #[test]
    fn splits_the_mount_off_a_path_keeping_the_query() -> Result<()> {
        assert_eq!(split_mount("/gen/data")?, ("gen", "/data".to_string()));
        assert_eq!(
            split_mount("/gen/data?rows=5&limit=2")?,
            ("gen", "/data?rows=5&limit=2".to_string())
        );
        assert_eq!(
            split_mount("/gen/tables/t")?,
            ("gen", "/tables/t".to_string())
        );
        assert!(split_mount("/").is_err());
        Ok(())
    }

    /// The registry answers by path like any other provider, dispatching on the
    /// leading segment.
    #[tokio::test]
    async fn reads_through_the_mount_prefix() -> Result<()> {
        let registry = registry()?;
        let schema = serde_json::json!({
            "fields": [{ "name": "id", "data_type": "Int64", "nullable": false }]
        });
        let encoded = urlencoding::encode(&schema.to_string()).into_owned();
        let path = format!("/gen/data?schema={encoded}&rows=3");

        let stream = ProviderObject::read(&registry, &path).await?;
        let schema = stream.schema.clone();
        let page = stream.first().await?.expect("a page");
        assert_eq!(page.data.to_json_rows(&schema)?.len(), 3);
        Ok(())
    }

    /// Endpoints come back mount-prefixed, so one listing addresses every mount.
    #[test]
    fn endpoints_carry_their_mount() -> Result<()> {
        let registry = registry()?;
        let paths: Vec<String> = ProviderObject::endpoints(&registry)
            .into_iter()
            .map(|e| e.path)
            .collect();
        assert_eq!(paths, vec!["/gen/data".to_string()]);
        Ok(())
    }

    #[tokio::test]
    async fn an_unknown_mount_errors() -> Result<()> {
        let registry = registry()?;
        let error = match ProviderObject::read(&registry, "/nope/data").await {
            Err(error) => error,
            Ok(_) => panic!("no such mount"),
        };
        assert!(error.to_string().contains("nothing mounted at `nope`"));
        Ok(())
    }
}
