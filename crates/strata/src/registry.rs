use std::collections::HashMap;

use anyhow::{Result, anyhow, bail};
use serde_json::{Value, json};

use strata_sdk::config::ProviderConfig;
use strata_sdk::provider::{Provider, ProviderObject, instance};
use strata_sdk::record::DataStream;
use strata_sdk::router::{Body, BoxFuture, EndpointInfo, Method, Response};

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
        self.providers
            .insert(mount.to_string(), instance::<P>(config)?);
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

pub fn split_mount(path: &str) -> Result<(&str, String)> {
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
    use strata_sdk::dummy::Dummy;

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
