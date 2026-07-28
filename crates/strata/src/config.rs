use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde::de::value::{Error, MapDeserializer};
use strata_types::Pipe;

#[derive(Debug, Default, Clone, Deserialize)]
pub struct ProviderConfig {
    #[serde(default)]
    pub backend: String,
    #[serde(default)]
    pub mount: String,
    #[serde(flatten)]
    params: HashMap<String, String>,
}

impl ProviderConfig {
    pub fn decode<T: DeserializeOwned>(&self) -> Result<T> {
        let de = MapDeserializer::<_, Error>::new(self.params.clone().into_iter());
        T::deserialize(de).context("decoding provider config")
    }
}

/// The parsed config file: provider instances keyed by table name, plus any
/// declared pipes.
///
/// Pipes are an array of tables, each a [`Pipe`] (source/destination endpoints):
///
/// ```toml
/// [[pipe]]
/// source = { mount = "congress", path = "/bill/119/hr?limit=20" }
/// destination = { mount = "postgres", path = "/tables/bills" }
/// ```
#[derive(Debug, Default, Deserialize)]
pub struct Config {
    #[serde(default)]
    provider: HashMap<String, ProviderConfig>,
    #[serde(default)]
    pipe: Vec<Pipe>,
}

impl Config {
    /// Parse a config file (errors if it exists but is malformed).
    pub fn load(path: impl AsRef<Path>) -> Result<Config> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let mut config: Config =
            toml::from_str(&text).with_context(|| format!("parsing config {}", path.display()))?;

        // If either mount or backend not found, use the reference name
        // TODO: This is legacy and it will be removed once a better way to define
        // mounts and providers is found.
        for (ref_name, provider) in &mut config.provider {
            if provider.mount.is_empty() {
                provider.mount = ref_name.clone();
            }
            if provider.backend.is_empty() {
                provider.backend = ref_name.clone();
            }
        }
        Ok(config)
    }

    /// Provider instances as in stable mount order.
    pub fn providers(&self) -> impl Iterator<Item = &ProviderConfig> {
        let mut entries: Vec<_> = self.provider.values().collect();
        entries.sort_by(|a, b| a.mount.cmp(&b.mount));
        entries.into_iter()
    }

    /// The declared pipes, in file order.
    pub fn pipes(&self) -> &[Pipe] {
        &self.pipe
    }
}
