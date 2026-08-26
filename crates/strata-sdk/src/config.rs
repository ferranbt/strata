use std::collections::HashMap;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde::de::value::{Error, MapDeserializer};

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

    /// A single named setting, or `None` if the instance didn't declare it.
    pub fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(String::as_str)
    }

    /// The whole config flattened back to strings — how it crosses to an
    /// out-of-process provider, which rebuilds it with the same field names.
    pub fn to_map(&self) -> HashMap<String, String> {
        let mut map = self.params.clone();
        map.insert("backend".to_string(), self.backend.clone());
        map.insert("mount".to_string(), self.mount.clone());
        map
    }
}

