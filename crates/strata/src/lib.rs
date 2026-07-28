//! strata: define a data provider's API as typed Rust endpoints, register them
//! in a router, and call any endpoint by path.

pub mod catalog;
pub mod config;
pub mod datagen;
pub mod dataset;
pub mod flight;
pub mod graphql;
pub mod harness;
pub mod page;
pub mod pipe;
pub mod provider;
pub mod providers;
pub mod record;
pub mod request;
pub mod router;
#[cfg(test)]
pub mod testkit;

use std::path::Path;

use anyhow::Result;

pub use catalog::Catalog;
pub use config::{Config, ProviderConfig};
pub use dataset::{Chunk, DataStream, Dataset, Disposition};
pub use page::{Cursor, Page};
pub use provider::{Provider, ProviderObject, Registry};
pub use router::{Body, EndpointInfo, Method, Params, Response, Router};

/// Config file consulted by [`registry`] when no explicit path is given.
pub const DEFAULT_CONFIG: &str = "strata.toml";

/// Every known backend. Used to build the default registry (all backends at
/// their default mounts) when there's no config file.
pub const BACKENDS: &[&str] = &[
    "clickhouse",
    "congress",
    "dummy",
    "fs",
    "github",
    "google",
    "iceberg",
    "mysql",
    "postgres",
    "sqlite",
    "rss",
    "spotify",
    "substack",
    "linear",
];

/// Build the registry: from `strata.toml` if present, else every backend mounted
/// at its default mount point (its name).
pub fn registry() -> Result<Registry> {
    if Path::new(DEFAULT_CONFIG).exists() {
        registry_from_config(DEFAULT_CONFIG)
    } else {
        registry_default()
    }
}

/// The config file actually in effect: the explicit `--config`, else
/// `strata.toml` when it exists, else `None` (the all-backends default has no
/// file). Used by `serve` to load pipe definitions from the same source the
/// registry was built from.
pub fn config_path(explicit: Option<&str>) -> Option<String> {
    match explicit {
        Some(path) => Some(path.to_string()),
        None => Path::new(DEFAULT_CONFIG)
            .exists()
            .then(|| DEFAULT_CONFIG.to_string()),
    }
}

/// Build the registry from a config file (errors if missing/malformed). Each
/// `[provider.<name>]` is mounted: `backend` selects the type (default: the
/// table name), `mount` the router location (default: the table name).
pub fn registry_from_config(path: impl AsRef<Path>) -> Result<Registry> {
    let config = Config::load(path)?;
    let mut registry = Registry::new();
    for settings in config.providers() {
        let backend = settings.backend.clone();
        let mount = settings.mount.clone();
        mount_backend(&mut registry, backend.as_str(), mount.as_str(), settings)?;
    }
    mount_dummy(&mut registry)?;
    Ok(registry)
}

/// The generated source is first-class: always mounted at `dummy`, with or without
/// a config, so tests and benchmarks have a source everywhere. It's stateless, so
/// there's nothing to configure — a config may still mount its own (e.g. at another
/// name), in which case that one wins.
fn mount_dummy(registry: &mut Registry) -> Result<()> {
    if registry.names().iter().any(|name| name == DUMMY) {
        return Ok(());
    }
    mount_backend(registry, DUMMY, DUMMY, &ProviderConfig::default())
}

const DUMMY: &str = "dummy";

/// Build a registry with every backend at its default mount, settings from env
/// only (the no-config default).
pub fn registry_default() -> Result<Registry> {
    let empty = ProviderConfig::default();
    let mut registry = Registry::new();
    for backend in BACKENDS {
        mount_backend(&mut registry, backend, backend, &empty)?;
    }
    Ok(registry)
}

/// Map a backend name to its concrete provider type and mount it. The one place
/// that enumerates known backends; config parsing stays generic.
fn mount_backend(
    registry: &mut Registry,
    backend: &str,
    mount: &str,
    config: &ProviderConfig,
) -> Result<()> {
    match backend {
        "clickhouse" => registry.mount::<providers::clickhouse::Clickhouse>(mount, config),
        "congress" => registry.mount::<providers::congress::Congress>(mount, config),
        "dummy" => registry.mount::<providers::dummy::Dummy>(mount, config),
        "fs" => registry.mount::<providers::fs::Fs>(mount, config),
        "github" => registry.mount::<providers::github::Github>(mount, config),
        "google" => registry.mount::<providers::google::Google>(mount, config),
        "iceberg" => registry.mount::<providers::iceberg::Iceberg>(mount, config),
        "mysql" => registry.mount::<providers::mysql::Mysql>(mount, config),
        "postgres" => registry.mount::<providers::postgres::Postgres>(mount, config),
        "sqlite" => registry.mount::<providers::sqlite::Sqlite>(mount, config),
        "rss" => registry.mount::<providers::rss::Rss>(mount, config),
        "spotify" => registry.mount::<providers::spotify::Spotify>(mount, config),
        "substack" => registry.mount::<providers::substack::Substack>(mount, config),
        "linear" => registry.mount::<providers::linear::Linear>(mount, config),
        other => anyhow::bail!("unknown backend `{other}` (known: {})", BACKENDS.join(", ")),
    }
}
