//! The provider-authoring surface: what a provider needs to define its endpoints
//! and be served, without any of the host's query surfaces.

// `#[config]` and `#[derive(Provider)]` generate `::strata_sdk::` paths, which
// have to resolve inside this crate too.
extern crate self as strata_sdk;

pub mod config;
pub mod datagen;
pub mod dummy;
pub mod page;
pub mod plugin;
pub mod provider;
pub mod record;
pub mod router;
pub mod sql;
pub mod testkit;

pub use config::ProviderConfig;
pub use page::{Cursor, Page};
pub use provider::{Provider, ProviderObject, Registry};
pub use strata_sdk_macro::{Provider, config};
pub use record::{Batch, BatchPage, DataStream, Disposition};
pub use router::{Body, EndpointInfo, Method, Params, Response, Route, Router};
