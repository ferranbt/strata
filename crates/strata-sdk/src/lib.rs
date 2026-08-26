//! The provider-authoring surface: what a provider needs to define its endpoints
//! and be served, without any of the host's query surfaces.

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
pub use record::{Batch, BatchPage, DataStream, Disposition};
pub use router::{Body, EndpointInfo, Method, Params, Response, Route, Router};
