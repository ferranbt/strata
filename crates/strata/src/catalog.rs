//! A mount-scoped view of the catalog DB, handed to a provider so its handlers can
//! read their own persisted schema/annotations. Cheap to clone (the pool is an
//! `Arc` internally); `None` when `serve` runs without a catalog DB.

use anyhow::Result;
use database::Database;
use schema::Schema;
use strata_types::Endpoint;

use crate::router::{BoxFuture, SchemaSource};

/// The catalog, scoped to one provider's `mount`. A handler queries it by the
/// endpoint `path` it's serving; the mount is fixed at construction.
#[derive(Clone, Debug)]
pub struct Catalog {
    db: Database,
    mount: String,
}

impl Catalog {
    pub fn new(db: Database, mount: impl Into<String>) -> Self {
        Catalog {
            db,
            mount: mount.into(),
        }
    }
}

impl SchemaSource for Catalog {
    fn schema(&self, path: String) -> BoxFuture<'static, Result<Option<Schema>>> {
        let db = self.db.clone();
        let mount = self.mount.clone();
        Box::pin(async move { db.get_source_schema(&Endpoint::new(&mount, path)).await })
    }
}
