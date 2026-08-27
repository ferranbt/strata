//! The Pipe datastore: where a running pipe's progress is persisted.
//!
//! The sync driver checkpoints against a [`PipeStore`] after each durably-written
//! chunk. It's a trait — carried into [`run_pass`](super::run_pass) as a generic, so
//! it's monomorphized rather than dyn-dispatched — so the driver isn't welded to
//! Postgres: a later store can persist stats and timing too, or none at all for a
//! DB-less one-shot `pipe` and tests.
//!
//! For now the only method is [`checkpoint`](PipeStore::checkpoint), and the only
//! implementation is [`PgPipeStore`] (the catalog `pipes` table).

use anyhow::Result;
use strata_database::Database;
use uuid::Uuid;

use crate::router::BoxFuture;

/// The durable state of a running pipe. The sync driver reports progress here; the
/// implementation decides what to keep and where.
pub trait PipeStore: Send + Sync {
    /// Persist a pipe's advancing resume point: the `cursor` for the pipe `id`, so a
    /// restart continues from it. Called after each chunk is durably written.
    fn checkpoint(&self, id: Uuid, cursor: Option<String>) -> BoxFuture<'static, Result<()>>;
}

/// The production store: persists to the catalog `pipes` table.
pub struct PgPipeStore(pub Database);

impl PipeStore for PgPipeStore {
    fn checkpoint(&self, id: Uuid, cursor: Option<String>) -> BoxFuture<'static, Result<()>> {
        let db = self.0.clone();
        Box::pin(async move { db.update_cursor(id, cursor).await })
    }
}

/// A no-op store — for a DB-less `pipe` and tests: the cursor advances in memory for
/// the run and nothing is persisted.
pub struct NoPipeStore;

impl PipeStore for NoPipeStore {
    fn checkpoint(&self, _id: Uuid, _cursor: Option<String>) -> BoxFuture<'static, Result<()>> {
        Box::pin(async { Ok(()) })
    }
}
