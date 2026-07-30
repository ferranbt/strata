//! A dataset: the `(schema, rows)` unit that flows from a reader to a writer.
//!
//! It's exactly what a read produces — the rows of a `list`/`get` plus that
//! endpoint's resolved [`DataType`] schema — so a writer can use the schema as
//! the contract (create a table if absent, validate if present) and load the
//! rows. This is the framework-level shape behind "pipe readers to writers";
//! over Arrow Flight the same `(schema, rows)` rides natively in a `DoPut`.

use anyhow::{Result, bail};
use arrow_array::RecordBatch;
use futures::stream::{BoxStream, StreamExt};
use schema::Schema;
use serde::Serialize;
use serde_json::Value;

use crate::{
    DataStream,
    page::Cursor,
    record::{Batch, BatchPage},
};

/// How a sink should apply a written dataset. Rides as metadata on the existing
/// `put` verb (the reserved `disposition` query param) rather than a new verb, so
/// every sink shares one write surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub enum Disposition {
    /// Insert every row (the historical behavior). Re-running adds duplicates.
    #[default]
    Append,
    /// Idempotent write-by-key: upsert each row on the dataset's key fields
    /// (insert, or update the non-key columns on conflict). Requires the schema
    /// to declare a key; this is what makes a re-fetching pipe dedup itself.
    Merge,
}

impl Disposition {
    /// The reserved `put` query param that selects the disposition.
    pub const PARAM: &str = "disposition";

    /// Parse the param value (`append` | `merge`/`upsert`); defaults to `Append`
    /// when absent.
    pub fn from_param(value: Option<&str>) -> Result<Self> {
        match value {
            None | Some("append") => Ok(Disposition::Append),
            Some("merge") | Some("upsert") => Ok(Disposition::Merge),
            Some(other) => bail!("unknown write disposition `{other}` (append|merge)"),
        }
    }

    /// The param value (inverse of [`from_param`](Self::from_param)).
    pub fn as_param(self) -> &'static str {
        match self {
            Disposition::Append => "append",
            Disposition::Merge => "merge",
        }
    }
}

/// The reader→writer unit: the rows as Arrow [`Records`] (strata's internal
/// currency) paired with the **native** `schema` that governs how a sink stores
/// them. `schema` is carried explicitly rather than derived from the Arrow
/// schema, so `Decimal`/`Json` fidelity survives — the Arrow mapping folds those
/// to `Utf8`, and a sink's DDL needs the real types. It's typically a
/// `List(Struct(..))` (a list endpoint's response) or a bare `Struct`.
///
/// Future direction: `records` should become a *stream* of batches following
/// `schema` (a checkpointed cursor-driven stream) so a writer can consume
/// unbounded data without materializing it — the materialized batches here are
/// the stand-in until the streaming list driver lands.
#[derive(Debug, Clone)]
pub struct Dataset {
    pub schema: Schema,
    pub records: Batch,
}

impl Dataset {
    pub fn new(schema: Schema, records: Batch) -> Self {
        Dataset { schema, records }
    }

    /// Build a dataset from typed `rows`: the schema is `T`'s [`Schema`] (with any
    /// `#[schema(key)]`/annotations it declares) and the rows are Arrow-encoded
    /// against it. The typed-input counterpart of a `put`.
    pub fn of<T: serde::Serialize + schema::HasSchema>(rows: &[T]) -> Result<Self> {
        let schema = T::schema();
        let records = Batch::encode(schema.clone(), rows)?;
        Ok(Dataset::new(schema, records))
    }

    /// Interim bridge: decode the Arrow rows to JSON for sinks that aren't yet
    /// Arrow-native. This is the one labeled Arrow→JSON on the write path; Phase B
    /// removes it as each sink binds Arrow columns directly.
    pub fn to_json_rows(&self) -> Result<Vec<Value>> {
        self.records.to_json_rows(&self.schema)
    }

    /// Bridge to a single-chunk [`DataStream`], until readers produce batches lazily.
    pub fn into_stream(self) -> DataStream {
        let chunk = BatchPage {
            data: self.records,
            cursor: None,
        };
        DataStream {
            schema: self.schema,
            chunks: futures::stream::once(async move { Ok(chunk) }).boxed(),
        }
    }
}
