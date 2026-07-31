//! How a sink applies a written dataset.

use anyhow::{Result, bail};
use serde::Serialize;

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
