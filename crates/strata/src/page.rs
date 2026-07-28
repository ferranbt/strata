use anyhow::Context;
use serde::{Deserialize, Serialize};

/// The cursor returned with a [`Page`]: the resume position for the next call.
/// `next` is `None` at the tail of the scan (nothing further right now).
///
/// The token is an opaque JSON encoding of the provider's own cursor state — the
/// framework never interprets it. A provider builds one with [`Cursor::new`] and
/// reads it back via `Params::cursor`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Cursor {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,
    // The total number of elements in the set. Empty if it is not known
    pub total: Option<u32>,
}

impl Cursor {
    /// A cursor with nothing to continue to (a single, complete page).
    pub fn empty() -> Self {
        Cursor::default()
    }

    /// A cursor whose next position is `state`, serialized to a JSON token — the
    /// inverse of `Params::cursor`.
    pub fn new<T: Serialize>(state: &T) -> anyhow::Result<Cursor> {
        let json = serde_json::to_string(state).context("encoding cursor")?;
        Ok(Cursor {
            next: Some(json),
            total: None,
        })
    }
}

/// One page of a list endpoint: the items plus the cursor to continue from.
pub struct Page<T> {
    pub items: Vec<T>,
    pub cursor: Cursor,
}

impl<T> Page<T> {
    pub fn new(items: Vec<T>, cursor: Cursor) -> Self {
        Page { items, cursor }
    }
}

/// How the external sync layer walks a list endpoint. A provider declares one per
/// `list` route (`.strategy(…)`); the router only stores the signal — the sync
/// system alongside `pipe` is what interprets it. The two differ in where a run
/// starts, when it stops, and what a re-sync does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListStrategy {
    /// Finite backfill by offset/limit. `next` reaches `None` at the tail, so a run
    /// starts from the beginning and drains to completion. A re-sync re-scans from
    /// the start and stops once it has seen enough already-known rows (dedup-based
    /// catch-up).
    Offset,
    /// Incremental tail, walked old→new. `next` is *always* set, so the scan never
    /// self-terminates on `None`: a run resumes from the last persisted cursor and
    /// stops when a page yields no new rows. The cursor advances forward across runs.
    NextLink,
}
