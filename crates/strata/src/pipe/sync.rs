//! The sync strategy layer: how a walk over a `list` endpoint advances, chunk by
//! chunk.
//!
//! This is an *external* system that only consumes router primitives — `read` (the
//! cursor-driven [`DataStream`](crate::dataset::DataStream)) and the provider's
//! declared [`ListStrategy`]. The router itself never interprets the strategy; it
//! just reports it. [`pipe`](super) is the mover that drives one live stream,
//! writing each chunk and persisting the resume point, and asks [`advance`] — after
//! each chunk — whether to keep pulling or stop.
//!
//! The two strategies differ only in how they read the tail:
//!
//! - [`Offset`](ListStrategy::Offset): a finite backfill whose cursor reaches
//!   `None` at the end. The stream self-terminates, so the tail is `next == None`.
//! - [`NextLink`](ListStrategy::NextLink): an unbounded forward walk whose cursor
//!   is *always* set — `None` never comes. The tail is instead an **empty page**: a
//!   chunk that brought no rows means we've reached the live edge for now, so the
//!   driver stops pulling and drops the (otherwise infinite) stream.

use crate::page::ListStrategy;
use crate::record::BatchPage;

/// What the sync driver should do after writing one [`BatchPage`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Progress {
    /// More to pull right now: keep consuming the live stream. The token is this
    /// chunk's resume checkpoint — persist it before pulling the next chunk so a
    /// crash resumes from here.
    Continue(String),
    /// Caught up for now: stop pulling, drop the stream, and re-poll later. The
    /// stored token is where the *next* run begins (`None` = start over).
    CaughtUp(Option<String>),
}

/// Decide how the walk advances after `chunk`, per the endpoint's `strategy`.
///
/// `Offset` follows the cursor until it goes `None`; `NextLink`'s cursor never goes
/// `None`, so it stops when a page comes back empty. See the module docs.
pub fn advance(strategy: ListStrategy, chunk: &BatchPage) -> Progress {
    let next = chunk.cursor.as_ref().and_then(|c| c.next.clone());
    match strategy {
        // Finite backfill: drain while the cursor advances; `None` is the tail.
        // Caught up → start over next run (dedup-based early stop is deferred).
        ListStrategy::Offset => match next {
            Some(token) => Progress::Continue(token),
            None => Progress::CaughtUp(None),
        },
        // Incremental tail: the cursor is always set, so an empty page — not
        // `None` — marks the live edge. When caught up, resume from the same
        // forward position so the next run picks up whatever has since arrived.
        ListStrategy::NextLink => {
            if chunk.data.row_count() == 0 {
                Progress::CaughtUp(next)
            } else {
                match next {
                    Some(token) => Progress::Continue(token),
                    None => Progress::CaughtUp(None),
                }
            }
        }
    }
}
