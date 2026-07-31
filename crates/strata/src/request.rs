//! Little fluent builders for the reserved framework query params, so callers set a
//! resume cursor or a write disposition without hand-encoding `?cursor=` onto a path
//! string. Each wraps a path and yields the encoded path via `.path()`.

use crate::record::Disposition;
use crate::router::CURSOR_PARAM;

/// A read path plus an optional resume `cursor`.
pub struct ReadRequest {
    path: String,
    cursor: Option<String>,
}

impl ReadRequest {
    pub fn new(path: impl Into<String>) -> Self {
        ReadRequest {
            path: path.into(),
            cursor: None,
        }
    }

    /// Set (or clear, with `None`) the resume cursor.
    pub fn with_cursor(mut self, cursor: Option<String>) -> Self {
        self.cursor = cursor;
        self
    }

    /// The encoded path, with `?cursor=…` set or removed to match the cursor.
    pub fn path(self) -> String {
        set_param(&self.path, CURSOR_PARAM, self.cursor.as_deref())
    }
}

/// A write path plus an optional `disposition`.
pub struct WriteRequest {
    path: String,
    disposition: Option<Disposition>,
}

impl WriteRequest {
    pub fn new(path: impl Into<String>) -> Self {
        WriteRequest {
            path: path.into(),
            disposition: None,
        }
    }

    /// Set the write disposition (append vs. merge).
    pub fn with_disposition(mut self, disposition: Disposition) -> Self {
        self.disposition = Some(disposition);
        self
    }

    /// The encoded path, with `?disposition=…` set to match the disposition.
    pub fn path(self) -> String {
        set_param(
            &self.path,
            Disposition::PARAM,
            self.disposition.map(|d| d.as_param()),
        )
    }
}

/// Set query param `key` to `value` on `path`, replacing any prior value; `None`
/// removes it. Preserves the rest of the query.
fn set_param(path: &str, key: &str, value: Option<&str>) -> String {
    let (base, query) = path.split_once('?').unwrap_or((path, ""));
    let mut pairs: Vec<(String, String)> = serde_urlencoded::from_str(query).unwrap_or_default();
    pairs.retain(|(k, _)| k != key);
    if let Some(value) = value {
        pairs.push((key.to_string(), value.to_string()));
    }
    match serde_urlencoded::to_string(&pairs) {
        Ok(q) if !q.is_empty() => format!("{base}?{q}"),
        _ => base.to_string(),
    }
}
