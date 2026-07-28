use std::sync::Arc;

use anyhow::{Result, anyhow};
use schema::Schema;
use serde::{Deserialize, Serialize};

use crate::datagen::Generator;
use crate::page::{Cursor, ListStrategy};
use crate::provider::Provider;
use crate::record::RecordPage;
use crate::router::{Params, Route, Router};

const DEFAULT_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 1000;

const DEFAULT_ROWS: u32 = 1000;
const MAX_ROWS: u32 = 10_000;

pub struct Dummy;

impl Provider for Dummy {
    fn name() -> &'static str {
        "dummy"
    }

    fn new(_config: &crate::config::ProviderConfig) -> Result<Self> {
        Ok(Dummy)
    }

    fn register(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/data")
                .list_records(generate)
                .data_type(types_schema)
                .strategy(ListStrategy::Offset)
                .description("Generated rows; `types` is a JSON DataType"),
        );
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct DummyCursor {
    #[serde(default)]
    offset: u32,
}

fn types(p: &Params) -> Result<Schema> {
    let raw = p
        .query("schema")
        .ok_or_else(|| anyhow!("`schema` is required: a JSON-encoded DataType"))?;
    serde_json::from_str(raw).map_err(|e| anyhow!("decoding `schema`: {e}"))
}

async fn types_schema(_d: Arc<Dummy>, p: Params) -> Result<Schema> {
    types(&p)
}

/// One page of generated rows — a pure range of the generator, no state, no I/O.
async fn generate(_d: Arc<Dummy>, p: Params) -> Result<RecordPage> {
    let generator = Generator::new(&types(&p)?)?
        .seed(p.query("seed").and_then(|v| v.parse().ok()).unwrap_or(0));

    let cursor: DummyCursor = p.cursor()?;
    let limit = p.limit(DEFAULT_LIMIT, MAX_LIMIT);
    let total = p
        .query("rows")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ROWS)
        .min(MAX_ROWS);

    let start = cursor.offset.min(total);
    let end = start.saturating_add(limit).min(total);
    let data = generator.rows(start as usize..end as usize)?;

    let next = if end < total {
        Cursor::new(&DummyCursor { offset: end })?
    } else {
        Cursor::empty()
    };
    Ok(RecordPage { data, cursor: next })
}
