use std::sync::Arc;

use anyhow::{Result, anyhow};
use futures::StreamExt;
use schema::Schema;
use serde::{Deserialize, Serialize};

use crate::datagen::Generator;
use crate::page::{Cursor, ListStrategy};
use crate::provider::Provider;
use crate::record::BatchPage;
use crate::router::{Pages, Params, Route, Router};

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
                .list_stream(generate)
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

/// Generated rows as a stream
async fn generate(_d: Arc<Dummy>, p: Params) -> Result<Pages> {
    let generator = Arc::new(
        Generator::new(&types(&p)?)?
            .seed(p.query("seed").and_then(|v| v.parse().ok()).unwrap_or(0)),
    );

    let limit = p.limit(DEFAULT_LIMIT, MAX_LIMIT);
    let total = p
        .query("rows")
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ROWS)
        .min(MAX_ROWS);
    let start: DummyCursor = p.cursor()?;

    let pages = futures::stream::unfold(Some(start.offset.min(total)), move |offset| {
        let generator = generator.clone();
        async move {
            let start = offset?;
            let end = start.saturating_add(limit).min(total);
            let page = generate_page(&generator, start, end, total);
            match page {
                Ok(page) => Some((Ok(page), (end < total).then_some(end))),
                Err(e) => Some((Err(e), None)),
            }
        }
    });
    Ok(pages.boxed())
}

fn generate_page(generator: &Generator, start: u32, end: u32, total: u32) -> Result<BatchPage> {
    let data = generator.rows(start as usize..end as usize)?;
    let cursor = if end < total {
        Cursor::new(&DummyCursor { offset: end })?
    } else {
        Cursor::empty()
    };
    Ok(BatchPage {
        data,
        cursor: Some(cursor),
    })
}
