//! Local filesystem provider: list directories and read files.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use strata_schema::HasSchema;
use serde::{Deserialize, Serialize};

use strata_sdk::page::{Cursor, ListStrategy, Page};
use strata_sdk::router::{Params, Route, Router};

/// Stateless: each call touches the filesystem directly.
#[derive(strata_sdk::Provider)]
pub struct Fs;

impl Fs {
    fn new() -> Result<Self> {
        Ok(Fs)
    }

    fn routes(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/file/*path")
                .list(list_dir)
                .strategy(ListStrategy::Offset),
        );
        r.add(Route::new().path("/file/*path").get(read_file));
        r.add(Route::new().path("/file/*path").create(write_file));
    }
}

/// Resolve the captured catch-all into an absolute path. The catch-all drops the
/// leading slash (and is empty at the root), so put it back.
fn abs(p: &Params) -> PathBuf {
    let rest = p.get("path").unwrap_or("");
    if rest.is_empty() {
        PathBuf::from("/")
    } else {
        PathBuf::from(format!("/{rest}"))
    }
}

#[derive(Deserialize, Serialize)]
struct FsCursor {
    #[serde(default)]
    offset: u32,
}

/// Entries of a directory, sorted by name, paginated by offset.
async fn list_dir(_fs: Arc<Fs>, p: Params) -> Result<Page<DirEntry>> {
    let dir = abs(&p);
    let mut entries: Vec<DirEntry> = std::fs::read_dir(&dir)
        .with_context(|| format!("reading directory {}", dir.display()))?
        .filter_map(|entry| entry.ok())
        .map(|entry| {
            let meta = entry.metadata().ok();
            DirEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                path: entry.path().to_string_lossy().into_owned(),
                is_dir: meta.as_ref().map(|m| m.is_dir()).unwrap_or(false),
                size: meta.as_ref().filter(|m| m.is_file()).map(|m| m.len()),
            }
        })
        .collect();
    entries.sort_by(|a, b| a.name.cmp(&b.name));

    let cursor: FsCursor = p.cursor()?;
    let page: Vec<DirEntry> = entries
        .into_iter()
        .skip(cursor.offset as usize)
        .take(100)
        .collect();
    let cursor = Cursor::new(&FsCursor {
        offset: cursor.offset + page.len() as u32,
    })?;
    Ok(Page::new(page, cursor))
}

async fn read_file(_fs: Arc<Fs>, p: Params) -> Result<FileContent> {
    let path = abs(&p);
    let bytes = std::fs::read(&path).with_context(|| format!("reading file {}", path.display()))?;
    Ok(FileContent {
        path: path.to_string_lossy().into_owned(),
        size: bytes.len() as u64,
        content: String::from_utf8_lossy(&bytes).into_owned(),
    })
}

async fn write_file(_fs: Arc<Fs>, p: Params, input: FileInput) -> Result<FileContent> {
    let path = abs(&p);
    std::fs::write(&path, &input.content)
        .with_context(|| format!("writing file {}", path.display()))?;

    Ok(FileContent {
        path: path.to_string_lossy().into_owned(),
        size: input.content.len() as u64,
        content: input.content,
    })
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct FileInput {
    content: String,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct DirEntry {
    name: String,
    path: String,
    is_dir: bool,
    size: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct FileContent {
    path: String,
    size: u64,
    content: String,
}
