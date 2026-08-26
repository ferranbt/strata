//! Google drive endpoints

use std::sync::Arc;

use anyhow::Result;
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use schema::HasSchema;
use serde::{Deserialize, Serialize};

use super::{Google, Paging};
use strata_sdk::page::{ListStrategy, Page};
use strata_sdk::router::{Params, Route, Router};

const BASE: &str = "https://www.googleapis.com/drive/v3";
const PAGING: Paging = Paging::new("pageSize", 100, 1000);

pub(crate) fn register(r: &mut Router<Google>) {
    r.add(
        Route::new()
            .path("/drive")
            .list(list_root)
            .strategy(ListStrategy::Offset),
    );
    r.add(Route::new().path("/drive/:id").get(get_file));
    r.add(Route::new().path("/drive/:id/download").get(download));
}

/// List the root folder's children — a convenient entry point for finding IDs.
async fn list_root(g: Arc<Google>, p: Params) -> Result<Page<DriveFile>> {
    g.page(
        &format!("{BASE}/files"),
        "files",
        &PAGING,
        &p,
        &[
            ("q", "'root' in parents and trashed = false"),
            ("fields", "nextPageToken,files(id,name,mimeType)"),
        ],
    )
    .await
}

async fn get_file(g: Arc<Google>, p: Params) -> Result<DriveFile> {
    fetch_meta(&g, p.get("id")?).await
}

/// Download the file's bytes as-is, base64-encoded.
async fn download(g: Arc<Google>, p: Params) -> Result<DownloadedFile> {
    let id = p.get("id")?;
    let meta = fetch_meta(&g, id).await?;
    let bytes = g
        .authed_get(&format!("{BASE}/files/{id}"), &[("alt", "media")])
        .await?
        .bytes()
        .await?;

    Ok(DownloadedFile {
        id: meta.id,
        name: meta.name,
        mime_type: meta.mime_type,
        content: STANDARD.encode(&bytes),
    })
}

async fn fetch_meta(g: &Google, id: &str) -> Result<DriveFile> {
    g.api_get(
        &format!("{BASE}/files/{id}"),
        &[("fields", "id,name,mimeType,size")],
    )
    .await
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct DriveFile {
    id: String,
    name: String,
    mime_type: Option<String>,
    size: Option<String>,
}

#[derive(Debug, Serialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct DownloadedFile {
    id: String,
    name: String,
    mime_type: Option<String>,
    content: String,
}
