//! Gmail endpoints (read-only).

use std::sync::Arc;

use anyhow::Result;
use schema::HasSchema;
use serde::{Deserialize, Serialize};

use strata_sdk::page::{ListStrategy, Page};
use strata_sdk::router::{Params, Route, Router};

use super::{Google, Paging};

const BASE: &str = "https://gmail.googleapis.com/gmail/v1/users/me";
const PAGING: Paging = Paging::new("maxResults", 20, 500);

pub(crate) fn register(r: &mut Router<Google>) {
    r.add(Route::new().path("/gmail/profile").get(get_profile));
    r.add(
        Route::new()
            .path("/gmail/messages")
            .list(list_messages)
            .strategy(ListStrategy::Offset),
    );
}

async fn get_profile(g: Arc<Google>, _p: Params) -> Result<Profile> {
    g.api_get(&format!("{BASE}/profile"), &[]).await
}

async fn list_messages(g: Arc<Google>, p: Params) -> Result<Page<Message>> {
    g.page(&format!("{BASE}/messages"), "messages", &PAGING, &p, &[])
        .await
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct Message {
    id: String,
    thread_id: String,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct Profile {
    email_address: String,
    messages_total: u64,
    threads_total: u64,
}
