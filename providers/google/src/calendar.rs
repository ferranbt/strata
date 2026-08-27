//! Google Calendar endpoints.

use std::sync::Arc;

use anyhow::Result;
use strata_schema::HasSchema;
use serde::{Deserialize, Serialize};

use super::{Google, Paging};
use strata_sdk::page::{ListStrategy, Page};
use strata_sdk::router::{Params, Route, Router};

const BASE: &str = "https://www.googleapis.com/calendar/v3";
const PAGING: Paging = Paging::new("maxResults", 30, 250);

pub(crate) fn register(r: &mut Router<Google>) {
    r.add(
        Route::new()
            .path("/calendar")
            .list(list_calendars)
            .strategy(ListStrategy::Offset),
    );
    r.add(Route::new().path("/calendar/:calendar").get(get_calendar));
    r.add(
        Route::new()
            .path("/calendar/:calendar/events")
            .list(list_events)
            .strategy(ListStrategy::Offset),
    );
    r.add(
        Route::new()
            .path("/calendar/:calendar/events")
            .create(create_event),
    );
}

async fn list_calendars(g: Arc<Google>, p: Params) -> Result<Page<CalendarEntry>> {
    g.page(
        &format!("{BASE}/users/me/calendarList"),
        "items",
        &PAGING,
        &p,
        &[],
    )
    .await
}

async fn get_calendar(g: Arc<Google>, p: Params) -> Result<Calendar> {
    // `:calendar` may be `primary` or an email address — URL-encode it.
    let id = urlencoding::encode(p.get("calendar")?);
    g.api_get(&format!("{BASE}/calendars/{id}"), &[]).await
}

async fn list_events(g: Arc<Google>, p: Params) -> Result<Page<Event>> {
    let id = urlencoding::encode(p.get("calendar")?);
    g.page(
        &format!("{BASE}/calendars/{id}/events"),
        "items",
        &PAGING,
        &p,
        &[("singleEvents", "true"), ("orderBy", "startTime")],
    )
    .await
}

async fn create_event(g: Arc<Google>, p: Params, input: EventInput) -> Result<Event> {
    let id = urlencoding::encode(p.get("calendar")?);
    let body = serde_json::to_value(&input)?;
    g.api_post(&format!("{BASE}/calendars/{id}/events"), &body)
        .await
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct EventInput {
    start: EventTime,
    end: EventTime,
    summary: Option<String>,
    description: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct CalendarEntry {
    id: String,
    summary: Option<String>,
    #[serde(default)]
    primary: bool,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct Calendar {
    id: String,
    summary: Option<String>,
    time_zone: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct Event {
    id: Option<String>,
    summary: Option<String>,
    start: Option<EventTime>,
    end: Option<EventTime>,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
#[serde(rename_all = "camelCase")]
struct EventTime {
    date_time: Option<String>,
    date: Option<String>,
}
