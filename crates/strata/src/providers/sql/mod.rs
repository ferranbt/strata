//! Shared framework for SQL-shaped providers (postgres, mysql, sqlite,
//! clickhouse).
//!
//! These providers expose an identical read surface — `list /tables`,
//! `list_records /tables/:table/data` (+ a dynamic `.data_type`) — over different
//! SQL dialects and drivers. [`SqlSource`] is the coarse dialect seam: name the
//! tables, resolve a table's schema, and fetch a page of a table's rows. The
//! generic [`list_tables`], [`table_data`], and [`table_data_schema`] handlers
//! own the shared orchestration (cursor decode/encode, Arrow encode).
//!
//! The write (`put`) path deliberately stays per-provider: DDL, parameter
//! binding, and merge/upsert semantics diverge too much — and are
//! performance-sensitive per engine — to share here.

use std::future::Future;
use std::sync::Arc;

use futures::StreamExt;

use anyhow::{Result, anyhow, bail};
use schema::{DataType, HasSchema, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::dataset::{Dataset, Disposition};
use crate::page::{Cursor, ListStrategy, Page};
use crate::provider::Provider;
use crate::record::{Batch, BatchPage, DataStream, stringify_text_columns};
use crate::router::{Pages, Params, Route, Router};

mod filter;
use filter::parse_filter;
pub use filter::{Filter, Op, where_clause};

/// A row of the `/tables` listing: a table's name.
#[derive(Debug, Serialize, Deserialize, HasSchema)]
pub struct TableName {
    pub name: String,
}

/// An error common to every SQL provider while resolving a table's schema.
#[derive(Debug)]
pub enum SqlError {
    /// A database column type a provider's `<dialect>_to_data_type` mapping doesn't
    /// recognize — raised so an unknown type fails loudly instead of silently
    /// becoming text. Carries the offending type string.
    UnsupportedColumnType(String),
    /// The requested table doesn't exist. Carries the table name.
    TableNotFound(String),
}

impl std::fmt::Display for SqlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlError::UnsupportedColumnType(ty) => write!(f, "unsupported SQL column type: `{ty}`"),
            SqlError::TableNotFound(table) => write!(f, "table `{table}` not found"),
        }
    }
}

impl std::error::Error for SqlError {}

/// Whether `error` is a [`SqlError::TableNotFound`] — so an absent table can be
/// told apart from a real failure.
pub fn is_table_not_found(error: &anyhow::Error) -> bool {
    matches!(
        error.downcast_ref::<SqlError>(),
        Some(SqlError::TableNotFound(_))
    )
}

/// Outcome of a `put` into a table — the response of every SQL provider's write.
#[derive(Debug, Serialize, Deserialize, HasSchema)]
pub struct WriteResult {
    pub table: String,
    /// Whether the table was created by this call (vs. already existed).
    pub created: bool,
    pub rows_written: u64,
}

/// Quote a SQL identifier with `quote` (e.g. `"` or `` ` ``), doubling any
/// embedded quote char so a name that contains it can't break out of the quoting.
/// The identifier escaping every SQL provider needs; the dialect only picks the
/// quote character.
pub fn quote_ident(ident: &str, quote: char) -> String {
    let escaped = ident.replace(quote, &format!("{quote}{quote}"));
    format!("{quote}{escaped}{quote}")
}

/// Quote each identifier with `quote` and comma-join them — for `(col1, col2, …)`
/// clauses.
pub fn quote_idents(idents: &[&str], quote: char) -> String {
    idents
        .iter()
        .map(|i| quote_ident(i, quote))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Quote a string *literal* (single quotes), doubling any embedded `'`. The quote
/// char is standard SQL, so unlike identifiers this needs no dialect parameter.
pub fn quote_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Default page size, and the ceiling a caller's `?limit=` is clamped to.
const DEFAULT_LIMIT: u32 = 10;
const MAX_LIMIT: u32 = 1000;

/// Reserved read param carrying a JSON-encoded [`Filter`], decoded like the cursor.
const FILTER_PARAM: &str = "filter";

/// Reserved read param selecting a subset of columns: a comma-separated list of
/// field names (`?fields=id,name`). Absent means the whole row.
const FIELDS_PARAM: &str = "fields";

fn get_projection(p: &Params) -> Option<Vec<String>> {
    let cols: Vec<String> = p
        .query(FIELDS_PARAM)?
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();
    (!cols.is_empty()).then_some(cols)
}

/// Narrow `schema` to the projected columns, preserving the schema's field order
/// and erroring on an unknown column. Returns the full schema when nothing is
/// projected.
fn project_schema(schema: &Schema, projection: Option<&[String]>) -> Result<Schema> {
    let Some(cols) = projection else {
        return Ok(schema.clone());
    };
    for col in cols {
        if !schema.fields.iter().any(|f| &f.name == col) {
            bail!("unknown projected column `{col}`");
        }
    }
    let fields = schema
        .fields
        .iter()
        .filter(|f| cols.contains(&f.name))
        .cloned()
        .collect();
    Ok(Schema::new(fields))
}

/// Drop every key not in the projected `schema` from each row, so the JSON rows
/// match the narrowed schema before Arrow encoding. Runs after the DB fetch, so a
/// filter or cursor column that was projected away is still available upstream.
fn project_rows(schema: &Schema, rows: &mut [Value]) {
    let keep: std::collections::HashSet<&str> =
        schema.fields.iter().map(|f| f.name.as_str()).collect();
    for row in rows.iter_mut() {
        if let Value::Object(map) = row {
            map.retain(|k, _| keep.contains(k.as_str()));
        }
    }
}

/// The page cursor for `/tables/:table/data`.
/// When `cursor` (a column name) is set the provider orders by it
/// (`ORDER BY cursor ASC`) so paging is deterministic.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SqlCursor {
    #[serde(default)]
    pub offset: u32,
    /// Page size — hydrated per request, not part of the token.
    #[serde(skip)]
    pub limit: u32,
    /// The cursor column to order by — hydrated per request, not part of the token.
    #[serde(skip)]
    pub cursor: Option<String>,
}

/// The dialect seam every SQL provider implements — coarse-grained: each method
/// runs the dialect SQL and returns cooked results. The futures are `Send` so
/// they compose into the router's boxed handler futures.
pub trait SqlSource: Send + Sync + 'static {
    /// The names of the catalog's tables. Unpaginated — a catalog is small enough
    /// to return in one page.
    fn table_names(&self) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// The resolved schema (columns) of `<table>`, from the catalog. Errors if the
    /// table doesn't exist.
    fn table_schema(&self, table: &str) -> impl Future<Output = Result<Schema>> + Send;

    /// One page of `<table>`'s rows as JSON objects (one object per row), per the
    /// [`SqlCursor`]: ordered by `cursor.cursor` when set (else engine order), and
    /// windowed by `cursor.limit`/`cursor.offset`. When `filter` is set, restrict
    /// the rows to it, compiling the predicate with [`where_clause`] and the
    /// dialect's identifier quote.
    fn table_rows(
        &self,
        table: &str,
        cursor: &SqlCursor,
        filter: Option<&Filter>,
    ) -> impl Future<Output = Result<Vec<Value>>> + Send;

    /// Reconcile `<table>` with `schema`, creating it when absent (with the schema's
    /// key fields as PRIMARY KEY, so a later merge has a conflict target). Returns
    /// whether this call created it. Altering an existing table to match comes
    /// later; for now an existing table is left as-is.
    fn upsert_table(
        &self,
        table: &str,
        schema: &Schema,
    ) -> impl Future<Output = Result<bool>> + Send;

    /// Sink `data` into `table`, appending or (for `Disposition::Merge`) upserting on
    /// the schema's key fields. The table is guaranteed to exist — callers go through
    /// [`upsert_table`](Self::upsert_table) first. Returns the rows written.
    fn write_table(
        &self,
        table: &str,
        data: Dataset,
        disposition: Disposition,
    ) -> impl Future<Output = Result<WriteResult>> + Send;

    /// Consume a data-plane [`DataStream`] into `table`. Default: fold over the
    /// chunks, calling [`write_table`](Self::write_table) on each. Override to bind
    /// batches directly / stream server-side.
    fn write_stream(
        &self,
        table: &str,
        stream: DataStream,
        disposition: Disposition,
    ) -> impl Future<Output = Result<WriteResult>> + Send {
        async move {
            let DataStream { schema, mut chunks } = stream;
            let existing = self.table_schema(table).await.ok();
            let created = match &existing {
                Some(current) if current == &schema => false,
                _ => self.upsert_table(table, &schema).await?,
            };
            let mut rows_written = 0;
            while let Some(chunk) = chunks.next().await {
                // TODO: We still create a dataset object, later on we should send
                // the stream directly to the SQL provider but we would need a way for the provider
                // to notify which batch has been written.
                let dataset = Dataset::new(schema.clone(), chunk?.data);
                rows_written += self
                    .write_table(table, dataset, disposition)
                    .await?
                    .rows_written;
            }
            Ok(WriteResult {
                table: table.to_string(),
                created,
                rows_written,
            })
        }
    }

    fn get_row(
        &self,
        table: &str,
        key: &str,
    ) -> impl Future<Output = Result<Option<Value>>> + Send {
        async move {
            let schema = self.table_schema(table).await?;
            let key_field = schema
                .fields
                .iter()
                .find(|f| f.is_key())
                .ok_or_else(|| anyhow!("table `{table}` has no key column"))?;
            let value = if matches!(key_field.data_type, DataType::Int64 | DataType::UInt64) {
                key.parse::<i64>()
                    .map(Value::from)
                    .unwrap_or_else(|_| Value::String(key.to_string()))
            } else {
                Value::String(key.to_string())
            };
            let filter = Filter::Cmp {
                field: key_field.name.clone(),
                op: Op::Eq,
                value,
            };
            let cursor = SqlCursor {
                offset: 0,
                limit: 1,
                cursor: None,
            };
            Ok(self
                .table_rows(table, &cursor, Some(&filter))
                .await?
                .into_iter()
                .next())
        }
    }

    fn register_tables(r: &mut Router<Self>)
    where
        Self: Provider,
    {
        r.add(
            Route::new()
                .path("/tables")
                .list(list_tables::<Self>)
                .strategy(ListStrategy::Offset),
        );
        r.add(
            Route::new()
                .path("/tables/:table")
                .list_stream(table_stream::<Self>)
                .data_type(table_data_schema::<Self>)
                .strategy(ListStrategy::Offset)
                .queryable(),
        );
        r.add(
            Route::new()
                .path("/tables/:table/:id")
                .get_records(table_get::<Self>)
                .data_type(table_data_schema::<Self>),
        );
        r.add(Route::new().path("/tables/:table").put(write_table::<Self>));
    }
}

/// `list /tables`: the catalog's tables, in a single page.
pub async fn list_tables<S: SqlSource>(db: Arc<S>, _p: Params) -> Result<Page<TableName>> {
    let items = db
        .table_names()
        .await?
        .into_iter()
        .map(|name| TableName { name })
        .collect();
    Ok(Page::new(items, Cursor::empty()))
}

pub async fn table_get<S: SqlSource>(db: Arc<S>, p: Params) -> Result<Value> {
    let table = p.get("table")?;
    let key = p.get("id")?;
    let mut row = db
        .get_row(table, key)
        .await?
        .ok_or_else(|| anyhow!("no row with key `{key}` in table `{table}`"))?;
    if let Some(fields) = get_projection(&p)
        && let Value::Object(map) = &mut row
    {
        map.retain(|k, _| fields.contains(k));
    }
    Ok(row)
}

/// `list_stream /tables/:table`: a table's rows as typed Arrow columns, walked
/// offset by offset. The provider owns the walk, so the table schema, projection,
/// filter, and cursor column are resolved once for the whole read rather than on
/// every page; each page is still a single [`SqlSource::table_rows`] call, so the
/// dialect implementations are untouched.
pub async fn table_stream<S: SqlSource>(db: Arc<S>, p: Params) -> Result<Pages> {
    let name = p.get("table")?.to_string();
    // The full table schema drives the fetch (cursor column, filter columns); the
    // projected schema governs what's returned. The cursor is resolved off the full
    // schema so ordering works even when the cursor column is projected away.
    // TODO: Select the table fields at the provider level itself.
    let full = enrich(db.table_schema(&name).await?, p.schema());
    let projection = get_projection(&p);
    let schema = project_schema(&full, projection.as_deref())?;

    let limit = p.limit(DEFAULT_LIMIT, MAX_LIMIT);
    let cursor_column = resolve_cursor(&p, &full);
    let filter = p.query(FILTER_PARAM).map(parse_filter).transpose()?;
    let start: SqlCursor = p.cursor()?;

    let pages = futures::stream::unfold(Some(start.offset), move |offset| {
        let (db, name, schema) = (db.clone(), name.clone(), schema.clone());
        let (projection, cursor_column, filter) =
            (projection.clone(), cursor_column.clone(), filter.clone());

        async move {
            let offset = offset?;
            let cursor = SqlCursor {
                offset,
                limit,
                cursor: cursor_column,
            };
            let page = table_page(
                db.as_ref(),
                &name,
                &schema,
                &cursor,
                filter.as_ref(),
                projection.is_some(),
            )
            .await;
            match page {
                Ok((page, more)) => Some((Ok(page), more.then(|| offset + limit))),
                Err(e) => Some((Err(e), None)),
            }
        }
    });
    Ok(pages.boxed())
}

/// One page of `table`, plus whether a further page may follow.
async fn table_page<S: SqlSource>(
    db: &S,
    table: &str,
    schema: &Schema,
    cursor: &SqlCursor,
    filter: Option<&Filter>,
    projected: bool,
) -> Result<(BatchPage, bool)> {
    let mut rows = db.table_rows(table, cursor, filter).await?;
    let next = next_cursor(cursor, rows.len())?;
    if projected {
        project_rows(schema, &mut rows);
    }
    stringify_text_columns(schema, &mut rows);
    normalize_temporals(schema, &mut rows);
    let more = next.next.is_some();
    let page = BatchPage {
        data: Batch::encode(schema, &rows)?,
        cursor: Some(next),
    };
    Ok((page, more))
}

/// Rewrite `Timestamp`/`Date` columns to their canonical string form so
/// [`Records::encode`] can parse them into native Arrow temporal arrays. Engines
/// render these differently — postgres `2024-01-01T12:00:00`, mysql and clickhouse
/// `2024-01-01 12:00:00` — and only the canonical form parses.
fn normalize_temporals(schema: &Schema, rows: &mut [Value]) {
    for row in rows.iter_mut() {
        let Value::Object(map) = row else { continue };
        for field in schema.fields.iter() {
            let Some(slot) = map.get_mut(&field.name) else {
                continue;
            };
            let Some(text) = slot.as_str() else { continue };
            match field.data_type {
                schema::DataType::Timestamp => {
                    if let Some(value) = schema::Timestamp::parse(text) {
                        *slot = Value::String(value.to_string());
                    }
                }
                schema::DataType::Date => {
                    if let Some(value) = schema::Date::parse(text) {
                        *slot = Value::String(value.to_string());
                    }
                }
                _ => {}
            }
        }
    }
}

/// Resolve the cursor column for a read, by precedence: (1) a `cursor` annotation
/// on the endpoint's persisted schema, (2) an explicit `?cursor_field=`, (3) an
/// auto-detected `created_at`/`updated_at` column. `None` → blind offset paging.
fn resolve_cursor(p: &Params, schema: &Schema) -> Option<String> {
    // (1) A cursor annotation on the persisted schema the router resolved for this
    // endpoint (e.g. propagated from a source during a pipe).
    if let Some(col) = cursor_annotation(schema) {
        return Some(col);
    }
    // (2) Explicitly supplied by the caller.
    // TODO: Eventually we want to have the Cursor be modified as well by user
    // request.
    if let Some(col) = p.query("cursor_field") {
        return Some(col.to_string());
    }
    // (3) Auto-detected from the table's own columns.
    autodetect_cursor(schema)
}

/// The name of the field carrying a `cursor` annotation, if any (unwrapping a
/// `List` row `Struct`).
fn cursor_annotation(schema: &Schema) -> Option<String> {
    schema
        .fields
        .iter()
        .find(|f| f.is_cursor())
        .map(|f| f.name.clone())
}

/// A conventional cursor column present on the table: `created_at` (append-only,
/// so safe with offset) preferred, else `updated_at`. Case-insensitive.
fn autodetect_cursor(schema: &Schema) -> Option<String> {
    let by_name = |want: &str| {
        schema
            .fields
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(want))
            .map(|c| c.name.clone())
    };
    by_name("created_at").or_else(|| by_name("updated_at"))
}

/// The `.data_type()` resolver for `/tables/:table/data`.
pub async fn table_data_schema<S: SqlSource>(db: Arc<S>, p: Params) -> Result<Schema> {
    let full = enrich(db.table_schema(p.get("table")?).await?, p.schema());
    project_schema(&full, get_projection(&p).as_deref())
}

fn enrich(mut schema: Schema, annotated: Option<&Schema>) -> Schema {
    let Some(annotated) = annotated else {
        return schema;
    };
    for field in &mut schema.fields {
        if let Some(source) = annotated.fields.iter().find(|f| f.name == field.name) {
            for (key, value) in source.annotations.to_map() {
                field.annotate(key, value);
            }
        }
    }
    schema
}

/// `put /tables/:table`: sink a [`DataStream`]. Decodes the `disposition` param and
/// validates the shared write contract off the stream's schema (row-shaped,
/// non-empty; a key when merging), then hands the stream to the provider.
pub async fn write_table<S: SqlSource>(
    db: Arc<S>,
    p: Params,
    stream: DataStream,
) -> Result<WriteResult> {
    let disposition = Disposition::from_param(p.query(Disposition::PARAM))?;
    if stream.schema.fields.iter().len() == 0 {
        bail!("dataset schema has no columns");
    }
    let has_primary_key = !stream.schema.get_key_fields().is_empty();
    // Merge needs a key to upsert on; the provider reads the key fields itself.
    if disposition == Disposition::Merge && !has_primary_key {
        bail!("merge disposition requires a key, but the dataset schema declares none");
    }
    let table = p.get("table")?;
    db.write_stream(table, stream, disposition).await
}

/// The cursor for the next page: advance the offset by the page size, but only
/// while the page came back *full*. A short page is the tail, so it yields an
/// empty cursor — which is what a follower (`--follow`, or the streaming `unfold`)
/// stops on. Without this an offset cursor never terminates.
fn next_cursor(cursor: &SqlCursor, returned: usize) -> Result<Cursor> {
    if cursor.limit > 0 && returned as u32 == cursor.limit {
        Cursor::new(&SqlCursor {
            offset: cursor.offset + returned as u32,
            ..Default::default()
        })
    } else {
        Ok(Cursor::empty())
    }
}

/// Shared write-semantics tests for every SQL backend. Each provider's own test
/// module mounts its backend and runs these against it, so append-dedup and
/// merge-overwrite are verified once but proven on postgres, mysql, sqlite, and
/// clickhouse alike.
#[cfg(test)]
pub mod suite {
    use super::{TableName, WriteResult};
    use crate::dataset::Dataset;
    use crate::provider::Provider;
    use crate::testkit::Client;
    use anyhow::Result;
    use schema::{DataType, HasSchema};

    #[derive(serde::Serialize, serde::Deserialize, schema::HasSchema)]
    pub struct Row {
        #[schema(key)]
        id: i64,
        name: String,
    }

    #[derive(serde::Serialize, serde::Deserialize, schema::HasSchema)]
    pub struct Event {
        #[schema(key)]
        id: i64,
        #[schema(cursor)]
        created_at: schema::Timestamp,
        name: String,
    }

    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    pub struct RowProjected {
        name: String,
    }

    /// A table created through `put` shows up in `/tables`, and its schema resolves
    /// back from the catalog with the columns it was created with.
    pub async fn lists_tables_and_schema<S: Provider>(client: &Client<S>) -> Result<()> {
        let rows = [Row {
            id: 1,
            name: "a".into(),
        }];
        let _: WriteResult = client.put("/tables/catalog", Dataset::of(&rows)?).await?;

        let mut tables = client.list("/tables").await?;
        let names: Vec<TableName> = tables.next().await?;
        assert!(
            names.iter().any(|t| t.name == "catalog"),
            "created table must appear in /tables, got {:?}",
            names.iter().map(|t| &t.name).collect::<Vec<_>>()
        );

        let stream = client.list::<Row>("/tables/catalog").await?;
        let schema = stream.schema();
        let columns: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(columns, ["id", "name"]);
        assert_eq!(schema.fields[0].data_type, DataType::Int64);
        assert!(schema.fields[0].is_key(), "`id` must round-trip as the key");
        assert!(!schema.fields[1].is_key(), "`name` is not a key");
        Ok(())
    }

    /// Rows written through `put` read back in cursor order, across page boundaries:
    /// 20 rows drain as two 10-row chunks whose ids concatenate to `0..20`.
    pub async fn write_then_read_paginates_by_cursor<S: Provider>(
        client: &Client<S>,
    ) -> Result<()> {
        let generator = crate::datagen::Generator::new(&Event::schema())?;
        let result: WriteResult = client.put("/tables/events", generator.dataset(20)?).await?;
        assert!(result.created);
        assert_eq!(result.rows_written, 20);

        let mut stream = client.list("/tables/events").await?;
        let first: Vec<Event> = stream.next().await?;
        let second: Vec<Event> = stream.next().await?;
        let ids: Vec<i64> = first.iter().chain(second.iter()).map(|e| e.id).collect();
        assert_eq!(ids, (0..20).collect::<Vec<i64>>());
        Ok(())
    }

    /// A whole table drains through the read stream, across many pages.
    pub async fn streams_whole_table<S: Provider>(client: &Client<S>) -> Result<()> {
        const ROWS: usize = 2000;

        let generator = crate::datagen::Generator::new(&Event::schema())?;
        let result: WriteResult = client
            .put("/tables/streamed", generator.dataset(ROWS)?)
            .await?;
        assert_eq!(result.rows_written, ROWS as u64);

        let mut stream = client.list::<Event>("/tables/streamed").await?;
        let mut ids = Vec::new();
        let mut pages = 0;
        while let Some(page) = stream.try_next().await? {
            pages += 1;
            ids.extend(page.into_iter().map(|event| event.id));
        }

        assert_eq!(
            ids.len(),
            ROWS,
            "the stream must yield every row exactly once"
        );
        assert!(
            pages > 1,
            "{ROWS} rows must span several pages, got {pages}"
        );
        assert_eq!(
            ids,
            (0..ROWS as i64).collect::<Vec<i64>>(),
            "rows must stay in cursor order across every page boundary"
        );
        Ok(())
    }

    /// Append with a key skips already-present rows rather than duplicating them:
    /// piping the same keyed rows twice leaves the row count unchanged.
    pub async fn append_skips_duplicates<S: Provider>(client: &Client<S>) -> Result<()> {
        let rows = [
            Row {
                id: 1,
                name: "a".into(),
            },
            Row {
                id: 2,
                name: "b".into(),
            },
        ];
        let _: WriteResult = client.put("/tables/dedup", Dataset::of(&rows)?).await?;
        let _: WriteResult = client.put("/tables/dedup", Dataset::of(&rows)?).await?;

        let mut stream = client.list("/tables/dedup").await?;
        let found_rows: Vec<Row> = stream.next().await?;

        assert_eq!(
            found_rows.len(),
            2,
            "append with a key must skip existing rows, not duplicate them"
        );
        Ok(())
    }

    /// Merge overwrites the non-key columns of an existing key.
    pub async fn merge_overwrites<S: Provider>(client: &Client<S>) -> Result<()> {
        let _: WriteResult = client
            .put(
                "/tables/up",
                Dataset::of(&[Row {
                    id: 1,
                    name: "a".into(),
                }])?,
            )
            .await?;
        let _: WriteResult = client
            .put(
                "/tables/up?disposition=merge",
                Dataset::of(&[Row {
                    id: 1,
                    name: "b".into(),
                }])?,
            )
            .await?;

        let mut stream = client.list("/tables/up").await?;
        let rows: Vec<Row> = stream.next().await?;

        assert_eq!(rows.len(), 1, "merge must not duplicate the key");
        assert_eq!(
            rows[0].name, "b",
            "merge must overwrite the non-key columns"
        );
        Ok(())
    }

    /// A `?filter=` predicate restricts the rows a `list` returns.
    pub async fn filters_rows<S: Provider>(client: &Client<S>) -> Result<()> {
        let rows = [
            Row {
                id: 1,
                name: "a".into(),
            },
            Row {
                id: 2,
                name: "b".into(),
            },
            Row {
                id: 3,
                name: "c".into(),
            },
            Row {
                id: 4,
                name: "d".into(),
            },
        ];
        let _: WriteResult = client.put("/tables/filtered", Dataset::of(&rows)?).await?;

        let predicate = serde_json::json!({
            "and": [
                { "cmp": { "field": "id", "op": "gte", "value": 2 } },
                { "or": [
                    { "cmp": { "field": "name", "op": "eq", "value": "b" } },
                    { "cmp": { "field": "name", "op": "eq", "value": "d" } }
                ] }
            ]
        });

        // TODO: List does not have a request helper
        let encoded = urlencoding::encode(&predicate.to_string()).into_owned();

        let mut stream = client
            .list(&format!("/tables/filtered?filter={encoded}"))
            .await?;
        let mut found: Vec<Row> = stream.next().await?;
        found.sort_by_key(|r| r.id);

        let names: Vec<&str> = found.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(
            names,
            ["b", "d"],
            "filter must return only rows matching `id >= 2 AND (name = b OR name = d)`"
        );
        Ok(())
    }

    /// A `?fields=` projection returns only the named columns
    pub async fn projects_columns<S: Provider>(client: &Client<S>) -> Result<()> {
        let rows = [
            Row {
                id: 1,
                name: "a".into(),
            },
            Row {
                id: 2,
                name: "b".into(),
            },
        ];
        let _: WriteResult = client.put("/tables/projected", Dataset::of(&rows)?).await?;

        let mut stream = client
            .list::<RowProjected>("/tables/projected?fields=name")
            .await?;

        let declared: Vec<&str> = stream
            .schema()
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect();
        assert_eq!(
            declared,
            ["name"],
            "projected read must declare only the requested column"
        );

        let projected = stream.next().await?;
        let names: Vec<&str> = projected.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["a", "b"], "projected rows must carry only `name`");

        let mut full = client.list::<Row>("/tables/projected").await?;
        assert_eq!(full.next().await?.len(), 2);
        Ok(())
    }

    pub async fn gets_single_row<S: Provider>(client: &Client<S>) -> Result<()> {
        let rows = [
            Row {
                id: 1,
                name: "a".into(),
            },
            Row {
                id: 2,
                name: "b".into(),
            },
        ];
        let _: WriteResult = client.put("/tables/getone", Dataset::of(&rows)?).await?;

        let row: Row = client.get("/tables/getone/2").await?;
        assert_eq!(row.id, 2);
        assert_eq!(row.name, "b");

        let projected: RowProjected = client.get("/tables/getone/2?fields=name").await?;
        assert_eq!(projected.name, "b");
        Ok(())
    }
}
