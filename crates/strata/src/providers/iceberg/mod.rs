use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use schema::{HasSchema, Schema};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use arrow_array::RecordBatch;
use config_macro::config;
use futures::{StreamExt, TryStreamExt};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use parquet::file::properties::WriterProperties;

mod convert;

use convert::{align_to, iceberg_to_strata_schema, strata_to_iceberg_schema};

use crate::dataset::Disposition;
use crate::page::{Cursor, ListStrategy, Page};
use crate::provider::Provider;
use crate::record::{Batch, BatchPage, DataStream};
use crate::router::{Pages, Params, Route, Router};

/// All strata tables live in one namespace.
const NAMESPACE: &str = "strata";

/// Iceberg has no server-side limit, so the max is just a sanity bound.
const DEFAULT_LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 1000;

/// This provider's cursor state: the scan offset (iceberg pages by slicing the
/// materialized scan).
#[derive(Debug, Default, Serialize, Deserialize)]
struct IcebergCursor {
    #[serde(default)]
    offset: u32,
}

#[config]
struct Config {
    #[config(default_env = "STRATA_WAREHOUSE")]
    warehouse: String,
}

/// Provider state: warehouse/catalog locations plus the lazily-built catalog.
pub struct Iceberg {
    /// `file://` URI of the warehouse (FileIO root).
    warehouse: String,
    /// `sqlite://` URI of the catalog DB.
    db_uri: String,
    /// Local warehouse directory (created on first use).
    dir: String,
    catalog: Mutex<Option<Arc<dyn Catalog>>>,
}

impl Provider for Iceberg {
    fn name() -> &'static str {
        "iceberg"
    }

    fn new(config: &crate::config::ProviderConfig) -> Result<Self> {
        // Warehouse from config (`warehouse`) or `STRATA_WAREHOUSE`, default
        // `./warehouse`. Each mounted instance can point at its own warehouse.
        let config: Config = config.decode()?;
        // Neither the `warehouse` config key nor `STRATA_WAREHOUSE` set → `./warehouse`
        // (not the current directory, which silently scattered tables into the repo).
        let dir = if config.warehouse.is_empty() {
            "./warehouse".to_string()
        } else {
            config.warehouse
        };
        let abs = if std::path::Path::new(&dir).is_absolute() {
            std::path::PathBuf::from(&dir)
        } else {
            std::env::current_dir()?.join(&dir)
        };
        let abs = abs.to_string_lossy().into_owned();
        Ok(Iceberg {
            warehouse: format!("file://{abs}"),
            db_uri: format!("sqlite://{abs}/catalog.db?mode=rwc"),
            dir: abs,
            catalog: Mutex::new(None),
        })
    }

    fn register(r: &mut Router<Self>) {
        r.add(
            Route::new()
                .path("/tables")
                .list(list_tables)
                .strategy(ListStrategy::Offset),
        );
        // Table *metadata* is its own entity, on its own path — `/tables/:table` is
        // the table's rows (list) and its sink (put), mirroring the SQL providers.
        r.add(Route::new().path("/tables/:table/info").get(get_table));
        r.add(Route::new().path("/tables/:table").put(put_table));
        r.add(
            Route::new()
                .path("/tables/:table")
                .list_stream(scan)
                .data_type(scan_schema)
                .strategy(ListStrategy::Offset),
        );
    }
}

impl Iceberg {
    /// Build (once) and return the SQLite-backed catalog.
    async fn catalog(&self) -> Result<Arc<dyn Catalog>> {
        let mut guard = self.catalog.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        std::fs::create_dir_all(&self.dir)?;
        // The SQL catalog uses sqlx's `Any` driver; register the SQLite driver.
        sqlx::any::install_default_drivers();
        let catalog = SqlCatalogBuilder::default()
            .with_storage_factory(Arc::new(iceberg::io::LocalFsStorageFactory))
            .load(
                "strata",
                HashMap::from([
                    (SQL_CATALOG_PROP_URI.to_string(), self.db_uri.clone()),
                    (
                        SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                        self.warehouse.clone(),
                    ),
                    (
                        SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                        SqlBindStyle::QMark.to_string(),
                    ),
                ]),
            )
            .await?;
        let catalog: Arc<dyn Catalog> = Arc::new(catalog);
        *guard = Some(catalog.clone());
        Ok(catalog)
    }
}

fn namespace() -> NamespaceIdent {
    NamespaceIdent::new(NAMESPACE.to_string())
}

fn table_ident(table: &str) -> TableIdent {
    TableIdent::new(namespace(), table.to_string())
}

/// The catalog: tables in the `strata` namespace.
async fn list_tables(ice: Arc<Iceberg>, p: Params) -> Result<Page<TableEntry>> {
    let catalog = ice.catalog().await?;
    let ns = namespace();
    let mut names: Vec<String> = if catalog.namespace_exists(&ns).await? {
        catalog
            .list_tables(&ns)
            .await?
            .iter()
            .map(|t| t.name().to_string())
            .collect()
    } else {
        Vec::new()
    };
    names.sort();

    let cursor: IcebergCursor = p.cursor()?;
    let limit = p.limit(DEFAULT_LIMIT, MAX_LIMIT);
    let offset = cursor.offset as usize;
    let total = names.len();
    let page: Vec<TableEntry> = names
        .into_iter()
        .skip(offset)
        .take(limit as usize)
        .map(|name| TableEntry { name })
        .collect();
    let next = if offset + page.len() < total {
        Cursor::new(&IcebergCursor {
            offset: cursor.offset + limit,
        })?
    } else {
        Cursor::empty()
    };
    Ok(Page::new(page, next))
}

/// Table metadata — schema, snapshots, location.
async fn get_table(ice: Arc<Iceberg>, p: Params) -> Result<TableInfo> {
    let name = p.get("table")?;
    let catalog = ice.catalog().await?;
    let table = catalog.load_table(&table_ident(name)).await?;
    let meta = table.metadata();
    Ok(TableInfo {
        name: name.to_string(),
        location: meta.location().to_string(),
        snapshots: meta.snapshots().count() as u64,
        current_snapshot: meta.current_snapshot_id(),
        schema: iceberg_to_strata_schema(meta.current_schema()).to_json_schema(),
    })
}

/// Scan the table's current rows, Arrow-native: iceberg's own batch stream is the
/// read, so the scan is opened once and each batch becomes a page. A resume cursor
/// skips the batches ahead of it rather than re-scanning from the start per page.
async fn scan(ice: Arc<Iceberg>, p: Params) -> Result<Pages> {
    let name = p.get("table")?.to_string();
    let catalog = ice.catalog().await?;
    let table = catalog.load_table(&table_ident(&name)).await?;

    // Align to strata's Arrow mapping (notably `tz=UTC`, which serde_arrow requires
    // — iceberg emits `+00:00`). Metadata-only, still no JSON.
    let schema = iceberg_to_strata_schema(table.metadata().current_schema());
    let arrow_schema = Arc::new(crate::record::strata_schema_to_arrow_schema(&schema));

    let start: IcebergCursor = p.cursor()?;
    let mut batches = table.scan().select_all().build()?.to_arrow().await?;

    // Drop whole batches that end before the cursor, then slice the one it lands in.
    let mut scanned = 0u32;
    let mut pending = None;
    while let Some(batch) = batches.try_next().await? {
        let rows = batch.num_rows() as u32;
        if scanned + rows <= start.offset {
            scanned += rows;
            continue;
        }
        let skip = (start.offset - scanned) as usize;
        pending = Some(batch.slice(skip, batch.num_rows() - skip));
        break;
    }

    // One batch ahead, so the final page can be told apart and close the cursor.
    let pages = futures::stream::unfold(
        (batches, pending, start.offset),
        move |(mut batches, pending, offset)| {
            let arrow_schema = arrow_schema.clone();
            async move {
                let batch = pending?;
                let offset = offset + batch.num_rows() as u32;
                let following = match batches.try_next().await {
                    Ok(following) => following,
                    Err(e) => return Some((Err(e.into()), (batches, None, offset))),
                };
                match scan_page(&arrow_schema, &batch, offset, following.is_some()) {
                    Ok(page) => Some((Ok(page), (batches, following, offset))),
                    Err(e) => Some((Err(e), (batches, None, offset))),
                }
            }
        },
    );
    Ok(pages.boxed())
}

fn scan_page(
    arrow_schema: &Arc<arrow_schema::Schema>,
    batch: &RecordBatch,
    next_offset: u32,
    more: bool,
) -> Result<BatchPage> {
    let cursor = if more {
        Cursor::new(&IcebergCursor {
            offset: next_offset,
        })?
    } else {
        Cursor::empty()
    };
    Ok(BatchPage {
        data: Batch::from_record_batch(&align_to(arrow_schema, batch)?),
        cursor: Some(cursor),
    })
}

/// Dynamic schema for `/tables/:table/data`: the table's Iceberg schema.
async fn scan_schema(ice: Arc<Iceberg>, p: Params) -> Result<Schema> {
    let name = p.get("table")?;
    let catalog = ice.catalog().await?;
    let table = catalog.load_table(&table_ident(name)).await?;
    Ok(iceberg_to_strata_schema(table.metadata().current_schema()))
}

/// Create the table from the dataset's schema if absent, then append the rows as
/// one new snapshot (Parquet data file + fast-append commit).
async fn put_table(ice: Arc<Iceberg>, p: Params, stream: DataStream) -> Result<WriteResult> {
    let name = p.get("table")?;
    // Shares the `put` write surface; only append is implemented so far (merge
    // would need equality-delete commits on the row key).
    if Disposition::from_param(p.query(Disposition::PARAM))? == Disposition::Merge {
        bail!("iceberg sink supports only `append` (merge is not yet implemented)");
    }
    // The stream's schema is the sink contract; its batches are written as they
    // arrive and committed together as one snapshot.
    let DataStream { schema, mut chunks } = stream;
    if schema.fields.is_empty() {
        bail!("dataset schema has no columns");
    }

    let catalog = ice.catalog().await?;
    let ns = namespace();
    if !catalog.namespace_exists(&ns).await? {
        catalog.create_namespace(&ns, HashMap::new()).await?;
    }

    let ident = table_ident(name);
    let created = !catalog.table_exists(&ident).await?;
    let table = if created {
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(strata_to_iceberg_schema(&schema)?)
            .build();
        catalog.create_table(&ns, creation).await?
    } else {
        catalog.load_table(&ident).await?
    };

    // The table's Arrow schema (carries the field ids the writer needs); each
    // incoming batch is aligned to it before writing.
    let arrow_schema = Arc::new(iceberg::arrow::schema_to_arrow_schema(
        table.metadata().current_schema(),
    )?);

    let location_gen = DefaultLocationGenerator::new(table.metadata())?;
    // Unique filename prefix per write, else successive appends collide on the
    // same `strata-00000.parquet` path.
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let file_name_gen = DefaultFileNameGenerator::new(
        format!("strata-{unique}"),
        None,
        iceberg::spec::DataFileFormat::Parquet,
    );
    let parquet = ParquetWriterBuilder::new(
        WriterProperties::builder().build(),
        table.metadata().current_schema().clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet,
        table.file_io().clone(),
        location_gen,
        file_name_gen,
    );
    let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
    // Write the stream's Arrow batches straight through — no JSON round-trip.
    let mut rows_written = 0u64;
    while let Some(chunk) = chunks.try_next().await? {
        if chunk.data.row_count() == 0 {
            continue;
        }
        let aligned = align_to(&arrow_schema, &chunk.data.to_record_batch(&schema)?)?;
        rows_written += aligned.num_rows() as u64;
        writer.write(aligned).await?;
    }
    let data_files = writer.close().await?;

    let tx = Transaction::new(&table);
    let action = tx.fast_append().add_data_files(data_files);
    let tx = action.apply(tx)?;
    tx.commit(catalog.as_ref()).await?;

    Ok(WriteResult {
        table: name.to_string(),
        created,
        rows_written,
    })
}

// === Response shapes ===

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct TableEntry {
    name: String,
}

#[derive(Debug, Serialize, HasSchema)]
struct TableInfo {
    name: String,
    location: String,
    snapshots: u64,
    current_snapshot: Option<i64>,
    schema: Value,
}

#[derive(Debug, Serialize, Deserialize, HasSchema)]
struct WriteResult {
    table: String,
    created: bool,
    rows_written: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::Client;
    use schema::{Date, Timestamp};

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, HasSchema)]
    struct Meta {
        key: String,
        weight: i64,
    }

    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize, HasSchema)]
    struct Event {
        id: i64,
        name: String,
        day: Date,
        at: Timestamp,
        tags: Vec<String>,
        meta: Meta,
    }

    fn client(dir: &str) -> Result<Client<Iceberg>> {
        let config: crate::config::ProviderConfig = serde_json::from_value(serde_json::json!({
            "backend": "iceberg",
            "warehouse": dir,
        }))?;
        Client::<Iceberg>::mount(&config)
    }

    /// Round-trips a dataset through the arrow-native `put`/`scan` paths
    #[tokio::test]
    async fn put_then_scan_roundtrips() -> Result<()> {
        let dir = std::env::temp_dir()
            .join(format!("strata-iceberg-{}", std::process::id()))
            .to_string_lossy()
            .into_owned();
        std::fs::remove_dir_all(&dir).ok();
        let c = client(&dir)?;

        const ROWS: usize = 100;
        let schema = Event::schema();
        let generator = crate::datagen::Generator::new(&schema)?;
        let batch = generator.rows(0..ROWS)?;
        let expected: Vec<Event> = batch.decode(&schema)?;

        let result: WriteResult = c
            .put("/tables/events", DataStream::once(schema, batch))
            .await?;
        assert!(result.created);
        assert_eq!(result.rows_written, ROWS as u64);

        let mut stream = c.list::<Event>("/tables/events").await?;
        let mut got = stream.consume_all().await?;
        got.sort_by_key(|e| e.id);

        assert_eq!(got, expected);

        std::fs::remove_dir_all(&dir).ok();
        Ok(())
    }
}
