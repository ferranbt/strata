//! SQLite provider: list/read tables and sink datasets into a local `.sqlite`
//! file.
//!
//! The database file comes from the instance's config (`path`), so different
//! mounts target different files. Each call opens its own connection (via sqlx),
//! so strata only holds SQLite's single-writer file lock for the duration of an
//! operation — external tools (`sqlite3`, Python, BI) can open the file in the
//! gaps. The file is created if missing.
//!
//! The read path (`list /tables`, `/tables/:table/data`) lives in the shared
//! [`sql`](strata_sdk::sql) framework; this module supplies the SQLite
//! dialect via [`SqlSource`]. SQLite has neither `to_jsonb` (read) nor
//! `jsonb_populate_record` (write), so reads build a JSON object per row with
//! SQLite's `json_object` and writes bind each field to a positional parameter.
//! SQLite also has no native date/decimal types (only type affinities), so
//! `Timestamp`/`Date`/`Decimal` are stored as ISO/decimal `TEXT`; the *declared*
//! schema of `/tables/:table/data` is still resolved dynamically from the catalog.

use anyhow::{Context, Result, bail};
use strata_sdk::config;
use schema::{DataType, Field, Schema};
use serde_json::Value;
use sqlx::Row as _;
use sqlx::sqlite::{SqliteArguments, SqliteConnectOptions, SqlitePool};

use strata_sdk::provider::Provider;
use strata_sdk::sql::{
    self, Filter, SqlCursor, SqlError, SqlSource, WriteResult, is_table_not_found, quote_str,
};
use strata_sdk::record::{Batch, Disposition};
use strata_sdk::router::Router;

/// Bound parameters per statement. SQLite's own cap is 999 before 3.32 and higher
/// after; stay at the floor so a multi-row insert is portable.
const MAX_BIND_PARAMS: usize = 999;

#[config]
struct Config {
    path: String,
}

/// Provider state: the database file path for this instance. Each call opens its
/// own connection, so several mounts can target different files.
pub struct Sqlite {
    config: Config,
    /// Built once and reused — a fresh pool per call costs more than the writes.
    pool: tokio::sync::OnceCell<SqlitePool>,
}

impl Provider for Sqlite {
    fn name() -> &'static str {
        "sqlite"
    }

    fn new(config: &strata_sdk::config::ProviderConfig) -> Result<Self> {
        // The DB file from config (`path`). Optional so the provider still
        // registers without it; calls then error clearly.
        Ok(Sqlite {
            config: config.decode()?,
            pool: tokio::sync::OnceCell::new(),
        })
    }

    fn register(r: &mut Router<Self>) {
        Self::register_tables(r);
    }
}

impl Sqlite {
    /// The connection pool for this instance's file, opened on first use and
    /// creating the file if missing.
    async fn connect(&self) -> Result<SqlitePool> {
        let path = &self.config.path;
        self.pool
            .get_or_try_init(|| async {
                let options = SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(true);
                SqlitePool::connect_with(options)
                    .await
                    .with_context(|| format!("opening sqlite database `{path}`"))
            })
            .await
            .cloned()
    }
}

impl SqlSource for Sqlite {
    async fn table_names(&self) -> Result<Vec<String>> {
        let pool = self.connect().await?;
        let rows = sqlx::query(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' \
             ORDER BY name",
        )
        .fetch_all(&pool)
        .await?;
        Ok(rows.iter().map(|r| r.get::<String, _>("name")).collect())
    }

    async fn table_schema(&self, table: &str) -> Result<Schema> {
        let pool = self.connect().await?;
        // PRAGMA doesn't accept bind params, so the (trusted) identifier is quoted in.
        let sql = format!("PRAGMA table_info({})", quote_ident(table));
        let rows = sqlx::query(&sql).fetch_all(&pool).await?;
        if rows.is_empty() {
            return Err(SqlError::TableNotFound(table.to_string()).into());
        }
        let fields: Vec<Field> = rows
            .iter()
            .map(|r| {
                let not_null = r.get::<i64, _>("notnull") != 0;
                let mut field = Field::new(
                    r.get::<String, _>("name"),
                    sqlite_to_data_type(&r.get::<String, _>("type")),
                    !not_null,
                );
                if r.get::<i64, _>("pk") != 0 {
                    field.annotate(Field::KEY, "true");
                }
                field
            })
            .collect();
        Ok(Schema::new(fields))
    }

    async fn table_rows(
        &self,
        table: &str,
        cursor: &SqlCursor,
        filter: Option<&Filter>,
    ) -> Result<Vec<Value>> {
        let pool = self.connect().await?;
        let schema = self.table_schema(table).await?;
        // `json_object('c1', "c1", …)` turns each row into a JSON object,
        // delegating value typing (int/real/text/null) to SQLite.
        let obj_args = schema
            .fields
            .iter()
            .map(|c| format!("{}, {}", quote_str(&c.name), quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        let where_ = sql::where_clause(filter, '"')?;
        let order = match &cursor.cursor {
            Some(col) => format!("ORDER BY {} ASC ", quote_ident(col)),
            None => String::new(),
        };
        let sql = format!(
            "SELECT json_object({obj_args}) AS row FROM {} {where_}{order}LIMIT {} OFFSET {}",
            quote_ident(table),
            cursor.limit,
            cursor.offset,
        );
        let db_rows = sqlx::query(&sql).fetch_all(&pool).await?;
        db_rows
            .iter()
            .map(|r| {
                let json: String = r.get("row");
                serde_json::from_str(&json).context("parsing row JSON")
            })
            .collect()
    }

    async fn upsert_table(&self, table: &str, schema: &Schema) -> Result<bool> {
        let pool = self.connect().await?;
        let existing = match self.table_schema(table).await {
            Ok(existing) => Some(existing),
            Err(e) if is_table_not_found(&e) => None,
            Err(e) => return Err(e),
        };

        let Some(existing) = existing else {
            let keys = schema.get_key_fields();
            let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
            let mut cols: Vec<String> = schema
                .fields
                .iter()
                .map(|f| {
                    let null = if f.nullable { "" } else { " NOT NULL" };
                    format!(
                        "{} {}{}",
                        quote_ident(&f.name),
                        data_type_to_sql(&f.data_type),
                        null
                    )
                })
                .collect();
            if !keys.is_empty() {
                cols.push(format!("PRIMARY KEY ({})", quote_idents(&key_refs)));
            }
            let ddl = format!("CREATE TABLE {} ({})", quote_ident(table), cols.join(", "));
            sqlx::query(&ddl)
                .execute(&pool)
                .await
                .context("creating table")?;
            return Ok(true);
        };

        for f in &schema.fields {
            if !existing.fields.iter().any(|c| c.name == f.name) {
                bail!(
                    "column `{}` not present in existing table `{table}`",
                    f.name
                );
            }
        }
        Ok(false)
    }

    async fn write_table(
        &self,
        table: &str,
        schema: &Schema,
        data: Batch,
        disposition: Disposition,
    ) -> Result<WriteResult> {
        let fields = &schema.fields;
        let keys = schema.get_key_fields();
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let ident = quote_ident(table);
        let pool = self.connect().await?;

        // Build one parameterized INSERT and re-bind it per row. For merge, an
        // `ON CONFLICT` clause turns the insert into an upsert on the key.
        let col_idents = quote_idents(&fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>());
        let placeholders = vec!["?"; fields.len()].join(", ");
        // With a key, every write skips or overwrites an existing row rather than
        // duplicating it: `Append` skips (DO NOTHING), `Merge` overwrites the non-key
        // columns (DO UPDATE). Without a key there's nothing to conflict on.
        let conflict = if key_refs.is_empty() {
            String::new()
        } else {
            let updates: Vec<String> = match disposition {
                Disposition::Append => Vec::new(),
                Disposition::Merge => fields
                    .iter()
                    .filter(|f| !f.is_key())
                    .map(|f| {
                        let c = quote_ident(&f.name);
                        format!("{c} = excluded.{c}")
                    })
                    .collect(),
            };
            if updates.is_empty() {
                format!(" ON CONFLICT ({}) DO NOTHING", quote_idents(&key_refs))
            } else {
                format!(
                    " ON CONFLICT ({}) DO UPDATE SET {}",
                    quote_idents(&key_refs),
                    updates.join(", ")
                )
            }
        };
        // Interim: decode the Arrow rows to JSON to bind positionally (Phase B binds
        // Arrow columns directly).
        let rows = data.to_json_rows(schema)?;

        // One statement per chunk of rows rather than one per row, all inside a
        // single transaction — SQLite otherwise autocommits (and fsyncs) every insert.
        let per_statement = (MAX_BIND_PARAMS / fields.len().max(1)).max(1);
        let mut rows_written = 0u64;
        let mut tx = pool.begin().await.context("beginning transaction")?;
        for chunk in rows.chunks(per_statement) {
            let values = vec![format!("({placeholders})"); chunk.len()].join(", ");
            let insert = format!("INSERT INTO {ident} ({col_idents}) VALUES {values}{conflict}");
            let mut query = sqlx::query(&insert);
            for row in chunk {
                for f in fields {
                    let value = row.get(f.name.as_str()).unwrap_or(&Value::Null);
                    query = bind_json(query, value);
                }
            }
            let result = query.execute(&mut *tx).await.context("inserting rows")?;
            rows_written += result.rows_affected();
        }
        tx.commit().await.context("committing rows")?;

        Ok(WriteResult {
            table: table.to_string(),
            created: false,
            rows_written,
        })
    }
}

/// Bind a JSON value to the next positional parameter, mapping it to the SQLite
/// storage class (nested arrays/objects are stored as a JSON string).
fn bind_json<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>>,
    value: &Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, SqliteArguments<'q>> {
    match value {
        Value::Null => query.bind(None::<String>),
        Value::Bool(b) => query.bind(*b as i64),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(u) = n.as_u64() {
                query.bind(u as i64)
            } else {
                query.bind(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => query.bind(s.clone()),
        other => query.bind(other.to_string()),
    }
}

/// Map a [`DataType`] to the SQLite column type used when creating a table.
/// SQLite has only type affinities and no native date/decimal, so temporal and
/// decimal values are stored as ISO/decimal `TEXT`.
fn data_type_to_sql(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool | DataType::Int64 | DataType::UInt64 => "INTEGER",
        DataType::Float64 => "REAL",
        DataType::Bytes => "BLOB",
        DataType::String | DataType::Timestamp | DataType::Date | DataType::Decimal => "TEXT",
        // Nested/unknown shapes round-trip as JSON text.
        DataType::Json | DataType::List(_) | DataType::Struct(_) => "TEXT",
    }
}

/// Map a declared SQLite column type to a `DataType` using SQLite's [type
/// affinity](https://www.sqlite.org/datatype3.html#determination_of_column_affinity)
/// rules: SQLite allows arbitrary declared type names (`VARCHAR(255)`, `DATETIME`,
/// `BOOLEAN`, …), so we classify by substring rather than an exact allowlist. This
/// always resolves — hence infallible, unlike the strict server dialects.
fn sqlite_to_data_type(sql_type: &str) -> DataType {
    let ty = sql_type.to_ascii_uppercase();
    if ty.contains("INT") {
        DataType::Int64
    } else if ty.contains("CHAR") || ty.contains("CLOB") || ty.contains("TEXT") {
        DataType::String
    } else if ty.contains("BLOB") || ty.is_empty() {
        DataType::Bytes
    } else if ty.contains("REAL") || ty.contains("FLOA") || ty.contains("DOUB") {
        DataType::Float64
    } else {
        // NUMERIC affinity (incl. `NUMERIC`/`DECIMAL`/`DATETIME`/`BOOLEAN`): stored
        // and surfaced as text.
        DataType::String
    }
}

/// Quote a SQLite identifier (double-quoted).
fn quote_ident(ident: &str) -> String {
    sql::quote_ident(ident, '"')
}

/// Quote and comma-join SQLite identifiers — for `(col1, col2, …)` clauses.
fn quote_idents(idents: &[&str]) -> String {
    sql::quote_idents(idents, '"')
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_sdk::sql;

    /// Run the shared SQL suite against a throwaway sqlite file.
    #[tokio::test]
    async fn sql_suite() -> Result<()> {
        let path = std::env::temp_dir().join("strata_sqlite_suite.sqlite");
        let _ = std::fs::remove_file(&path);
        let config: strata_sdk::config::ProviderConfig = serde_json::from_value(serde_json::json!({
            "backend": "sqlite",
            "path": path.to_str().unwrap(),
        }))?;
        let client = strata_sdk::testkit::Client::<Sqlite>::mount(&config)?;
        sql::suite::append_skips_duplicates(&client).await?;
        sql::suite::merge_overwrites(&client).await?;
        sql::suite::lists_tables_and_schema(&client).await?;
        sql::suite::write_then_read_paginates_by_cursor(&client).await?;
        sql::suite::filters_rows(&client).await?;
        sql::suite::projects_columns(&client).await?;
        sql::suite::gets_single_row(&client).await?;
        sql::suite::streams_whole_table(&client).await?;
        let _ = std::fs::remove_file(&path);
        Ok(())
    }
}
