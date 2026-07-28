use anyhow::{Context, Result, anyhow, bail};
use config_macro::config;
use http_client::HttpClient;
use schema::{DataType, Field, Schema};
use serde::Deserialize;
use serde_json::Value;

use crate::dataset::{Dataset, Disposition};
use crate::provider::Provider;
use crate::providers::sql::{self, Filter, SqlCursor, SqlError, SqlSource, WriteResult, quote_str};
use crate::router::Router;

#[config]
struct Config {
    #[config(default_env = "CLICKHOUSE_URL")]
    url: String,
    #[config(default_env = "CLICKHOUSE_DATABASE")]
    database: String,
    #[config(default_env = "CLICKHOUSE_USER")]
    user: String,
    #[config(default_env = "CLICKHOUSE_PASSWORD")]
    password: String,
}

pub struct Clickhouse {
    http: HttpClient,
}

impl Provider for Clickhouse {
    fn name() -> &'static str {
        "clickhouse"
    }

    fn new(config: &crate::config::ProviderConfig) -> Result<Self> {
        let config: Config = config.decode()?;
        let http = HttpClient::builder()
            .basic_auth(&config.user, Some(&config.password))
            .base_url(config.url.clone())
            .query([
                ("database", config.database.as_str()),
                ("date_time_input_format", "best_effort"),
            ])
            .no_cache()
            .build()
            .context("building http client")?;

        Ok(Clickhouse { http })
    }

    fn register(r: &mut Router<Self>) {
        Self::register_tables(r);
    }
}

impl Clickhouse {
    async fn run(&self, sql: String) -> Result<String> {
        let request = self.http.post("").body(sql);
        let response = self
            .http
            .send(request)
            .await
            .context("clickhouse request")?;
        response.text().await.context("reading clickhouse response")
    }

    async fn query_rows(&self, sql: String) -> Result<Vec<Value>> {
        let body = self.run(sql).await?;
        body.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).context("parsing clickhouse JSON row"))
            .collect()
    }

    async fn table_columns(&self, table: &str) -> Result<Vec<Field>> {
        let sql = format!(
            "SELECT name, type FROM system.columns \
             WHERE database = currentDatabase() AND table = {} \
             ORDER BY position FORMAT JSONEachRow",
            quote_str(table),
        );
        self.query_rows(sql)
            .await?
            .into_iter()
            .map(|r| {
                let raw: RawColumn = serde_json::from_value(r).context("decoding column row")?;
                let (base, nullable) = strip_nullable(&raw.sql_type);
                Ok(Field::new(raw.name, ch_to_data_type(base)?, nullable))
            })
            .collect()
    }
}

impl SqlSource for Clickhouse {
    async fn table_names(&self) -> Result<Vec<String>> {
        let sql = "SELECT name FROM system.tables WHERE database = currentDatabase() \
                   ORDER BY name FORMAT JSONEachRow"
            .to_string();
        self.query_rows(sql)
            .await?
            .into_iter()
            .map(|r| {
                r.get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .ok_or_else(|| anyhow!("table row missing `name`"))
            })
            .collect()
    }

    async fn table_schema(&self, table: &str) -> Result<Schema> {
        let columns = self.table_columns(table).await?;
        if columns.is_empty() {
            return Err(SqlError::TableNotFound(table.to_string()).into());
        }
        Ok(Schema::new(columns))
    }

    async fn table_rows(
        &self,
        table: &str,
        cursor: &SqlCursor,
        filter: Option<&Filter>,
    ) -> Result<Vec<Value>> {
        // `FINAL` collapses a ReplacingMergeTree's duplicate keys at query time;
        // `output_format_json_quote_64bit_integers = 0` keeps Int64/UInt64 as JSON
        // numbers rather than ClickHouse's default quoted strings. `ORDER BY` must
        // precede `SETTINGS`/`FORMAT`.
        let where_ = sql::where_clause(filter, '`')?;
        let order = match &cursor.cursor {
            Some(col) => format!("ORDER BY {} ASC ", quote_ident(col)),
            None => String::new(),
        };
        let sql = format!(
            "SELECT * FROM {} FINAL {where_}{order}LIMIT {} OFFSET {} \
             SETTINGS output_format_json_quote_64bit_integers = 0 FORMAT JSONEachRow",
            quote_ident(table),
            cursor.limit,
            cursor.offset,
        );
        self.query_rows(sql).await
    }

    async fn upsert_table(&self, table: &str, schema: &Schema) -> Result<bool> {
        let keys = schema.get_key_fields();
        let key_refs: Vec<&str> = keys.iter().map(String::as_str).collect();
        let ident = quote_ident(table);

        // Create from the schema if absent (a ReplacingMergeTree ordered by the key,
        // so duplicate keys collapse); otherwise require the dataset's columns to
        // exist.
        let existing = self.table_columns(table).await?;
        let created = existing.is_empty();
        if created {
            let key_set: std::collections::HashSet<&str> = key_refs.iter().copied().collect();
            let cols: Vec<String> = schema
                .fields
                .iter()
                .map(|f| {
                    let base = data_type_to_ch(&f.data_type);
                    // Sorting-key columns must be non-nullable; other nullable columns
                    // are wrapped in `Nullable`.
                    let ty = if f.nullable && !key_set.contains(f.name.as_str()) {
                        format!("Nullable({base})")
                    } else {
                        base.to_string()
                    };
                    format!("{} {}", quote_ident(&f.name), ty)
                })
                .collect();
            let engine = if keys.is_empty() {
                "ENGINE = MergeTree ORDER BY tuple()".to_string()
            } else {
                format!(
                    "ENGINE = ReplacingMergeTree ORDER BY ({})",
                    quote_idents(&key_refs)
                )
            };
            let ddl = format!("CREATE TABLE {ident} ({}) {engine}", cols.join(", "));
            self.run(ddl).await.context("creating table")?;
        } else {
            for f in &schema.fields {
                if !existing.iter().any(|c| c.name == f.name) {
                    bail!(
                        "column `{}` not present in existing table `{table}`",
                        f.name
                    );
                }
            }
        }
        Ok(created)
    }

    async fn write_table(
        &self,
        table: &str,
        data: Dataset,
        _disposition: Disposition,
    ) -> Result<WriteResult> {
        let schema = &data.schema;
        let ident = quote_ident(table);

        // Build a single `INSERT … FORMAT JSONEachRow` body: one JSON object per row,
        // with nested (JSON-typed) columns serialized to a string to match their
        // `String` storage.
        // ClickHouse's HTTP interface speaks JSONEachRow, so decode the Arrow rows to
        // JSON here — this is the one provider where JSON is the wire protocol, not an
        // interim bridge.
        let rows = data.to_json_rows()?;
        let mut rows_written = 0u64;
        if !rows.is_empty() {
            let mut body = format!("INSERT INTO {ident} FORMAT JSONEachRow\n");
            for row in &rows {
                let mut obj = serde_json::Map::with_capacity(schema.fields.len());
                for f in &schema.fields {
                    let value = row.get(f.name.as_str()).cloned().unwrap_or(Value::Null);
                    let value = match &f.data_type {
                        // Stored as `String`; nest as a JSON string, not a bare object.
                        DataType::Json | DataType::List(_) | DataType::Struct(_) => match value {
                            Value::Null => Value::Null,
                            other => Value::String(other.to_string()),
                        },
                        _ => value,
                    };
                    obj.insert(f.name.clone(), value);
                }
                body.push_str(&serde_json::to_string(&Value::Object(obj))?);
                body.push('\n');
            }
            self.run(body).await.context("inserting rows")?;
            rows_written = rows.len() as u64;
        }

        Ok(WriteResult {
            table: table.to_string(),
            created: false,
            rows_written,
        })
    }
}

/// A raw `system.columns` row, before `Nullable(…)`/type resolution.
#[derive(Deserialize)]
struct RawColumn {
    name: String,
    #[serde(rename = "type")]
    sql_type: String,
}

/// Split a ClickHouse `Nullable(T)` wrapper, returning `(inner_type, nullable)`.
fn strip_nullable(ty: &str) -> (&str, bool) {
    match ty
        .strip_prefix("Nullable(")
        .and_then(|t| t.strip_suffix(')'))
    {
        Some(inner) => (inner, true),
        None => (ty, false),
    }
}

fn data_type_to_ch(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool => "Bool",
        DataType::Int64 => "Int64",
        DataType::UInt64 => "UInt64",
        DataType::Float64 => "Float64",
        DataType::Decimal => "Decimal(38, 10)",
        DataType::String | DataType::Bytes => "String",
        DataType::Timestamp => "DateTime64(3)",
        DataType::Date => "Date",
        DataType::Json | DataType::List(_) | DataType::Struct(_) => "String",
    }
}

fn ch_to_data_type(ty: &str) -> Result<DataType, SqlError> {
    let head = ty.split('(').next().unwrap_or(ty);
    Ok(match head {
        "Bool" => DataType::Bool,
        "Int8" | "Int16" | "Int32" | "Int64" | "Int128" | "Int256" | "UInt8" | "UInt16"
        | "UInt32" | "UInt64" | "UInt128" | "UInt256" => DataType::Int64,
        "Float32" | "Float64" => DataType::Float64,
        "Decimal" | "Decimal32" | "Decimal64" | "Decimal128" | "Decimal256" => DataType::Decimal,
        "DateTime" | "DateTime64" => DataType::Timestamp,
        "Date" | "Date32" => DataType::Date,
        // Nested/composite shapes are stored as JSON strings.
        "Array" | "Tuple" | "Map" | "Nested" => DataType::Json,
        // Text-like scalars (and types with no closer IR match) ride as strings.
        "String" | "FixedString" | "UUID" | "Enum8" | "Enum16" | "IPv4" | "IPv6" => {
            DataType::String
        }
        other => return Err(SqlError::UnsupportedColumnType(other.to_string())),
    })
}

fn quote_ident(ident: &str) -> String {
    sql::quote_ident(ident, '`')
}

fn quote_idents(idents: &[&str]) -> String {
    sql::quote_idents(idents, '`')
}

#[cfg(test)]
mod tests {
    use super::*;
    use dockertest::{Dockertest, Running};

    async fn start_clickhouse_server() -> Result<(Running, crate::config::ProviderConfig)> {
        let server = Dockertest::image("clickhouse/clickhouse-server")
            .tag("latest")
            .env("CLICKHOUSE_USER", "strata")
            .env("CLICKHOUSE_PASSWORD", "strata")
            .port(8123)
            .retry_attempts(60)
            .retry(|ep| async move {
                match HttpClient::builder()
                    .base_url(ep.url("http", 8123))
                    .no_cache()
                    .build()
                {
                    Ok(client) => client.send(client.get("/ping")).await.is_ok(),
                    Err(_) => false,
                }
            })
            .build()
            .await?;

        let url = server.url("http", 8123);
        let map: serde_json::Map<String, Value> = [
            ("backend", "clickhouse"),
            ("url", url.as_str()),
            ("database", "default"),
            ("user", "strata"),
            ("password", "strata"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
        .collect();
        let config = serde_json::from_value(Value::Object(map)).unwrap();

        Ok((server, config))
    }

    #[tokio::test]
    async fn sql_suite() -> Result<()> {
        let (_server, config) = start_clickhouse_server().await?;
        let client = crate::testkit::Client::<Clickhouse>::mount(&config)?;
        sql::suite::append_skips_duplicates(&client).await?;
        sql::suite::merge_overwrites(&client).await?;
        sql::suite::lists_tables_and_schema(&client).await?;
        sql::suite::write_then_read_paginates_by_cursor(&client).await?;
        sql::suite::filters_rows(&client).await?;
        Ok(())
    }
}
