use anyhow::{Context, Result, bail};
use config_macro::config;
use schema::{DataType, Field, Schema};
use serde_json::Value;
use sqlx::Row as _;
use sqlx::mysql::{MySqlArguments, MySqlPool};

use strata_sdk::provider::Provider;
use strata_sdk::sql::{
    self, Filter, SqlCursor, SqlError, SqlSource, WriteResult, is_table_not_found, quote_str,
};
use strata_sdk::record::{Batch, Disposition};
use strata_sdk::router::Router;

/// Placeholders per statement. MySQL's protocol caps them at 65535; stay well
/// under so a wide multi-row insert can't trip it.
const MAX_BIND_PARAMS: usize = 16384;

pub struct Mysql {
    config: Config,
    /// Built once and reused — a fresh pool per call costs more than the writes.
    pool: tokio::sync::OnceCell<MySqlPool>,
}

#[config]
struct Config {
    #[config(default_env = "MYSQL_URL")]
    url: String,
}

impl Provider for Mysql {
    fn name() -> &'static str {
        "mysql"
    }

    fn new(config: &strata_sdk::config::ProviderConfig) -> Result<Self> {
        Ok(Mysql {
            config: config.decode()?,
            pool: tokio::sync::OnceCell::new(),
        })
    }

    fn register(r: &mut Router<Self>) {
        Self::register_tables(r);
    }
}

impl Mysql {
    async fn connect(&self) -> Result<MySqlPool> {
        self.pool
            .get_or_try_init(|| async {
                MySqlPool::connect(&self.config.url)
                    .await
                    .context("connecting to mysql")
            })
            .await
            .cloned()
    }
}

impl SqlSource for Mysql {
    async fn table_names(&self) -> Result<Vec<String>> {
        let pool = self.connect().await?;
        let rows = sqlx::query(
            "SELECT CAST(table_name AS CHAR) AS `name` FROM information_schema.tables \
             WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE' ORDER BY table_name",
        )
        .fetch_all(&pool)
        .await?;
        Ok(rows.iter().map(|r| r.get::<String, _>("name")).collect())
    }

    async fn table_schema(&self, table: &str) -> Result<Schema> {
        let pool = self.connect().await?;

        let rows = sqlx::query(
            "SELECT CAST(column_name AS CHAR) AS `name`, CAST(data_type AS CHAR) AS `type`, \
                CAST(is_nullable AS CHAR) AS `nullable`, CAST(column_key AS CHAR) AS `key` \
         FROM information_schema.columns \
         WHERE table_schema = DATABASE() AND table_name = ? ORDER BY ordinal_position",
        )
        .bind(table)
        .fetch_all(&pool)
        .await?;

        if rows.is_empty() {
            return Err(SqlError::TableNotFound(table.to_string()).into());
        }
        let fields = rows
            .iter()
            .map(|r| {
                let mut field = Field::new(
                    r.get::<String, _>("name"),
                    mysql_to_data_type(&r.get::<String, _>("type"))?,
                    r.get::<String, _>("nullable") == "YES",
                );
                if r.get::<String, _>("key") == "PRI" {
                    field.annotate(Field::KEY, "true");
                }
                Ok(field)
            })
            .collect::<Result<Vec<Field>>>()?;
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
        // `JSON_OBJECT('c1', `c1`, …)` turns each row into a JSON object; CAST to
        // CHAR so it decodes as text. Native `JSON` columns embed as nested objects.
        let obj_args = schema
            .fields
            .iter()
            .map(|c| format!("{}, {}", quote_str(&c.name), quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        let where_ = sql::where_clause(filter, '`')?;
        let order = match &cursor.cursor {
            Some(col) => format!("ORDER BY {} ASC ", quote_ident(col)),
            None => String::new(),
        };
        let sql = format!(
            "SELECT CAST(JSON_OBJECT({obj_args}) AS CHAR) AS `row` FROM {} \
             {where_}{order}LIMIT {} OFFSET {}",
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
            let key_set: std::collections::HashSet<&str> = key_refs.iter().copied().collect();
            let mut cols: Vec<String> = schema
                .fields
                .iter()
                .map(|f| {
                    let null = if f.nullable { "" } else { " NOT NULL" };
                    let sql_type =
                        data_type_to_sql(&f.data_type, key_set.contains(f.name.as_str()));
                    format!("{} {}{}", quote_ident(&f.name), sql_type, null)
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
        // `ON DUPLICATE KEY UPDATE` clause turns the insert into an upsert on the key.
        let col_idents = quote_idents(&fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>());
        let placeholders = vec!["?"; fields.len()].join(", ");
        // With a key, a conflicting row is skipped (`Append`) or overwritten
        // (`Merge`). MySQL has no `DO NOTHING`, so a skip is a no-op self-assign on
        // the key. Without a key there's nothing to conflict on.
        let update = if key_refs.is_empty() {
            String::new()
        } else {
            let non_key: Vec<String> = match disposition {
                Disposition::Append => Vec::new(),
                Disposition::Merge => fields
                    .iter()
                    .filter(|f| !f.is_key())
                    .map(|f| {
                        let c = quote_ident(&f.name);
                        format!("{c} = VALUES({c})")
                    })
                    .collect(),
            };
            // `Append`, or a `Merge` where every column is a key → skip via a no-op
            // self-assign on the first key column.
            let assignments = if non_key.is_empty() {
                let k = quote_ident(&keys[0]);
                format!("{k} = {k}")
            } else {
                non_key.join(", ")
            };
            format!(" ON DUPLICATE KEY UPDATE {assignments}")
        };
        // Interim: decode the Arrow rows to JSON to bind positionally (Phase B binds
        // Arrow columns directly).
        let rows = data.to_json_rows(schema)?;

        // One statement per chunk of rows rather than one per row, chunked by the
        // placeholder budget a single statement can carry.
        let per_statement = (MAX_BIND_PARAMS / fields.len().max(1)).max(1);
        let mut rows_written = 0u64;
        for chunk in rows.chunks(per_statement) {
            let values = vec![format!("({placeholders})"); chunk.len()].join(", ");
            let insert = format!("INSERT INTO {ident} ({col_idents}) VALUES {values}{update}");
            let mut query = sqlx::query(&insert);
            for row in chunk {
                for f in fields {
                    let value = row.get(f.name.as_str()).unwrap_or(&Value::Null);
                    query = bind_json(query, value);
                }
            }
            let result = query.execute(&pool).await.context("inserting rows")?;
            rows_written += result.rows_affected();
        }

        Ok(WriteResult {
            table: table.to_string(),
            created: false,
            rows_written,
        })
    }
}

/// Bind a JSON value to the next positional parameter, mapping it to the MySQL
/// param type (nested arrays/objects are bound as a JSON string, which MySQL
/// parses into a `JSON` column).
fn bind_json<'q>(
    query: sqlx::query::Query<'q, sqlx::MySql, MySqlArguments>,
    value: &Value,
) -> sqlx::query::Query<'q, sqlx::MySql, MySqlArguments> {
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

/// Map a [`DataType`] to the MySQL column type used when creating a table. String
/// key columns become `VARCHAR(255)` (MySQL can't index `TEXT` without a prefix
/// length), non-key strings become `TEXT`.
fn data_type_to_sql(dt: &DataType, is_key: bool) -> &'static str {
    match dt {
        DataType::Bool => "BOOLEAN",
        DataType::Int64 | DataType::UInt64 => "BIGINT",
        DataType::Float64 => "DOUBLE",
        DataType::Decimal => "DECIMAL(38,10)",
        DataType::String => {
            if is_key {
                "VARCHAR(255)"
            } else {
                "TEXT"
            }
        }
        DataType::Bytes => "LONGBLOB",
        DataType::Timestamp => "DATETIME",
        DataType::Date => "DATE",
        // Nested/unknown shapes round-trip as native JSON.
        DataType::Json | DataType::List(_) | DataType::Struct(_) => "JSON",
    }
}

/// Map a MySQL `information_schema.columns.data_type` to a [`DataType`].
/// Unrecognized types raise [`SqlError::UnsupportedColumnType`] rather than
/// silently becoming text.
fn mysql_to_data_type(sql_type: &str) -> Result<DataType, SqlError> {
    Ok(match sql_type.to_ascii_lowercase().as_str() {
        "tinyint" | "smallint" | "mediumint" | "int" | "integer" | "bigint" => DataType::Int64,
        "float" | "double" => DataType::Float64,
        "decimal" | "numeric" => DataType::Decimal,
        "datetime" | "timestamp" => DataType::Timestamp,
        "date" => DataType::Date,
        "json" => DataType::Json,
        "binary" | "varbinary" | "blob" | "tinyblob" | "mediumblob" | "longblob" => DataType::Bytes,
        // Text-like scalars (and types with no closer IR match) ride as strings.
        "char" | "varchar" | "text" | "tinytext" | "mediumtext" | "longtext" | "enum" | "set"
        | "time" | "year" | "bit" => DataType::String,
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

    async fn start_mysql_server() -> Result<(Running, strata_sdk::config::ProviderConfig)> {
        let server = Dockertest::image("mysql")
            .tag("latest")
            .env("MYSQL_ROOT_PASSWORD", "strata")
            .env("MYSQL_DATABASE", "strata")
            .env("MYSQL_USER", "strata")
            .env("MYSQL_PASSWORD", "strata")
            .port(3306)
            .retry_attempts(60)
            .retry(|ep| async move {
                let url = format!(
                    "mysql://strata:strata@{}:{}/strata",
                    ep.host(),
                    ep.port(3306)
                );
                match MySqlPool::connect(&url).await {
                    Ok(pool) => sqlx::query("SELECT 1").execute(&pool).await.is_ok(),
                    Err(_) => false,
                }
            })
            .build()
            .await?;

        let url = format!(
            "mysql://strata:strata@{}:{}/strata",
            server.host(),
            server.port(3306)
        );
        let map: serde_json::Map<String, Value> = [("backend", "mysql"), ("url", url.as_str())]
            .into_iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
            .collect();
        let config = serde_json::from_value(Value::Object(map)).unwrap();

        Ok((server, config))
    }

    #[tokio::test]
    async fn sql_suite() -> Result<()> {
        let (_server, config) = start_mysql_server().await?;
        let client = strata_sdk::testkit::Client::<Mysql>::mount(&config)?;
        sql::suite::append_skips_duplicates(&client).await?;
        sql::suite::merge_overwrites(&client).await?;
        sql::suite::lists_tables_and_schema(&client).await?;
        sql::suite::write_then_read_paginates_by_cursor(&client).await?;
        sql::suite::filters_rows(&client).await?;
        sql::suite::projects_columns(&client).await?;
        sql::suite::gets_single_row(&client).await?;
        sql::suite::streams_whole_table(&client).await?;
        Ok(())
    }
}
