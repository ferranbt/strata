use anyhow::{Context, Result, bail};
use strata_schema::{DataType, Field, Schema};
use serde_json::Value;
use tokio_postgres::NoTls;

use strata_sdk::sql::{
    self, Filter, SqlCursor, SqlError, SqlSource, WriteResult, is_table_not_found,
};
use strata_sdk::record::{Batch, Disposition};
use strata_sdk::router::Router;
use strata_sdk::config;

#[derive(strata_sdk::Provider)]
#[config(Config)]
pub struct Postgres {
    config: Config,
}

#[config]
struct Config {
    #[config(env = "DATABASE_URL", description = "postgres:// connection URL", secret)]
    url: String,
}

impl Postgres {
    fn new(config: Config) -> Result<Self> {
        Ok(Postgres { config })
    }

    fn routes(r: &mut Router<Self>) {
        Self::register_tables(r);
    }

    async fn connect(&self) -> Result<tokio_postgres::Client> {
        let url = &self.config.url;
        let (client, connection) = tokio_postgres::connect(url, NoTls)
            .await
            .context("connecting to postgres")?;
        tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::error!("postgres connection error: {error}");
            }
        });
        Ok(client)
    }
}

impl SqlSource for Postgres {
    async fn table_names(&self) -> Result<Vec<String>> {
        let client = self.connect().await?;
        let rows = client
            .query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema NOT IN ('pg_catalog', 'information_schema') \
                 ORDER BY table_schema, table_name",
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|r| r.get::<_, String>("table_name"))
            .collect())
    }

    async fn table_schema(&self, table: &str) -> Result<Schema> {
        let client = self.connect().await?;
        let rows = client
            .query(
                "SELECT column_name, data_type, is_nullable \
                 FROM information_schema.columns WHERE table_name = $1 \
                 ORDER BY ordinal_position",
                &[&table],
            )
            .await?;
        if rows.is_empty() {
            return Err(SqlError::TableNotFound(table.to_string()).into());
        }
        let pk_rows = client
            .query(
                "SELECT a.attname AS name FROM pg_index i \
                 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) \
                 WHERE i.indrelid = to_regclass($1) AND i.indisprimary",
                &[&table],
            )
            .await?;
        let keys: std::collections::HashSet<String> =
            pk_rows.iter().map(|r| r.get::<_, String>("name")).collect();
        let columns = rows
            .iter()
            .map(|r| {
                let name: String = r.get("column_name");
                let sql_type: String = r.get("data_type");
                let nullable = r.get::<_, String>("is_nullable") == "YES";
                let mut field = Field::new(name.clone(), pg_to_data_type(&sql_type)?, nullable);
                if keys.contains(&name) {
                    field.annotate(Field::KEY, "true");
                }
                Ok(field)
            })
            .collect::<Result<_>>()?;
        Ok(Schema::new(columns))
    }

    async fn table_rows(
        &self,
        table: &str,
        cursor: &SqlCursor,
        filter: Option<&Filter>,
    ) -> Result<Vec<Value>> {
        let client = self.connect().await?;
        let (namespace, name) = resolve_table(&client, table).await?;
        let where_ = sql::where_clause(filter, '"')?;
        let order = match &cursor.cursor {
            Some(col) => format!("ORDER BY {} ASC ", quote_ident(col)),
            None => String::new(),
        };
        let sql = format!(
            "SELECT to_jsonb(t) AS row FROM {}.{} t {where_}{order}LIMIT {} OFFSET {}",
            quote_ident(&namespace),
            quote_ident(&name),
            cursor.limit,
            cursor.offset,
        );
        let rows = client.query(&sql, &[]).await?;
        Ok(rows.iter().map(|r| r.get::<_, Value>("row")).collect())
    }

    async fn upsert_table(&self, table: &str, schema: &Schema) -> Result<bool> {
        let client = self.connect().await?;
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
            let ddl = format!(
                "CREATE TABLE public.{} ({})",
                quote_ident(table),
                cols.join(", ")
            );
            client.execute(&ddl, &[]).await.context("creating table")?;
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
        let client = self.connect().await?;

        // Write each row by populating the table's rowtype from the row's JSON. The
        // row binds directly as `jsonb` (tokio-postgres `with-serde_json-1`). With a
        // key, a conflicting row is skipped (`Append`) or overwritten (`Merge`)
        // rather than duplicated; without a key there's nothing to conflict on.
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
                        format!("{c} = EXCLUDED.{c}")
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
        // `jsonb_populate_recordset` expands a whole JSON *array* into the table's
        // rowtype, so the batch is one statement and one round-trip.
        let insert = format!(
            "INSERT INTO public.{ident} SELECT * FROM jsonb_populate_recordset(NULL::public.{ident}, $1){conflict}"
        );
        // Interim: decode the Arrow rows to JSON to bind as jsonb (Phase B binds
        // Arrow columns directly).
        let rows = Value::Array(data.to_json_rows(schema)?);
        let rows_written = client
            .execute(&insert, &[&rows])
            .await
            .context("inserting rows")?;

        Ok(WriteResult {
            table: table.to_string(),
            created: false,
            rows_written,
        })
    }
}

/// Resolve a table name to its real `(schema, name)` via the catalog (trusted,
/// parameterized) before interpolating identifiers into a query.
async fn resolve_table(client: &tokio_postgres::Client, name: &str) -> Result<(String, String)> {
    let meta = client
        .query_opt(
            "SELECT table_schema, table_name FROM information_schema.tables \
             WHERE table_name = $1 LIMIT 1",
            &[&name],
        )
        .await?
        .ok_or_else(|| SqlError::TableNotFound(name.to_string()))?;
    Ok((meta.get("table_schema"), meta.get("table_name")))
}

/// Map a [`DataType`] to the SQL column type used when creating a table.
fn data_type_to_sql(dt: &DataType) -> &'static str {
    match dt {
        DataType::Bool => "boolean",
        DataType::Int64 => "bigint",
        DataType::UInt64 => "bigint",
        DataType::Float64 => "double precision",
        DataType::Decimal => "numeric",
        DataType::String => "text",
        DataType::Bytes => "bytea",
        DataType::Timestamp => "timestamptz",
        DataType::Date => "date",
        // Nested/unknown shapes round-trip as JSON.
        DataType::Json | DataType::List(_) | DataType::Struct(_) => "jsonb",
    }
}

/// Map an `information_schema.columns.data_type` string to a [`DataType`].
/// Unrecognized types raise [`SqlError::UnsupportedColumnType`] rather than
/// silently becoming text.
fn pg_to_data_type(sql_type: &str) -> Result<DataType, SqlError> {
    Ok(match sql_type {
        "boolean" => DataType::Bool,
        "smallint" | "integer" | "bigint" => DataType::Int64,
        "real" | "double precision" => DataType::Float64,
        "numeric" => DataType::Decimal,
        "timestamp with time zone" | "timestamp without time zone" => DataType::Timestamp,
        "date" => DataType::Date,
        "bytea" => DataType::Bytes,
        "json" | "jsonb" | "ARRAY" => DataType::Json,
        // Text-like scalars (and types with no closer IR match) ride as strings.
        "text"
        | "character varying"
        | "character"
        | "bpchar"
        | "name"
        | "uuid"
        | "citext"
        | "xml"
        | "money"
        | "interval"
        | "time without time zone"
        | "time with time zone"
        | "inet"
        | "cidr"
        | "macaddr"
        | "bit"
        | "bit varying" => DataType::String,
        other => return Err(SqlError::UnsupportedColumnType(other.to_string())),
    })
}

fn quote_ident(ident: &str) -> String {
    sql::quote_ident(ident, '"')
}

fn quote_idents(idents: &[&str]) -> String {
    sql::quote_idents(idents, '"')
}

#[cfg(test)]
mod tests {
    use super::*;
    use strata_dockertest::{Dockertest, Running};

    async fn start_postgres_server() -> Result<(Running, strata_sdk::config::ProviderConfig)> {
        let server = Dockertest::image("postgres")
            .tag("latest")
            .env("POSTGRES_USER", "strata")
            .env("POSTGRES_PASSWORD", "strata")
            .env("POSTGRES_DB", "strata")
            .port(5432)
            .retry_attempts(60)
            .retry(|ep| async move {
                let url = format!(
                    "postgres://strata:strata@{}:{}/strata",
                    ep.host(),
                    ep.port(5432)
                );
                match tokio_postgres::connect(&url, NoTls).await {
                    Ok((client, connection)) => {
                        tokio::spawn(async move {
                            let _ = connection.await;
                        });
                        client.simple_query("SELECT 1").await.is_ok()
                    }
                    Err(_) => false,
                }
            })
            .build()
            .await?;

        let url = format!(
            "postgres://strata:strata@{}:{}/strata",
            server.host(),
            server.port(5432)
        );
        let map: serde_json::Map<String, Value> = [("backend", "postgres"), ("url", url.as_str())]
            .into_iter()
            .map(|(k, v)| (k.to_string(), Value::String(v.to_string())))
            .collect();
        let config = serde_json::from_value(Value::Object(map)).unwrap();

        Ok((server, config))
    }

    #[tokio::test]
    async fn sql_suite() -> Result<()> {
        let (_server, config) = start_postgres_server().await?;
        let client = strata_sdk::testkit::Client::<Postgres>::mount(&config)?;
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
