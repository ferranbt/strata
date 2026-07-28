use schema::Schema;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use strata_types::{Endpoint, Pipe};
use uuid::Uuid;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

const MAX_CONNECTIONS: u32 = 5;
const DEFAULT_DBN: &str = "postgresql://postgres:postgres@localhost:5432/strata?sslmode=disable";

#[derive(Clone, Debug)]
pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(database_url: Option<&str>) -> anyhow::Result<Self> {
        let url = database_url.unwrap_or(DEFAULT_DBN);

        let pool = PgPoolOptions::new()
            .max_connections(MAX_CONNECTIONS)
            .connect(url)
            .await
            .map_err(|e| anyhow::anyhow!("connect Postgres: {e}"))?;
        MIGRATOR
            .run(&pool)
            .await
            .map_err(|e| anyhow::anyhow!("run migrations: {e}"))?;

        Ok(Database { pool })
    }

    /// Insert a pipe, skipping if one with the same `(source, destination)` already
    /// exists (`ON CONFLICT DO NOTHING`)
    pub async fn create_pipe(&self, pipe: &Pipe) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "INSERT INTO pipes
                 (id, source_mount, source_path, destination_mount, destination_path, cursor)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (source_mount, source_path, destination_mount, destination_path)
             DO NOTHING",
        )
        .bind(pipe.id)
        .bind(&pipe.source.mount)
        .bind(&pipe.source.path)
        .bind(&pipe.destination.mount)
        .bind(&pipe.destination.path)
        .bind(&pipe.cursor)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Advance a pipe's resume `cursor`, addressed by its `id`.
    pub async fn update_cursor(&self, id: Uuid, cursor: Option<String>) -> anyhow::Result<()> {
        sqlx::query("UPDATE pipes SET cursor = $2, updated_at = now() WHERE id = $1")
            .bind(id)
            .bind(cursor)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn list_pipes(&self) -> anyhow::Result<Vec<Pipe>> {
        let rows = sqlx::query(
            "SELECT id, source_mount, source_path, destination_mount, destination_path, cursor
             FROM pipes
             ORDER BY source_mount, source_path, destination_mount, destination_path",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| Pipe {
                id: r.get("id"),
                source: Endpoint {
                    mount: r.get("source_mount"),
                    path: r.get("source_path"),
                },
                destination: Endpoint {
                    mount: r.get("destination_mount"),
                    path: r.get("destination_path"),
                },
                cursor: r.get("cursor"),
            })
            .collect())
    }

    /// Upsert the canonical [`DataType`] schema strata knows for a source `endpoint`.
    pub async fn upsert_source_schema(
        &self,
        endpoint: &Endpoint,
        schema: &Schema,
    ) -> anyhow::Result<()> {
        let json = serde_json::to_string(schema)?;
        sqlx::query(
            "INSERT INTO source_schemas (mount, path, schema)
             VALUES ($1, $2, $3)
             ON CONFLICT (mount, path)
             DO UPDATE SET schema = EXCLUDED.schema, updated_at = now()",
        )
        .bind(&endpoint.mount)
        .bind(&endpoint.path)
        .bind(&json)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The stored schema for a source `endpoint`, or `None` if none is persisted.
    pub async fn get_source_schema(&self, endpoint: &Endpoint) -> anyhow::Result<Option<Schema>> {
        let row = sqlx::query("SELECT schema FROM source_schemas WHERE mount = $1 AND path = $2")
            .bind(&endpoint.mount)
            .bind(&endpoint.path)
            .fetch_optional(&self.pool)
            .await?;
        match row {
            Some(row) => Ok(Some(serde_json::from_str(&row.get::<String, _>("schema"))?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use schema::SchemaBuilder;

    use super::*;
    use crate::testing::TestPostgres;

    #[tokio::test]
    async fn migrations_apply_cleanly() -> anyhow::Result<()> {
        let pg = TestPostgres::start().await?;
        let _db = Database::new(Some(pg.url.as_str())).await?;

        Ok(())
    }

    #[tokio::test]
    async fn pipe_create_list_upsert_roundtrip() -> anyhow::Result<()> {
        let pg = TestPostgres::start().await?;
        let db = Database::new(Some(pg.url.as_str())).await?;

        let pipe = Pipe::new(
            Endpoint::new("congress", "/bill/119/hr?limit=20"),
            Endpoint::new("postgres", "/tables/bills"),
        );

        assert!(db.create_pipe(&pipe).await?);
        assert!(!db.create_pipe(&pipe).await?);

        let listed = db.list_pipes().await?;
        assert_eq!(listed, vec![pipe.clone()]);
        assert_eq!(listed[0].id, pipe.id);
        assert_eq!(listed[0].cursor, None);

        // `update_cursor` advances the resume token in place, by id.
        db.update_cursor(pipe.id, Some("20".to_string())).await?;
        let advanced = Pipe {
            cursor: Some("20".to_string()),
            ..pipe.clone()
        };
        let listed = db.list_pipes().await?;
        assert_eq!(listed, vec![advanced]);

        Ok(())
    }

    #[tokio::test]
    async fn source_schema_upsert_get_roundtrip() -> anyhow::Result<()> {
        let pg = TestPostgres::start().await?;
        let db = Database::new(Some(pg.url.as_str())).await?;

        let endpoint = Endpoint::new("postgres", "/tables/orders");

        assert!(db.get_source_schema(&endpoint).await?.is_none());

        let schema = SchemaBuilder::new()
            .column("id", schema::DataType::Int64)
            .column("updated_at", schema::DataType::Timestamp)
            .cursor()
            .build();

        db.upsert_source_schema(&endpoint, &schema).await?;
        assert_eq!(db.get_source_schema(&endpoint).await?, Some(schema.clone()));

        let replacement = Schema::new(vec![schema::Field::new(
            "id",
            schema::DataType::Int64,
            false,
        )]);
        db.upsert_source_schema(&endpoint, &replacement).await?;
        assert_eq!(db.get_source_schema(&endpoint).await?, Some(replacement));

        Ok(())
    }
}
