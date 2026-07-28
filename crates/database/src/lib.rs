pub mod store;

pub use store::Database;

#[cfg(feature = "test-support")]
pub mod testing {
    use dockertest::{Dockertest, Running};

    pub struct TestPostgres {
        /// Dropped → the container is removed.
        _container: Running,
        pub url: String,
    }

    impl TestPostgres {
        /// Start Postgres, wait for readiness, connect, and run all migrations.
        pub async fn start() -> anyhow::Result<Self> {
            let container = Dockertest::image("postgres")
                .tag("16-alpine")
                .env("POSTGRES_PASSWORD", "postgres")
                .env("POSTGRES_DB", "strata")
                .port(5432)
                .retry_attempts(60)
                .retry(|ep| async move {
                    sqlx::PgPool::connect(&pg_url(ep.host(), ep.port(5432)))
                        .await
                        .is_ok()
                })
                .build()
                .await
                .map_err(|e| anyhow::anyhow!("start postgres: {e}"))?;
            let url = pg_url(container.host(), container.port(5432));

            Ok(Self {
                _container: container,
                url,
            })
        }
    }

    fn pg_url(host: &str, port: u16) -> String {
        format!("postgres://postgres:postgres@{host}:{port}/strata")
    }
}
