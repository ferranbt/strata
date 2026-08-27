//! Write-throughput harness: repeatedly `put` generated rows at an endpoint and
//! report sustained rows/sec.
//!
//! The database comes from the config file, so this can be pointed at real
//! hardware rather than a container sharing the machine.
//!
//!     cargo run --bin loadgen -- --mount sqlite --path /tables/bench --rows 20000

use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use strata_schema::{DataType, SchemaBuilder};
use serde_json::Value;
use strata::datagen::Generator;
use strata::harness::{Plan, Stop, run};
use strata::{Body, DataStream, Method};

#[derive(Parser)]
#[command(about = "Measure write throughput against a provider endpoint")]
struct Cli {
    /// Config file mounting providers (TOML). Defaults to `strata.toml` if present.
    #[arg(long)]
    config: Option<String>,

    /// Mount point of the target provider, e.g. `postgres`.
    #[arg(long)]
    mount: String,

    /// Endpoint to write to, e.g. `/tables/bench`.
    #[arg(long)]
    path: String,

    /// Rows per `put`.
    #[arg(long, default_value_t = 1000)]
    batch: usize,

    /// Stop after this many measured rows.
    #[arg(long, conflicts_with = "duration")]
    rows: Option<u64>,

    /// Stop after this many seconds.
    #[arg(long)]
    duration: Option<u64>,

    /// Batches driven before measuring starts (the first creates the table).
    #[arg(long, default_value_t = 1)]
    warmup: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let stop = match (cli.rows, cli.duration) {
        (Some(rows), _) => Stop::Rows(rows),
        (None, Some(secs)) => Stop::After(Duration::from_secs(secs)),
        (None, None) => Stop::Rows(10_000),
    };

    let registry = match &cli.config {
        Some(path) => strata::registry_from_config(path).await?,
        None => strata::registry().await?,
    };
    let provider = registry.get(&cli.mount)?;

    let schema = SchemaBuilder::new()
        .column("id", DataType::Int64)
        .key()
        .column("created_at", DataType::Timestamp)
        .cursor()
        .column("name", DataType::String)
        .build();
    let generator = Generator::new(&schema)?;

    let plan = Plan::new(cli.batch, stop).warmup(cli.warmup);
    let report = run(&plan, |offset, batch| {
        let (schema, generator, path) = (&schema, &generator, cli.path.as_str());
        async move {
            let start = offset as usize;
            let records = generator.rows(start..start + batch)?;
            let body = Body {
                data: Some(DataStream::once(schema.clone(), records)),
                meta: Value::Null,
            };
            let response = provider
                .invoke(Method::Put, path, Some(body))
                .await
                .with_context(|| format!("writing to {path}"))?;
            // Every provider's write result reports `rows_written`; fall back to the
            // batch size so a provider that doesn't still drives the loop.
            Ok(response
                .output
                .get("rows_written")
                .and_then(Value::as_u64)
                .unwrap_or(batch as u64))
        }
    })
    .await?;

    println!("{} {} batch={}", cli.mount, cli.path, cli.batch);
    println!("{report}");
    Ok(())
}
