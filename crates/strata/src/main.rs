//! The `strata` CLI: list providers/routes and call endpoints by path.

use anyhow::Result;
use clap::{Parser, Subcommand, ValueEnum};
use futures::StreamExt;

#[derive(Parser)]
#[command(name = "strata", about = "An abstraction over data providers")]
struct Cli {
    /// Config file mounting providers (TOML). Defaults to `strata.toml` if
    /// present; otherwise every backend is mounted at its default name.
    #[arg(long, global = true)]
    config: Option<String>,
    #[command(subcommand)]
    command: Command,
}

/// The verb to dispatch with. `create` reads a JSON request body from stdin.
#[derive(Clone, Copy, ValueEnum)]
enum Verb {
    Get,
    List,
    Create,
}

impl From<Verb> for strata::Method {
    fn from(v: Verb) -> Self {
        match v {
            Verb::Get => strata::Method::Get,
            Verb::List => strata::Method::List,
            Verb::Create => strata::Method::Create,
        }
    }
}

#[derive(Subcommand)]
enum Command {
    /// Call an endpoint with an explicit verb, e.g.
    /// `strata call get fs /file/etc/hostname` or `strata call list fs /file/var/log`.
    /// With `create`, the JSON request body is read from stdin.
    Call {
        /// Verb to dispatch: get, list, or create.
        method: Verb,
        /// Provider name, e.g. `github`.
        provider: String,
        /// Endpoint path, e.g. `/repos/rust-lang/rust`.
        path: String,
        /// For `list`: keep paging by feeding each returned `next` cursor back in
        /// (500ms between requests) until the tail. Demonstrates cursor-driven
        /// iteration — the building block of a streaming list.
        #[arg(long)]
        follow: bool,
    },
    /// Pipe a reader into a writer: read `(schema, rows)` from the source and
    /// `put` it into the destination. e.g.
    /// `strata pipe fs /file/etc postgres /tables/etc_listing`.
    Pipe {
        /// Source provider to read from.
        src_provider: String,
        /// Source endpoint path (a list/get).
        src_path: String,
        /// Destination provider to write into.
        dst_provider: String,
        /// Destination `put` path, e.g. `/tables/<name>`.
        dst_path: String,
    },
    /// List providers, or the routes of one provider.
    List {
        /// Optional provider name to list its routes.
        provider: Option<String>,
    },
    /// Emit a JSON description of endpoints and their response schemas.
    Schema {
        /// Optional provider name; omit to describe every provider.
        provider: Option<String>,
    },
    /// Serve all providers over Arrow Flight (gRPC) and GraphQL (HTTP) at once.
    Serve {
        /// Arrow Flight address.
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        /// GraphQL HTTP address.
        #[arg(long, default_value = "127.0.0.1:8080")]
        graphql_addr: String,
    },
}

#[tokio::main]
async fn main() {
    init_tracing();
    if let Err(err) = run().await {
        tracing::error!("{err:#}");
        std::process::exit(1);
    }
}

/// Install the tracing subscriber. The level filter comes from `RUST_LOG`
/// (e.g. `RUST_LOG=debug`, or per-target `RUST_LOG=strata=debug,info`),
/// defaulting to `info`. Logs go to stderr so stdout stays clean for command
/// output (JSON results, schema, listings) that callers pipe.
fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

async fn run() -> Result<()> {
    // Load .env (e.g. GITHUB_TOKEN) if present; absence is fine.
    let _ = dotenvy::dotenv();

    let cli = Cli::parse();
    // Explicit `--config` wins; otherwise fall back to `strata.toml`-or-all.
    let registry = match &cli.config {
        Some(path) => strata::registry_from_config(path)?,
        None => strata::registry()?,
    };

    match cli.command {
        Command::Call {
            method,
            provider,
            path,
            follow,
        } => {
            let provider = registry.get(&provider)?;
            if follow {
                if !matches!(method, Verb::List) {
                    anyhow::bail!("--follow only applies to `list`");
                }
                follow_list(provider, &path).await?;
                return Ok(());
            }
            let display = match method {
                // Data plane: read the stream and show the first chunk (one page)
                // as `{ items, cursor }`. Drain the whole stream with `--follow`.
                Verb::List => match provider.read(&path).await?.first().await? {
                    Some(chunk) => serde_json::json!({
                        "items": chunk.records.to_json_rows()?,
                        "cursor": chunk.cursor,
                    }),
                    None => serde_json::json!({ "items": [], "cursor": null }),
                },
                // Entity plane: `get` takes no body; `create` reads its JSON entity
                // from stdin (the `meta` channel).
                Verb::Get | Verb::Create => {
                    let body = match method {
                        Verb::Create => Some(strata::Body {
                            data: None,
                            meta: serde_json::from_reader(std::io::stdin())
                                .map_err(|e| anyhow::anyhow!("invalid JSON body on stdin: {e}"))?,
                        }),
                        _ => None,
                    };
                    let response = provider.invoke(method.into(), &path, body).await?;
                    response.entity.unwrap_or(response.output)
                }
            };
            println!("{}", serde_json::to_string_pretty(&display)?);
        }
        Command::List { provider: None } => {
            for name in registry.names() {
                println!("{name}");
            }
        }
        Command::List {
            provider: Some(name),
        } => {
            for route in registry.get(&name)?.routes() {
                println!("{route}");
            }
        }
        Command::Schema { provider: None } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&registry.describe_all())?
            );
        }
        Command::Schema {
            provider: Some(name),
        } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&registry.describe(&name)?)?
            );
        }
        Command::Pipe {
            src_provider,
            src_path,
            dst_provider,
            dst_path,
        } => {
            // Drive one sync pass with no catalog DB (`NoPipeStore`): read the source
            // and `put` each chunk into the destination until the source is caught up,
            // following its declared `ListStrategy`.
            let mut pipe = strata_types::Pipe::new(
                strata_types::Endpoint::new(src_provider, src_path),
                strata_types::Endpoint::new(dst_provider, dst_path),
            );
            strata::pipe::run_pass(&registry, &strata::pipe::store::NoPipeStore, &mut pipe).await?;
            println!(
                "piped {}{} -> {}{}",
                pipe.source.mount, pipe.source.path, pipe.destination.mount, pipe.destination.path
            );
        }
        Command::Serve { addr, graphql_addr } => {
            let addr = addr.parse()?;
            let graphql_addr = graphql_addr.parse()?;

            // Pipe subsystem: only spun up when pipes are declared, so `serve`
            // still runs without a catalog DB when there are none.
            let pipes = match strata::config_path(cli.config.as_deref()) {
                Some(path) => strata::Config::load(path)?.pipes().to_vec(),
                None => Vec::new(),
            };

            // Connect the catalog when there are pipes, and wire it into the
            // registry (each provider scoped to its mount) before sharing it.
            let mut registry = registry;
            let db = if pipes.is_empty() {
                None
            } else {
                let db =
                    database::Database::new(std::env::var("DATABASE_URL").ok().as_deref()).await?;
                registry.set_catalog(db.clone());
                Some(db)
            };
            let registry = std::sync::Arc::new(registry);

            if let Some(db) = db {
                for pipe in &pipes {
                    let created = db.create_pipe(pipe).await?;
                    let verb = if created { "created" } else { "exists" };
                    tracing::debug!(
                        "pipe {verb}: {}{} -> {}{}",
                        pipe.source.mount,
                        pipe.source.path,
                        pipe.destination.mount,
                        pipe.destination.path
                    );

                    // Persist the source's annotated schema (keyed by its
                    // query-stripped path, so the read path matches), and copy it
                    // onto the destination entry so a later traversal inherits the
                    // source's cursor/key annotations. Best-effort: a source
                    // unreachable at startup shouldn't stop `serve`.
                    match registry
                        .get(&pipe.source.mount)?
                        .resolve_schema(&pipe.source.path)
                        .await
                    {
                        Ok(schema) => {
                            for ep in [&pipe.source, &pipe.destination] {
                                let ep =
                                    strata_types::Endpoint::new(&ep.mount, strip_query(&ep.path));
                                db.upsert_source_schema(&ep, &schema).await?;
                            }
                        }
                        Err(e) => tracing::warn!(
                            "skip schema persist for {}{}: {e:#}",
                            pipe.source.mount,
                            pipe.source.path
                        ),
                    }
                }
                let all = db.list_pipes().await?;
                tracing::info!("managing {} pipe(s)", all.len());
                let store = std::sync::Arc::new(strata::pipe::store::PgPipeStore(db));
                strata::pipe::spawn_all(registry.clone(), store, all);
            }

            tokio::try_join!(
                strata::flight::serve(registry.clone(), addr),
                strata::graphql::serve(registry, graphql_addr),
            )?;
        }
    }
    Ok(())
}

/// The path with any `?query` removed — the form used as a catalog key.
fn strip_query(path: &str) -> &str {
    path.split('?').next().unwrap_or(path)
}

/// Drain a `list` endpoint's [`DataStream`](strata::DataStream), printing each
/// chunk (page) as it arrives. The router does the cursor paging internally, so
/// this just consumes the stream to the tail.
async fn follow_list(provider: &dyn strata::ProviderObject, path: &str) -> Result<()> {
    let mut stream = provider.read(path).await?;
    let mut page = 1;
    while let Some(chunk) = stream.chunks.next().await {
        let chunk = chunk?;
        let output = serde_json::json!({
            "page": page,
            "items": chunk.records.to_json_rows()?,
            "cursor": chunk.cursor,
        });
        println!("{}", serde_json::to_string_pretty(&output)?);
        page += 1;
    }
    Ok(())
}
