//! The `strata` CLI: list providers/routes and call endpoints by path.

use anyhow::Result;
use clap::{Parser, Subcommand};

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

#[derive(clap::Args)]
struct ServerArgs {
    #[arg(
        long,
        default_value = "http://127.0.0.1:8080",
        help = "Address of the strata server"
    )]
    server: String,
}

#[derive(Subcommand)]
enum Command {
    #[command(about = "Read rows from a list endpoint, e.g. `strata read /local/tables/t`")]
    Read {
        #[command(flatten)]
        server: ServerArgs,
        #[arg(help = "Endpoint path, e.g. /local/tables/t")]
        path: String,
        #[arg(long, help = "Keep paging until the tail")]
        follow: bool,
        #[arg(long, help = "Page size hint")]
        limit: Option<u32>,
    },
    #[command(
        about = "Pipe a reader into a writer, e.g. `strata pipe /fs/file/etc /postgres/tables/etc`"
    )]
    Pipe {
        #[command(flatten)]
        server: ServerArgs,
        #[arg(help = "Source path to read from, e.g. /dummy/data")]
        source: String,
        #[arg(help = "Destination put path, e.g. /local/tables/t")]
        destination: String,
    },
    /// List endpoint paths, optionally under a path prefix.
    List {
        #[command(flatten)]
        server: ServerArgs,
        /// Path prefix, e.g. `/local`. Omit for every endpoint.
        path: Option<String>,
    },
    /// Emit a JSON description of endpoints and their response schemas.
    Schema {
        #[command(flatten)]
        server: ServerArgs,
        /// Path prefix, e.g. `/local`. Omit for every endpoint.
        path: Option<String>,
    },
    /// Run the strata server
    Serve {
        /// Arrow Flight address.
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        /// HTTP address of the server.
        #[arg(long, default_value = "127.0.0.1:8080")]
        http_addr: String,
        /// Whether to start the graphql server.
        #[arg(long, default_value = "false")]
        graphql: bool,
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

    match cli.command {
        Command::Read {
            server,
            path,
            follow,
            limit,
        } => {
            let client = strata::api::Client::new(&server.server);
            let mut query = strata::api::Query {
                limit,
                ..Default::default()
            };
            loop {
                let response = client.read(&path, &query).await?;
                let page = serde_json::json!({
                    "rows": response.rows,
                    "cursor": response.cursor,
                });
                println!("{}", serde_json::to_string_pretty(&page)?);
                match response.cursor.next {
                    Some(next) if follow => query.cursor = Some(next),
                    _ => break,
                }
            }
        }
        Command::List { server, path } => {
            let client = strata::api::Client::new(&server.server);
            for endpoint in client.list(path.as_deref()).await?.endpoints {
                match endpoint.as_str() {
                    Some(path) => println!("{path}"),
                    None => println!("{endpoint}"),
                }
            }
        }
        Command::Schema { server, path } => {
            let client = strata::api::Client::new(&server.server);
            let response = client.schema(path.as_deref()).await?;
            println!("{}", serde_json::to_string_pretty(&response.schema)?);
        }
        Command::Pipe {
            server,
            source,
            destination,
        } => {
            let client = strata::api::Client::new(&server.server);
            client.pipe(&source, &destination).await?;
            println!("piped {source} -> {destination}");
        }
        Command::Serve {
            addr,
            http_addr,
            graphql,
        } => {
            let mut options = strata::server::Options::new()
                .flight(addr.parse()?)
                .http(http_addr.parse()?)
                .graphql(graphql);
            if let Some(path) = &cli.config {
                options = options.config(path);
            }
            strata::server::Strata::new(options).await?;
        }
    }
    Ok(())
}
