// TODO: needs a testkit helper for "serve + connect"; this is too much scaffolding.

use anyhow::Result;
use schema::HasSchema;
use strata_sdk::config::ProviderConfig;
use strata_sdk::plugin::{connect, serve_on};
use crate::registry::Registry;
use strata_sdk::record::DataStream;
use strata_sdk::router::{Body, Method};
use strata_sqlite::Sqlite;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, schema::HasSchema)]
struct Row {
    #[schema(key)]
    id: i64,
    name: String,
}

#[tokio::test]
async fn registry_mounts_a_remote_provider() -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    drop(listener);
    tokio::spawn(async move { serve_on::<Sqlite>(addr).await });

    let path = std::env::temp_dir().join(format!("strata-remote-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let config: ProviderConfig = serde_json::from_value(serde_json::json!({
        "backend": "sqlite",
        "path": path.to_str().unwrap(),
    }))?;

    let remote = loop {
        match connect("remote", &format!("http://{addr}"), &config).await {
            Ok(remote) => break remote,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
        }
    };

    let mut registry = Registry::new();
    registry.mount_object("remote", Box::new(remote))?;
    let provider = registry.get("remote")?;

    let rows = [
        Row {
            id: 1,
            name: "a".into(),
        },
        Row {
            id: 2,
            name: "b".into(),
        },
    ];
    let body = Body {
        data: Some(DataStream::of(&rows)?),
        meta: serde_json::Value::Null,
    };
    let written = provider
        .invoke(Method::Put, "/tables/remote_rows", Some(body))
        .await?;
    assert_eq!(written.output["rows_written"], 2);

    let stream = provider.read("/tables/remote_rows").await?;
    let schema = stream.schema.clone();
    let page = stream.first().await?.expect("a page");
    assert_eq!(schema, Row::schema());
    assert_eq!(page.data.decode::<Row>(&schema)?, rows);

    let _ = std::fs::remove_file(&path);
    Ok(())
}
