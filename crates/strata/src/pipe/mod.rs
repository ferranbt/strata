pub mod store;
pub mod sync;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use futures::StreamExt;
use serde_json::Value;
use strata_types::Pipe;

use crate::dataset::DataStream;
use crate::request::{ReadRequest, WriteRequest};
use crate::router::Body;
use crate::{Method, Registry};
use store::PipeStore;
use sync::Progress;

pub async fn run_pass<S: PipeStore>(registry: &Registry, store: &S, pipe: &mut Pipe) -> Result<()> {
    let src = registry.get(&pipe.source.mount)?;
    let dst = registry.get(&pipe.destination.mount)?;
    let meta = src.resolve(&pipe.source.path).await?.metadata;
    let strategy = meta.strategy.ok_or_else(|| {
        anyhow!(
            "source `{}{}` is not a list endpoint (no sync strategy)",
            pipe.source.mount,
            pipe.source.path
        )
    })?;

    // One live stream; its `schema` is the sink's contract, held for every chunk.
    let src_path = ReadRequest::new(&pipe.source.path)
        .with_cursor(pipe.cursor.clone())
        .path();
    let stream = src.read(&src_path).await?;
    let schema = stream.schema.clone();
    // The source declares how a sink should apply its rows (merge if it re-emits
    // updates, else append). The sink still skips existing keys on append.
    let dst_path = WriteRequest::new(&pipe.destination.path)
        .with_disposition(meta.disposition)
        .path();
    let mut chunks = stream.chunks;

    let mut total = 0u64;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        // Decide before the chunk is moved into the write stream.
        let progress = sync::advance(strategy, &chunk);

        // Write the rows (skip an empty tail page — nothing to store).
        let rows = chunk.records.row_count() as u64;
        if rows > 0 {
            let page = DataStream {
                schema: schema.clone(),
                chunks: futures::stream::once(async move { Ok(chunk) }).boxed(),
            };
            let body = Body {
                data: Some(page),
                meta: Value::Null,
            };
            dst.invoke(Method::Put, &dst_path, Some(body)).await?;
            total += rows;
            tracing::debug!(
                "pipe[{}{} -> {}{}] stored {rows} rows ({total} this pass)",
                pipe.source.mount,
                pipe.source.path,
                pipe.destination.mount,
                pipe.destination.path
            );
        }

        // Checkpoint the resume point, then keep pulling or stop.
        let caught_up = matches!(progress, Progress::CaughtUp(_));
        match progress {
            Progress::Continue(token) => pipe.cursor = Some(token),
            Progress::CaughtUp(resume) => pipe.cursor = resume,
        }
        store.checkpoint(pipe.id, pipe.cursor.clone()).await?;
        if caught_up {
            tracing::debug!(
                "pipe[{}{} -> {}{}] caught up, {total} rows this pass",
                pipe.source.mount,
                pipe.source.path,
                pipe.destination.mount,
                pipe.destination.path
            );
            break;
        }
    }
    Ok(())
}

const POLL_INTERVAL: Duration = Duration::from_secs(5);

pub fn spawn_all<S: PipeStore + 'static>(registry: Arc<Registry>, store: Arc<S>, pipes: Vec<Pipe>) {
    for mut pipe in pipes {
        let registry = registry.clone();
        let store = store.clone();
        tokio::spawn(async move {
            let label = format!(
                "{}{} -> {}{}",
                pipe.source.mount, pipe.source.path, pipe.destination.mount, pipe.destination.path
            );
            loop {
                tracing::debug!("pipe[{label}] running from cursor {:?}", pipe.cursor);
                match run_pass(&registry, store.as_ref(), &mut pipe).await {
                    Ok(()) => tracing::debug!(
                        "pipe[{label}] caught up; polling again in {POLL_INTERVAL:?}"
                    ),
                    Err(e) => tracing::warn!("pipe[{label}] run: {e:#}"),
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProviderConfig;
    use crate::pipe::store::NoPipeStore;
    use crate::providers::dummy::Dummy;
    use crate::providers::sqlite::Sqlite;
    use schema::{DataType, SchemaBuilder as StrataSchemaBuilder};

    use serde_json::json;
    use strata_types::Endpoint;

    /// Drive `run_pass` end-to-end with no database: generate rows (`dummy`) and pipe
    /// them into a local sqlite file, then read them back.
    #[tokio::test]
    async fn run_pass_dummy_to_sqlite() -> Result<()> {
        let db_path = std::env::temp_dir().join("strata_run_pass.sqlite");
        let _ = std::fs::remove_file(&db_path);

        let mut registry = Registry::new();
        let dummy_cfg: ProviderConfig = serde_json::from_value(json!({ "backend": "dummy" }))?;
        registry.mount::<Dummy>("dummy", &dummy_cfg)?;
        let sqlite_cfg: ProviderConfig = serde_json::from_value(
            json!({ "backend": "sqlite", "path": db_path.to_str().unwrap() }),
        )?;
        registry.mount::<Sqlite>("local", &sqlite_cfg)?;

        // dummy takes its row shape on the URL as a JSON `Schema`; `id` is the key.
        let schema = StrataSchemaBuilder::new()
            .column("id", DataType::Int64)
            .key()
            .column("name", DataType::String)
            .build();

        let encoded = urlencoding::encode(&serde_json::to_string(&schema)?).into_owned();
        let src_path = format!("/data?schema={encoded}&rows=25");

        let count_rows = || async {
            let stream = registry.get("local")?.read("/tables/bench").await?;
            let mut chunks = stream.chunks;
            let mut total = 0usize;
            while let Some(chunk) = chunks.next().await {
                total += chunk?.records.row_count();
            }
            anyhow::Ok(total)
        };

        // First pass: 25 rows land.
        let mut pipe = Pipe::new(
            Endpoint::new("dummy", src_path.clone()),
            Endpoint::new("local", "/tables/bench"),
        );
        run_pass(&registry, &NoPipeStore, &mut pipe).await?;
        assert_eq!(count_rows().await?, 25);

        // Second pass over the same keyed rows: the source is append (`dummy`
        // declares no `.writes(Merge)`), so the sink skips existing keys — no dupes.
        let mut pipe = Pipe::new(
            Endpoint::new("dummy", src_path),
            Endpoint::new("local", "/tables/bench"),
        );
        run_pass(&registry, &NoPipeStore, &mut pipe).await?;
        assert_eq!(count_rows().await?, 25);

        let _ = std::fs::remove_file(&db_path);
        Ok(())
    }
}
