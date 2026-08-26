use strata::providers::clickhouse::Clickhouse;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Clickhouse>().await
}
