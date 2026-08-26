use strata_clickhouse::Clickhouse;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Clickhouse>().await
}
