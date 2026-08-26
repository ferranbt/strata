use strata_iceberg::Iceberg;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Iceberg>().await
}
