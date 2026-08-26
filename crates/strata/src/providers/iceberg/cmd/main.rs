use strata::providers::iceberg::Iceberg;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Iceberg>().await
}
