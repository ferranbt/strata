use strata::providers::linear::Linear;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Linear>().await
}
