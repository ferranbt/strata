use strata::providers::google::Google;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Google>().await
}
