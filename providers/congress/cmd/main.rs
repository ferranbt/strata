use strata_congress::Congress;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Congress>().await
}
