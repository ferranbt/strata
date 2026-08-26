use strata_github::Github;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Github>().await
}
