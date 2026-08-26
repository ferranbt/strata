use strata::providers::github::Github;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Github>().await
}
