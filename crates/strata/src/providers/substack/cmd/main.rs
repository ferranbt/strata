use strata::providers::substack::Substack;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Substack>().await
}
