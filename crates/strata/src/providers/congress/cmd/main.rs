use strata::providers::congress::Congress;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Congress>().await
}
