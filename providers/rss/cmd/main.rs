use strata_rss::Rss;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Rss>().await
}
