use strata::providers::rss::Rss;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Rss>().await
}
