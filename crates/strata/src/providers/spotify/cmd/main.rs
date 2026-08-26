use strata::providers::spotify::Spotify;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Spotify>().await
}
