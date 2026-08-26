use strata_spotify::Spotify;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Spotify>().await
}
