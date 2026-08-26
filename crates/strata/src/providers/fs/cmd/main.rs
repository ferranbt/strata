use strata::providers::fs::Fs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Fs>().await
}
