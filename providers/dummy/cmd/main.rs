use strata::providers::dummy::Dummy;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Dummy>().await
}
