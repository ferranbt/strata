use strata::providers::postgres::Postgres;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Postgres>().await
}
