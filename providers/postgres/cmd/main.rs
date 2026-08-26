use strata_postgres::Postgres;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Postgres>().await
}
