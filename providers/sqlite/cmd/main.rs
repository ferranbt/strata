use strata_sqlite::Sqlite;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Sqlite>().await
}
