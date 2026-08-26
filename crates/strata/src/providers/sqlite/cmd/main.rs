use strata::providers::sqlite::Sqlite;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Sqlite>().await
}
