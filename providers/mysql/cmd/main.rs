use strata_mysql::Mysql;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata_sdk::plugin::serve::<Mysql>().await
}
