use strata::providers::mysql::Mysql;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    strata::plugin::serve::<Mysql>().await
}
