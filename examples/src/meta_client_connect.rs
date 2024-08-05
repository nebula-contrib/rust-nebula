use rust_nebula::MetaClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let meta_addr = vec!["127.0.0.1:9559"];
    println!("meta addr: {:?}", &meta_addr);
    let meta_addr = meta_addr.iter().map(|s| String::from(*s)).collect();

    let mut mclient = MetaClient::new(&meta_addr).await?;

    let res = mclient.get_all_storage_addrs().await?;
    println!("{:?}", res);

    Ok(())
}
