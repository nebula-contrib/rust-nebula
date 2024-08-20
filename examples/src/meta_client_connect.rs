use rust_nebula::{HostAddress, MetaClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let meta_addr = vec![HostAddress::new("127.0.0.1", 9559)];
    println!("meta addr: {:?}", &meta_addr);

    let mut mclient = MetaClient::new(&meta_addr).await?;

    let res = mclient.get_all_storage_addrs().await?;
    println!("{:?}", res);

    Ok(())
}
