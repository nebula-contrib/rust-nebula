use rust_nebula::{MetaClient, StorageClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let meta_addr = vec!["127.0.0.1:9559"];
    let vspace_name = "basketballplayer";
    let tag_name = "player";
    let espace_name = "basketballplayer";
    let edge_name = "serve";

    println!("v3_meta_client {:?}", &meta_addr);

    let meta_addr = meta_addr.iter().map(|s| String::from(*s)).collect();
    let mclient = MetaClient::new(&meta_addr).await?;
    let mut sclient = StorageClient::new(mclient).await;

    // prop_names is None means return all properties.
    let res = sclient.scan_vertex(&vspace_name, &tag_name, None).await?;
    for output in res {
        if let Some(dataset) = output.dataset() {
            println!("{}", dataset);
        }
    }

    // prop_names is None means return all properties.
    let res = sclient.scan_edge(&espace_name, &edge_name, None).await?;
    for output in res {
        if let Some(dataset) = output.dataset() {
            println!("{}", dataset);
        }
    }

    Ok(())
}